import os
import sys
import re
import csv
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ========== TRỌNG SỐ CHO TỪNG CỘT ==========
WEIGHTS_CAU13 = {
    'chuẩn đầu ra': 5.0, 'mục tiêu môn học': 4.5, 'đáp ứng chương trình': 4.0,
    'nội dung': 3.0, 'học phần': 3.0, 'chương trình': 2.5, 'môn học': 2.5,
    'trang bị': 2.0, 'cung cấp': 2.0, 'đào tạo': 2.0, 'bám sát': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'đầy đủ': 1.0, 'hợp lý': 1.0, 
    'chất lượng': 1.0, 'bổ ích': 1.0, 'cần thiết': 1.0, 'quan trọng': 1.0, 
    'chi tiết': 1.0, 'cụ thể': 1.0, 'chuẩn': 1.0
}

WEIGHTS_CAU14 = {
    'giảng viên': 5.0, 'thầy giáo': 5.0, 'cô giáo': 5.0, 'tận tâm': 4.5,
    'nhiệt tình': 4.0, 'tận tình': 4.0, 'truyền cảm hứng': 4.0,
    'thầy': 3.0, 'cô': 3.0, 'gv': 3.0, 'dạy': 3.0, 'giảng': 3.0,
    'nhiệt huyết': 3.0, 'tâm huyết': 3.0, 'dễ hiểu': 3.0,
    'bài giảng': 2.0, 'truyền đạt': 2.0, 'giải thích': 2.0, 'hướng dẫn': 2.0,
    'sinh động': 2.0, 'linh hoạt': 2.0, 'đa dạng': 2.0, 'thu hút': 2.0,
    'tương tác': 2.0, 'sôi nổi': 2.0, 'thú vị': 2.0, 'hấp dẫn': 2.0,
    'vui vẻ': 1.0, 'thân thiện': 1.0, 'gần gũi': 1.0, 'thoải mái': 1.0,
    'hay': 1.0, 'tốt': 1.0
}

WEIGHTS_CAU15 = {
    'kiểm tra': 5.0, 'đánh giá': 5.0, 'công bằng': 4.5, 'minh bạch': 4.0,
    'đánh giá đúng': 4.0, 'phản ánh đúng': 4.0,
    'thi': 3.0, 'đề thi': 3.0, 'bài kiểm tra': 3.0, 'cho điểm': 3.0,
    'công khai': 3.0, 'nghiêm túc': 3.0, 'khách quan': 3.0,
    'điểm': 2.0, 'bài tập': 2.0, 'chấm': 2.0, 'giữa kỳ': 2.0, 'cuối kỳ': 2.0,
    'thực lực': 2.0, 'công tâm': 2.0, 'chính xác': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'kỹ càng': 1.0, 'chỉnh chu': 1.0
}

WEIGHTS_CAU16 = {
    'không có góp ý': 5.0, 'không ý kiến': 5.0, 'không góp ý': 4.5,
    'không': 3.0, 'ko': 3.0, 'k': 2.5, 'không có': 3.0,
    'tuyệt vời': 2.0, 'quá ok': 2.0, 'rất ok': 2.0, 'ổn hết': 2.0,
    'ok': 1.0, 'oki': 1.0, 'ổn': 1.0, 'được': 1.0, 'cảm ơn': 1.0, 'tốt hơn': 1.0
}

ALL_WEIGHTS = {
    'Cau13': WEIGHTS_CAU13,
    'Cau14': WEIGHTS_CAU14,
    'Cau15': WEIGHTS_CAU15,
    'Cau16': WEIGHTS_CAU16
}

COLUMN_ORDER = ['Cau13', 'Cau14', 'Cau15', 'Cau16']


def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        print(f"Đã tải file {SURVEY_FILE} từ blob")
        return True
    except Exception as e:
        print(f"Lỗi tải file từ blob: {e}")
        sys.exit(1)


def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        print(f"Đã upload file {output_path} lên blob")
        return True
    except Exception as e:
        print(f"Lỗi upload file lên blob: {e}")
        return False


def is_date_format(value):
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    value = value.strip()
    if len(value) == 7 and value.isdigit():
        return True
    if len(value) == 7 and value.startswith("TG"):
        return True
    if value == "gvDacThu_TKTH":
        return True
    return False


def calculate_weighted_score(text, column_name):
    """Tính điểm có trọng số cho một text đối với một cột cụ thể"""
    if not text or not isinstance(text, str):
        return 0.0
    
    text_lower = text.lower()
    total_score = 0.0
    weights = ALL_WEIGHTS.get(column_name, {})
    
    for keyword, weight in weights.items():
        if keyword in text_lower:
            count = text_lower.count(keyword)
            total_score += weight * (1 + 0.1 * (count - 1))
    
    # Điểm thưởng độ dài
    length_score = min(len(text) * 0.03, 1.0)
    total_score += length_score
    
    return total_score


def get_phrase_bonus(segment_parts):
    """Phát hiện cụm từ có nghĩa khi gộp các phần tử"""
    if len(segment_parts) < 2:
        return 0.0
    
    merged_text = ' '.join(segment_parts).lower()
    bonus = 0.0
    
    # Các cụm từ có nghĩa
    meaningful_phrases = [
        ('nội dung', 'đầy đủ', 1.0),
        ('nội dung', 'chi tiết', 1.0),
        ('đầu ra', 'chuẩn', 1.0),
        ('đánh giá', 'cụ thể', 1.5),
        ('kiểm tra', 'cụ thể', 1.5),
        ('giảng viên', 'nhiệt tình', 1.0),
        ('bài giảng', 'dễ hiểu', 1.0),
        ('đánh giá', 'công bằng', 1.0),
        ('kiểm tra', 'công bằng', 1.0)
    ]
    
    for kw1, kw2, weight in meaningful_phrases:
        if kw1 in merged_text and kw2 in merged_text:
            bonus += weight
    
    return bonus


def split_by_condition_1(text):
    """Cấp 1: Tách với điều kiện trước và sau dấu phẩy đều không có khoảng trắng"""
    parts = []
    current = []
    i = 0
    
    while i < len(text):
        if text[i] == ',':
            has_space_before = (i > 0 and text[i-1] == ' ')
            has_space_after = (i + 1 < len(text) and text[i+1] == ' ')
            
            if not has_space_before and not has_space_after:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
    
    if current:
        parts.append(''.join(current).strip())
    
    return [p for p in parts if p]


def split_by_condition_2(text):
    """Cấp 2: Tách với điều kiện sau dấu phẩy không có khoảng trắng"""
    parts = []
    current = []
    i = 0
    
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text) and text[i+1] == ' ':
                current.append(',')
            else:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
        else:
            current.append(text[i])
        i += 1
    
    if current:
        parts.append(''.join(current).strip())
    
    return [p for p in parts if p]


def split_by_condition_3(text):
    """Cấp 3: Tách với điều kiện sau dấu phẩy không có khoảng trắng VÀ chữ in hoa đầu tiên"""
    parts = []
    current = []
    i = 0
    
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text):
                next_char = text[i + 1]
                if next_char != ' ' and next_char.isupper():
                    if current:
                        parts.append(''.join(current).strip())
                        current = []
                else:
                    current.append(',')
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
    
    if current:
        parts.append(''.join(current).strip())
    
    return [p for p in parts if p]


def try_create_4th_column(parts):
    """Thử lấy phần tử cuối cùng sau dấu phẩy của cột cuối để tạo cột thứ 4"""
    if len(parts) == 3:
        last_col = parts[-1]
        if ',' in last_col:
            sub_parts = last_col.split(',')
            if len(sub_parts) >= 2:
                last_element = sub_parts[-1].strip()
                parts[-1] = ','.join(sub_parts[:-1]).strip()
                parts.append(last_element)
                return True, parts
    return False, parts


def sequential_scoring_classification(parts):
    """
    Phân loại các phần tử bằng trọng số, đảm bảo:
    - Sử dụng HẾT tất cả các phần tử
    - Giữ nguyên thứ tự
    - Mỗi cột có ít nhất 1 phần tử
    """
    if not parts:
        return []
    
    n = len(parts)
    num_columns = 4
    
    # DP table
    dp = [[-float('inf')] * num_columns for _ in range(n + 1)]
    choice = [[None] * num_columns for _ in range(n + 1)]
    
    dp[0][0] = 0
    
    # Điền DP
    for i in range(n):
        for j in range(num_columns):
            if dp[i][j] < 0:
                continue
            
            remaining_columns = num_columns - j
            min_remaining_parts = remaining_columns - 1
            max_k = n - i - min_remaining_parts
            
            for k in range(1, max_k + 1):
                segment_parts = parts[i:i+k]
                merged_text = ', '.join(segment_parts)
                
                # Tính điểm cho đoạn
                base_score = calculate_weighted_score(merged_text, COLUMN_ORDER[j])
                phrase_bonus = get_phrase_bonus(segment_parts)
                score = base_score + phrase_bonus
                
                if j + 1 < num_columns:
                    new_score = dp[i][j] + score
                    if new_score > dp[i + k][j + 1]:
                        dp[i + k][j + 1] = new_score
                        choice[i + k][j + 1] = (i, j, k, merged_text)
                else:
                    if i + k == n:
                        new_score = dp[i][j] + score
                        if new_score > dp[i + k][j]:
                            dp[i + k][j] = new_score
                            choice[i + k][j] = (i, j, k, merged_text)
    
    # Tìm kết quả tốt nhất
    best_score = dp[n][num_columns - 1]
    
    if best_score < 0:
        return fallback_even_split(parts)
    
    # Truy vết
    assignments = []
    i, j = n, num_columns - 1
    while i > 0 and j >= 0:
        if choice[i][j] is None:
            break
        
        prev_i, prev_j, k, text = choice[i][j]
        assignments.insert(0, {
            'column': COLUMN_ORDER[prev_j],
            'text': text,
            'num_parts': k
        })
        i, j = prev_i, prev_j
    
    return assignments


def fallback_even_split(parts):
    """Fallback: chia đều các phần tử, đảm bảo dùng hết"""
    n = len(parts)
    num_columns = 4
    
    sizes = [1] * num_columns
    remaining = n - num_columns
    
    for i in range(remaining):
        sizes[i % num_columns] += 1
    
    assignments = []
    start = 0
    for col_idx, size in enumerate(sizes):
        end = start + size
        merged_text = ', '.join(parts[start:end])
        assignments.append({
            'column': COLUMN_ORDER[col_idx],
            'text': merged_text,
            'num_parts': size
        })
        start = end
    
    return assignments


def split_after_null_by_scoring(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL:
    1. Dùng 3 cấp rule-based để tách
    2. Dùng trọng số để phân loại có thứ tự
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    # CẤP 1
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    # CẤP 2
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    # CẤP 3
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    # Chọn bộ parts có số lượng phần tử lớn nhất
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    # Đảm bảo có ít nhất 4 phần tử
    if len(best_parts) < 4:
        error_info = {
            'row_number': row_number,
            'original_after_null': original_text,
            'message': f'Chỉ có {len(best_parts)} phần tử, cần ít nhất 4'
        }
        return [original_text, '', '', ''], error_info
    
    # Dùng scoring để phân loại
    assignments = sequential_scoring_classification(best_parts)
    
    # Xây dựng kết quả
    result = {col: '' for col in COLUMN_ORDER}
    for assign in assignments:
        col = assign['column']
        text = assign['text']
        if result[col]:
            result[col] = f"{result[col]}, {text}"
        else:
            result[col] = text
    
    return [result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16']], None


def process_row(row, row_number=None):
    """Xử lý một dòng CSV theo logic"""
    if not row or len(row) < 2:
        return None, None, []
    
    try:
        # ========== PHẦN 1: XỬ LÝ CÁC CỘT TRƯỚC CỘT NULL ==========
        
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''
        
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break
        
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p.strip() for p in ho_dem_ten_parts if p and p.strip()])
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break
        
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        
        ho_dem_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 1 < len(row):
            ho_dem_gv = row[ma_gv_index + 1].strip()
        
        ten_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 2 < len(row):
            ten_gv = row[ma_gv_index + 2].strip()
        
        lop_hp = ''
        if ma_gv_index >= 0 and ma_gv_index + 3 < len(row):
            lop_hp = row[ma_gv_index + 3].strip()
        
        cau_hoi = ''
        if ma_gv_index >= 0 and ma_gv_index + 4 < len(row):
            cau_hoi = row[ma_gv_index + 4].strip()
        
        gia_tri = ''
        if ma_gv_index >= 0 and ma_gv_index + 5 < len(row):
            gia_tri = row[ma_gv_index + 5].strip()
        
        null_index = -1
        null_value = ''
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
                null_value = potential_null if potential_null else 'NULL'
        
        # ========== PHẦN 2: XỬ LÝ CÁC CỘT SAU CỘT NULL ==========
        cau13 = cau14 = cau15 = cau16 = ''
        split_errors = []
        
        if null_index >= 0 and null_index + 1 < len(row):
            after_null = row[null_index + 1:]
            split_result, error = split_after_null_by_scoring(after_null, row_number)
            
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            
            if error:
                split_errors.append(error)
        
        result = {
            'Lop': lop,
            'MaSV': ma_sv,
            'HoDem': ho_dem,
            'Ten': ten,
            'NgaySinh': ngay_sinh,
            'MaHP': ma_hp,
            'TenHP': ten_hp,
            'MaGV': ma_gv,
            'HoDemGV': ho_dem_gv,
            'TenGV': ten_gv,
            'LopHP': lop_hp,
            'CauHoi': cau_hoi,
            'GiaTri': gia_tri,
            'NULL': null_value,
            'Cau13': cau13,
            'Cau14': cau14,
            'Cau15': cau15,
            'Cau16': cau16
        }
        
        return result, None, split_errors
        
    except Exception as e:
        print(f"Lỗi xử lý dòng {row_number}: {e}")
        return None, str(e), []


def read_csv_manual(filename):
    rows = []
    error_rows = []
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                row = line.split(',')
                row = [col.strip() for col in row]
                rows.append(row)
                if line_num % 1000 == 0:
                    print(f"Đã đọc {line_num} dòng...")
        print(f"Đã đọc xong file: {len(rows)} dòng")
        return rows, error_rows
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return [], []


def main():
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("Kết nối blob storage thành công")
    except Exception as e:
        print(f"Lỗi kết nối blob: {e}")
        sys.exit(1)
    
    download_from_blob(blob_service)
    
    print("Đang đọc file CSV...")
    rows, read_errors = read_csv_manual(SURVEY_FILE)
    
    if not rows:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    print(f"Bắt đầu xử lý {len(rows)} dòng...")
    
    processed_rows = []
    process_errors = []
    split_errors = []
    
    for idx, row in enumerate(rows, 1):
        result, error, split_errs = process_row(row, idx)
        
        if result:
            processed_rows.append(result)
        
        if error:
            process_errors.append({
                'line_number': idx,
                'error': error,
                'row_length': len(row)
            })
        
        if split_errs:
            split_errors.extend(split_errs)
        
        if idx % 1000 == 0:
            print(f"Đã xử lý {idx}/{len(rows)} dòng...")
    
    result_df = pd.DataFrame(processed_rows)
    
    print(f"\n{'='*60}")
    print("BÁO CÁO XỬ LÝ")
    print(f"{'='*60}")
    print(f"Tổng số dòng đọc được: {len(rows)}")
    print(f"Số dòng xử lý thành công: {len(processed_rows)}")
    print(f"Số dòng xử lý lỗi: {len(process_errors)}")
    
    if split_errors:
        print(f"\n{'='*60}")
        print(f"CÁC DÒNG LỖI PHÂN LOẠI ({len(split_errors)} dòng)")
        print(f"{'='*60}")
        for err in split_errors[:10]:
            print(f"\nDòng {err.get('row_number', '?')}:")
            print(f"  Chuỗi sau NULL: {err.get('original_after_null', '')[:200]}")
        
        split_error_df = pd.DataFrame(split_errors)
        split_error_filename = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(split_error_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu {len(split_errors)} dòng lỗi vào file: {split_error_filename}")
    
    if len(processed_rows) > 0:
        output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        
        if upload_to_blob(blob_service, result_df, output_path):
            print(f"\n{'='*60}")
            print("THÀNH CÔNG!")
            print(f"{'='*60}")
            print(f"File kết quả: {output_path}")
            print(f"Số dòng đã xử lý: {len(processed_rows)}")
            print(f"{'='*60}")
        else:
            print("Upload file thất bại!")
            sys.exit(1)
    else:
        print("Không có dòng nào được xử lý thành công!")
        sys.exit(1)


if __name__ == "__main__":
    main()
