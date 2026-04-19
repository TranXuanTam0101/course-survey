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

# ========== TỪ KHÓA CHO CÁC CỘT ==========
KEYWORDS_CAU14 = [
    'giảng viên', 'cô', 'thầy', 'dạy', 'giảng', 'bài giảng',
    'dễ hiểu', 'nhiệt tình', 'tận tâm', 'tận tình', 'vui vẻ',
    'thân thiện', 'hấp dẫn', 'thú vị', 'tương tác', 'sôi nổi',
    'truyền đạt', 'giải thích', 'hướng dẫn', 'phương pháp',
    'giáo viên', 'gv', 'thầy giáo', 'cô giáo', 'nhiệt huyết'
]

KEYWORDS_CAU15 = [
    'kiểm tra', 'đánh giá', 'thi', 'bài tập', 'điểm', 'chấm',
    'đề thi', 'công bằng', 'minh bạch', 'nghiêm túc', 'phù hợp',
    'thực lực', 'khách quan', 'công tâm', 'đề kiểm tra',
    'giữa kỳ', 'cuối kỳ', 'bài kiểm tra', 'cho điểm'
]

KEYWORDS_CAU16 = [
    'không', 'ko', 'ok', 'oki', 'ổn', 'tốt', 'được',
    'không có', 'không ạ', 'dạ không', 'không có ý kiến',
    'hết', 'xong', 'cảm ơn', 'thanks', 'k', 'không góp ý'
]


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


def has_keyword(text, keywords):
    """Kiểm tra text có chứa bất kỳ từ khóa nào không"""
    if not text or not isinstance(text, str):
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def classify_5_parts(parts):
    """
    Phân loại cho 5 phần tử: [P1, P2, P3, P4, P5]
    P1 → Cau13 (luôn)
    P5 → Cau16 (luôn)
    P2, P3, P4 phân loại dựa trên từ khóa
    """
    P1, P2, P3, P4, P5 = parts
    
    cau13 = P1
    cau16 = P5
    cau14 = ""
    cau15 = ""
    
    # Xét P2 có từ khóa Cau14 không?
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14 = P2
        cau15_parts = [P3, P4]
        
        # Kiểm tra P3, P4 có từ khóa Cau14 không?
        for i, part in enumerate(cau15_parts):
            if has_keyword(part, KEYWORDS_CAU14):
                if cau14:
                    cau14 = f"{cau14}, {part}"
                else:
                    cau14 = part
                cau15_parts[i] = None
        
        cau15_parts = [p for p in cau15_parts if p is not None]
        cau15 = ", ".join(cau15_parts) if cau15_parts else ""
        
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14 = P3
        cau13 = f"{cau13}, {P2}"
        cau15 = P4
    else:
        cau13 = f"{cau13}, {P2}"
        cau15_parts = [P3, P4]
        
        for i, part in enumerate(cau15_parts):
            if has_keyword(part, KEYWORDS_CAU14):
                if cau14:
                    cau14 = f"{cau14}, {part}"
                else:
                    cau14 = part
                cau15_parts[i] = None
        
        cau15_parts = [p for p in cau15_parts if p is not None]
        cau15 = ", ".join(cau15_parts) if cau15_parts else ""
    
    return cau13, cau14, cau15, cau16


def classify_6_parts(parts):
    """
    Phân loại cho 6 phần tử: [P1, P2, P3, P4, P5, P6]
    P1 → Cau13 (luôn)
    P6 → Cau16 (luôn)
    P2, P3, P4, P5 phân loại dựa trên từ khóa
    """
    P1, P2, P3, P4, P5, P6 = parts
    
    cau13 = P1
    cau16 = P6
    cau14 = ""
    cau15 = ""
    
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14_parts = [P2, P3]
        
        if has_keyword(P4, KEYWORDS_CAU15):
            cau15_parts = [P4]
        else:
            cau14_parts.append(P4)
            cau15_parts = []
        
        cau15_parts.append(P5)
        
        # Kiểm tra P4, P5 có từ khóa Cau14 không?
        for i, part in enumerate(cau15_parts):
            if has_keyword(part, KEYWORDS_CAU14):
                cau14_parts.append(part)
                cau15_parts[i] = None
        
        cau15_parts = [p for p in cau15_parts if p is not None]
        cau14 = ", ".join(cau14_parts)
        cau15 = ", ".join(cau15_parts) if cau15_parts else ""
        
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14_parts = [P3, P4]
        cau13 = f"{cau13}, {P2}"
        cau15 = P5
        
        if has_keyword(P5, KEYWORDS_CAU14):
            cau14_parts.append(P5)
            cau15 = ""
        
        cau14 = ", ".join(cau14_parts)
    else:
        cau13 = f"{cau13}, {P2}"
        cau15_parts = [P3, P4, P5]
        
        for i, part in enumerate(cau15_parts):
            if has_keyword(part, KEYWORDS_CAU14):
                if cau14:
                    cau14 = f"{cau14}, {part}"
                else:
                    cau14 = part
                cau15_parts[i] = None
        
        cau15_parts = [p for p in cau15_parts if p is not None]
        cau15 = ", ".join(cau15_parts) if cau15_parts else ""
    
    return cau13, cau14, cau15, cau16


def classify_4_parts(parts):
    """Phân loại cho 4 phần tử"""
    P1, P2, P3, P4 = parts
    
    cau13 = P1
    
    if has_keyword(P4, KEYWORDS_CAU16):
        cau16 = P4
        remaining = [P2, P3]
    else:
        cau16 = ""
        remaining = [P2, P3, P4]
    
    cau14 = ""
    cau15 = ""
    
    for part in remaining:
        if has_keyword(part, KEYWORDS_CAU14):
            if cau14:
                cau14 = f"{cau14}, {part}"
            else:
                cau14 = part
        else:
            if cau15:
                cau15 = f"{cau15}, {part}"
            else:
                cau15 = part
    
    return cau13, cau14, cau15, cau16


def classify_3_parts(parts):
    """Phân loại cho 3 phần tử"""
    P1, P2, P3 = parts
    
    cau13 = P1
    
    if has_keyword(P3, KEYWORDS_CAU16):
        cau16 = P3
        remaining = [P2]
    else:
        cau16 = ""
        remaining = [P2, P3]
    
    cau14 = ""
    cau15 = ""
    
    for part in remaining:
        if has_keyword(part, KEYWORDS_CAU14):
            if cau14:
                cau14 = f"{cau14}, {part}"
            else:
                cau14 = part
        else:
            if cau15:
                cau15 = f"{cau15}, {part}"
            else:
                cau15 = part
    
    return cau13, cau14, cau15, cau16


def classify_by_position_and_keywords(parts):
    """
    Phân loại các phần tử vào 4 cột dựa trên vị trí và từ khóa
    """
    num_parts = len(parts)
    
    if num_parts == 5:
        return classify_5_parts(parts)
    elif num_parts == 6:
        return classify_6_parts(parts)
    elif num_parts == 4:
        return classify_4_parts(parts)
    elif num_parts == 3:
        return classify_3_parts(parts)
    elif num_parts == 2:
        P1, P2 = parts
        if has_keyword(P2, KEYWORDS_CAU16):
            return P1, "", "", P2
        else:
            return P1, P2, "", ""
    elif num_parts == 1:
        return parts[0], "", "", ""
    else:
        return "", "", "", ""


def split_by_condition_1(text):
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


def split_after_null_by_rules(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL:
    1. Dùng 3 cấp rule-based để tách
    2. Phân loại theo vị trí + từ khóa
    3. Nếu không thể, để toàn bộ vào cột đầu
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    # ===== CẤP 1 =====
    parts_level1 = split_by_condition_1(original_text)
    
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success:
            return new_parts[:4], None
    
    # ===== CẤP 2 =====
    parts_level2 = split_by_condition_2(original_text)
    
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success:
            return new_parts[:4], None
    
    # ===== CẤP 3 =====
    parts_level3 = split_by_condition_3(original_text)
    
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success:
            return new_parts[:4], None
    
    # ===== PHÂN LOẠI THEO VỊ TRÍ + TỪ KHÓA =====
    # Chọn bộ parts có số lượng phần tử lớn nhất để phân loại
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    if len(best_parts) >= 3:
        cau13, cau14, cau15, cau16 = classify_by_position_and_keywords(best_parts)
        
        # Kiểm tra nếu phân loại thành công (có ít nhất Cau13 hoặc Cau14 có nội dung)
        if cau13 or cau14 or cau15 or cau16:
            return [cau13, cau14, cau15, cau16], None
    
    # Nếu vẫn không phân loại được -> để toàn bộ vào cột đầu
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'level1_result': parts_level1,
        'level2_result': parts_level2,
        'level3_result': parts_level3,
        'final_count': len(best_parts),
        'message': f'Sau 3 cấp có {len(best_parts)} cột, không phân loại được - để TOÀN BỘ vào cột đầu'
    }
    return [original_text, '', '', ''], error_info


def process_row(row, row_number=None):
    """
    Xử lý một dòng CSV theo logic
    """
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
            split_result, error = split_after_null_by_rules(after_null, row_number)
            
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
        print(f"CÁC DÒNG KHÔNG PHÂN LOẠI ĐƯỢC - ĐÃ ĐỂ TOÀN BỘ VÀO CỘT ĐẦU ({len(split_errors)} dòng)")
        print(f"{'='*60}")
        for err in split_errors[:10]:
            print(f"\nDòng {err.get('row_number', '?')}:")
            print(f"  Chuỗi sau NULL: {err.get('original_after_null', '')[:200]}")
            print(f"  Số cột: {err.get('final_count', 0)}")
        
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
