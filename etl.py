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

# ========== TỪ KHÓA VỚI TRỌNG SỐ (SCORING) ==========
# Điểm càng cao = càng đặc trưng cho cột đó

KEYWORDS_CAU13 = {   # Nội dung học phần / Chuẩn đầu ra
    'nội dung': 5, 'chuẩn đầu ra': 6, 'chương trình': 4, 'học phần': 4, 'môn học': 4,
    'bám sát': 5, 'đầy đủ': 4, 'hợp lý': 3, 'cụ thể': 4, 'bổ ích': 4, 'cần thiết': 4,
    'kiến thức cơ bản': 4, 'mục tiêu': 4, 'phù hợp': 3, 'rõ ràng': 3
}

KEYWORDS_CAU14 = {   # Giảng viên - Hoạt động dạy học
    'thầy': 4, 'cô': 4, 'giảng viên': 5, 'gv': 3, 'dạy': 4, 'giảng': 4,
    'dễ hiểu': 6, 'nhiệt tình': 6, 'tận tâm': 6, 'tận tình': 6, 'vui vẻ': 5,
    'thân thiện': 5, 'tương tác': 6, 'sôi nổi': 5, 'truyền đạt': 5, 'giải thích': 5,
    'nhiệt huyết': 6, 'sinh động': 5, 'thu hút': 5, 'năng động': 5, 'có tâm': 6,
    'tâm huyết': 6, 'dễ thương': 5, 'gần gũi': 5, 'hay': 4
}

KEYWORDS_CAU15 = {   # Kiểm tra - Đánh giá
    'kiểm tra': 5, 'đánh giá': 5, 'thi': 4, 'đề thi': 5, 'công bằng': 7,
    'minh bạch': 7, 'nghiêm túc': 5, 'khách quan': 6, 'công tâm': 7,
    'cho điểm': 5, 'giữa kỳ': 4, 'cuối kỳ': 4
}

KEYWORDS_CAU16 = {   # Góp ý khác
    'không có ý kiến': 8, 'em ko có': 8, 'em không có': 8, 'ko có ý kiến': 8,
    'không ạ': 7, 'dạ không': 6, 'không góp ý': 7, 'cảm ơn': 5, 'thanks': 5,
    'ok': 4, 'oki': 4, 'ổn': 4, 'không có': 6, 'ko có': 6, 'k': 5
}

def calculate_score(text, keywords_dict):
    """Tính điểm cho một phần text theo từ khóa"""
    if not text or not isinstance(text, str):
        return 0
    text_lower = text.lower()
    score = 0
    for kw, point in keywords_dict.items():
        if kw in text_lower:
            score += point
    return score

def is_no_opinion(text):
    """Kiểm tra xem có phải là dạng 'không có ý kiến' không"""
    if not text:
        return False
    t = text.lower().strip()
    phrases = ['không có ý kiến', 'em ko có', 'em không có', 'ko có ý kiến', 
               'không ạ', 'em không ạ', 'không góp ý', 'ko có góp ý']
    return any(phrase in t for phrase in phrases)

# ====================== GIỮ NGUYÊN 3 CẤP TÁCH DẤU PHẨY ======================
def clean_special_characters(parts):
    return [p.strip() for p in parts if p and p.strip()]

def split_by_condition_1(text):  # Giữ nguyên như code gốc
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

def split_by_condition_2(text):  # Giữ nguyên
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

def split_by_condition_3(text):  # Giữ nguyên
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

def try_create_4th_column(parts):  # Giữ nguyên
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

# ====================== PHÂN LOẠI BẰNG TRỌNG SỐ (MỚI) ======================
def classify_by_scoring(parts):
    """Phân loại bằng trọng số sau khi đã tách bằng 3 cấp"""
    if not parts:
        return "", "", "", ""
    
    valid_parts = clean_special_characters(parts)
    if not valid_parts:
        return "", "", "", ""
    
    cau13 = cau14 = cau15 = cau16 = ""
    
    for i, part in enumerate(valid_parts):
        # Ưu tiên tuyệt đối nếu là "không có ý kiến"
        if is_no_opinion(part):
            if i == 0:                    # Phần đầu tiên
                cau13 = part
            elif i == len(valid_parts) - 2:  # Phần gần cuối (như ví dụ của bạn)
                cau15 = part
            else:
                cau16 = f"{cau16}, {part}" if cau16 else part
            continue
        
        # Tính điểm cho 3 cột chính
        s13 = calculate_score(part, KEYWORDS_CAU13)
        s14 = calculate_score(part, KEYWORDS_CAU14)
        s15 = calculate_score(part, KEYWORDS_CAU15)
        
        max_score = max(s13, s14, s15)
        
        if max_score == 0:
            cau16 = f"{cau16}, {part}" if cau16 else part
        elif max_score == s14:
            cau14 = f"{cau14}, {part}" if cau14 else part
        elif max_score == s15:
            cau15 = f"{cau15}, {part}" if cau15 else part
        else:
            cau13 = f"{cau13}, {part}" if cau13 else part
    
    # Phần cuối cùng luôn ưu tiên vào Cau16 (nếu chưa có)
    if valid_parts and not is_no_opinion(valid_parts[-1]) and not cau16:
        last_part = valid_parts[-1]
        if calculate_score(last_part, KEYWORDS_CAU16) > 0 or len(last_part) < 20:
            cau16 = last_part
    
    # Làm sạch dấu phẩy thừa
    for col in [cau13, cau14, cau15, cau16]:
        col = col.strip(', ')
    
    return cau13.strip(), cau14.strip(), cau15.strip(), cau16.strip()

# ====================== HÀM XỬ LÝ SAU NULL (ĐÃ CẬP NHẬT) ======================
def split_after_null_by_rules(after_null_list, row_number=None):
    """
    1. Dùng 3 cấp tách dấu phẩy (giữ nguyên như code gốc của bạn)
    2. Sau đó dùng trọng số (scoring) để phân loại
    """
    if not after_null_list:
        return ['', '', '', ''], None
   
    original_text = ','.join(after_null_list)
   
    # === 3 CẤP TÁCH DẤU PHẨY (GIỮ NGUYÊN) ===
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success:
            return new_parts[:4], None
   
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success:
            return new_parts[:4], None
   
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success:
            return new_parts[:4], None
   
    # Chọn bộ parts tốt nhất
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
   
    # === PHÂN LOẠI BẰNG TRỌNG SỐ ===
    if len(best_parts) >= 1:
        cau13, cau14, cau15, cau16 = classify_by_scoring(best_parts)
        return [cau13, cau14, cau15, cau16], None
   
    # Fallback
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'level1_result': parts_level1,
        'level2_result': parts_level2,
        'level3_result': parts_level3,
        'final_count': len(best_parts),
        'message': f'Sau 3 cấp có {len(best_parts)} phần'
    }
    return [original_text, '', '', ''], error_info

# ====================== CÁC HÀM CÒN LẠI GIỮ NGUYÊN ======================
# (is_date_format, is_ma_gv_format, process_row, read_csv_manual, main, download/upload...)

# === Giữ nguyên toàn bộ phần dưới này từ code gốc của bạn ===
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

def process_row(row, row_number=None):
    # ... (giữ nguyên toàn bộ hàm process_row của bạn)
    # Tôi giữ nguyên để tránh lỗi, chỉ thay đổi phần sau NULL
    if not row or len(row) < 2:
        return None, None, []
   
    try:
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
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
            'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri,
            'NULL': null_value, 'Cau13': cau13, 'Cau14': cau14,
            'Cau15': cau15, 'Cau16': cau16
        }
       
        return result, None, split_errors
       
    except Exception as e:
        print(f"Lỗi xử lý dòng {row_number}: {e}")
        return None, str(e), []

def read_csv_manual(filename):
    rows = []
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
        return rows, []
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return [], []

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
            process_errors.append({'line_number': idx, 'error': error, 'row_length': len(row)})
       
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
        print(f"CÁC DÒNG KHÔNG PHÂN LOẠI ĐƯỢC ({len(split_errors)} dòng)")
        print(f"{'='*60}")
        for err in split_errors[:10]:
            print(f"\nDòng {err.get('row_number', '?')}:")
            print(f" Chuỗi sau NULL: {err.get('original_after_null', '')[:200]}")
       
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
