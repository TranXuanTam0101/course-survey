import os
import sys
import re
import pandas as pd
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ========== TỪ KHÓA CHO CÁC CỘT ==========
KEYWORDS_CAU13 = [
    'nội dung', 'chuẩn đầu ra', 'chương trình', 'học phần', 'môn học',
    'đáp ứng', 'phù hợp', 'bám sát', 'rõ ràng', 'đầy đủ', 'hợp lý', 'hợp lí',
    'sát chương trình', 'dễ tiếp cận', 'kiến thức cơ bản', 'trang bị',
    'cung cấp', 'đào tạo', 'mục tiêu', 'chất lượng', 'đảm bảo',
    'bổ ích', 'cần thiết', 'quan trọng', 'trọng tâm', 'chi tiết',
    'cụ thể', 'đúng', 'chuẩn', 'ổn', 'hay', 'được', 'phương pháp', 'tệ','không','ko','k','rõ rãng'
]

KEYWORDS_CAU14 = [
    'thầy', 'cô', 'giảng viên', 'gv', 'thầy giáo', 'cô giáo',
    'dạy', 'giảng', 'bài giảng', 'dễ hiểu', 'nhiệt tình', 
    'tận tâm', 'tận tình', 'vui vẻ', 'thân thiện', 'hấp dẫn', 
    'thú vị', 'tương tác', 'sôi nổi', 'truyền đạt', 'giải thích', 
    'hướng dẫn', 'phương pháp', 'nhiệt huyết', 'rõ', 'kỹ',
    'sinh động', 'linh hoạt', 'đa dạng', 'thu hút',
    'ví dụ thực tế', 'dẫn dắt', 'tạo hứng thú', 'năng động', 
    'đáng yêu', 'dễ thương', 'dễ mến', 'dễ gần', 'tâm lý', 
    'thấu hiểu', 'quan tâm', 'chu đáo', 'tận tụy', 'sẵn sàng giúp đỡ',
    'giải đáp thắc mắc', 'hỗ trợ', 'chỉ bảo', 'năng nổ', 'có tâm',
    'truyền cảm hứng', 'gần gũi', 'thoải mái', 'hào hứng', 'vui',
    'dui dẻ', 'hòa đồng', 'thương học trò','hay','tâm huyết'
]

KEYWORDS_CAU15 = [
    'kiểm tra', 'đánh giá', 'thi', 'bài tập', 'điểm', 'chấm',
    'đề thi', 'công bằng', 'minh bạch', 'nghiêm túc', 'phù hợp',
    'thực lực', 'khách quan', 'công tâm', 'đề kiểm tra',
    'giữa kỳ', 'cuối kỳ', 'bài kiểm tra', 'cho điểm',
    'công khai', 'rõ ràng', 'đảm bảo tính công bằng',
    'nghiêm ngặt', 'đánh giá đúng', 'phản ánh đúng', 'thuyết phục',
    'chính xác', 'kỹ càng', 'chỉnh chu', 'đa dạng hình thức', 'tài liệu', 'đọc thêm','không','ko','k','công tác'
]

KEYWORDS_CAU16 = [
    'không', 'ko', 'ok', 'oki', 'ổn', 'được',
    'không có', 'không ạ', 'dạ không', 'không có ý kiến',
    'hết', 'xong', 'cảm ơn', 'thanks', 'k', 'không góp ý',
    'không có góp ý', 'không góp ý gì', 'không ý kiến',
    'em không có', 'dạ không có', 'không có ạ', 'ko có',
    'không gì', 'không có gì', 'không còn góp ý',
    'mãi yêu cô', 'yêu cô', 'cảm ơn cô', 'cảm ơn thầy',
    'tuyệt vời', 'quá ok', 'rất ok', 'ổn hết', 'tốt hơn'
]


def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        sys.exit(1)


def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        return True
    except Exception as e:
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
    if not text or not isinstance(text, str):
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)


def clean_special_characters(parts):
    return [p for p in parts if p and p.strip()]


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


def classify_general_parts(parts):
    valid_parts = clean_special_characters(parts)
    
    if not valid_parts:
        return "", "", "", ""
    
    current_col = "Cau13"
    cau13 = valid_parts[0]
    cau14 = ""
    cau15 = ""
    cau16 = ""
    
    if len(valid_parts) == 1:
        return cau13, cau14, cau15, cau16
    
    remaining_parts = valid_parts[1:]
    
    last_part = remaining_parts[-1]
    is_special_last = has_keyword(last_part, KEYWORDS_CAU16) and last_part.lower() in ['không', 'k', 'không có', 'ko']
    
    if is_special_last:
        cau16 = last_part
        remaining_parts = remaining_parts[:-1]
    else:
        cau16 = last_part
        remaining_parts = remaining_parts[:-1]
    
    for part in remaining_parts:
        if current_col == "Cau13":
            if has_keyword(part, KEYWORDS_CAU13):
                cau13 = f"{cau13}, {part}"
            elif has_keyword(part, KEYWORDS_CAU14):
                current_col = "Cau14"
                cau14 = part
            else:
                cau13 = f"{cau13}, {part}"
        
        elif current_col == "Cau14":
            if has_keyword(part, KEYWORDS_CAU14):
                cau14 = f"{cau14}, {part}"
            elif has_keyword(part, KEYWORDS_CAU15):
                current_col = "Cau15"
                cau15 = part
            else:
                current_col = "Cau15"
                cau15 = part
        
        elif current_col == "Cau15":
            if has_keyword(part, KEYWORDS_CAU15):
                cau15 = f"{cau15}, {part}"
            elif has_keyword(part, KEYWORDS_CAU16):
                current_col = "Cau16"
                cau16 = f"{cau16}, {part}" if cau16 else part
            else:
                current_col = "Cau16"
                cau16 = f"{cau16}, {part}" if cau16 else part
        
        else:
            cau16 = f"{cau16}, {part}" if cau16 else part
    
    return cau13, cau14, cau15, cau16


def classify_by_position_and_keywords(parts):
    num_parts = len(parts)
    
    if num_parts == 5:
        return classify_5_parts(parts)
    elif num_parts == 6:
        return classify_6_parts(parts)
    else:
        return classify_general_parts(parts)


def classify_5_parts(parts):
    valid_parts = clean_special_characters(parts)
    
    if len(valid_parts) < 5:
        return classify_general_parts(valid_parts)
    
    P1, P2, P3, P4, P5 = valid_parts
    
    cau13 = P1
    cau16 = P5
    cau14 = ""
    cau15 = ""
    
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14 = P2
        if has_keyword(P3, KEYWORDS_CAU14):
            cau14 = f"{cau14}, {P3}"
            cau15 = P4
        else:
            cau15 = P3
            cau15 = f"{cau15}, {P4}" if P4 else cau15
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14 = P3
        cau13 = f"{cau13}, {P2}"
        cau15 = P4
    else:
        cau13 = f"{cau13}, {P2}"
        if has_keyword(P4, KEYWORDS_CAU15):
            cau15 = P4
        else:
            cau14 = P3
            cau15 = P4
    
    return cau13, cau14, cau15, cau16


def classify_6_parts(parts):
    valid_parts = clean_special_characters(parts)
    
    if len(valid_parts) < 6:
        return classify_general_parts(valid_parts)
    
    P1, P2, P3, P4, P5, P6 = valid_parts
    
    cau13 = P1
    cau16 = P6
    cau14 = ""
    cau15 = ""
    
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14 = P2
        cau14 = f"{cau14}, {P3}"
        if has_keyword(P4, KEYWORDS_CAU15):
            cau15 = P4
            cau15 = f"{cau15}, {P5}" if P5 else cau15
        else:
            cau14 = f"{cau14}, {P4}"
            cau15 = P5
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14 = P3
        cau14 = f"{cau14}, {P4}"
        cau13 = f"{cau13}, {P2}"
        cau15 = P5
    else:
        cau13 = f"{cau13}, {P2}"
        if has_keyword(P3, KEYWORDS_CAU15):
            cau15 = P3
            cau15 = f"{cau15}, {P4}" if P4 else cau15
            cau15 = f"{cau15}, {P5}" if P5 else cau15
        else:
            cau14 = P3
            cau15 = P4
            cau15 = f"{cau15}, {P5}" if P5 else cau15
    
    return cau13, cau14, cau15, cau16


def split_after_null_by_rules(after_null_list, row_number=None):
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
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
    
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    if len(best_parts) >= 2:
        cau13, cau14, cau15, cau16 = classify_by_position_and_keywords(best_parts)
        if cau13 or cau14 or cau15 or cau16:
            return [cau13, cau14, cau15, cau16], None
    
    return [original_text, '', '', ''], None


def process_row(row, row_number=None):
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
        return None, str(e), []


def read_csv_manual(filename):
    rows = []
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                row = line.split(',')
                rows.append([col.strip() for col in row])
        return rows
    except Exception as e:
        return []


def main():
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        sys.exit(1)
    
    download_from_blob(blob_service)
    rows = read_csv_manual(SURVEY_FILE)
    
    if not rows:
        sys.exit(1)
    
    processed_rows = []
    
    for idx, row in enumerate(rows, 1):
        result, error, split_errs = process_row(row, idx)
        if result:
            processed_rows.append(result)
    
    if len(processed_rows) > 0:
        result_df = pd.DataFrame(processed_rows)
        output_filename = f"{FILE_NAME}_processed.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        upload_to_blob(blob_service, result_df, output_path)


if __name__ == "__main__":
    main()
