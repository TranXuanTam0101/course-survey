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

KEYWORDS_CAU13 = [
    'nội dung', 'chuẩn đầu ra', 'chương trình', 'học phần', 'môn học',
    'đáp ứng', 'phù hợp', 'bám sát', 'rõ ràng', 'đầy đủ', 'hợp lý', 'hợp lí',
    'sát chương trình', 'dễ tiếp cận', 'kiến thức', 'trang bị', 'cung cấp',
    'đào tạo', 'mục tiêu', 'chất lượng', 'đảm bảo', 'bổ ích', 'cần thiết',
    'quan trọng', 'trọng tâm', 'chi tiết', 'cụ thể', 'đúng', 'chuẩn', 'ổn', 'hay', 'phương pháp'
]

KEYWORDS_CAU14 = [
    'thầy', 'cô', 'giảng viên', 'gv', 'thầy giáo', 'cô giáo',
    'dạy', 'giảng', 'bài giảng', 'dễ hiểu', 'nhiệt tình', 'tận tâm',
    'tận tình', 'vui vẻ', 'thân thiện', 'hấp dẫn', 'thú vị', 'tương tác',
    'sôi nổi', 'truyền đạt', 'giải thích', 'hướng dẫn', 'phương pháp',
    'nhiệt huyết', 'rõ', 'kỹ', 'sinh động', 'linh hoạt', 'đa dạng', 'thu hút',
    'ví dụ thực tế', 'dẫn dắt', 'năng động', 'đáng yêu', 'dễ thương', 'dễ mến',
    'dễ gần', 'tâm lý', 'thấu hiểu', 'quan tâm', 'chu đáo', 'tận tụy',
    'sẵn sàng giúp đỡ', 'giải đáp thắc mắc', 'hỗ trợ', 'chỉ bảo', 'năng nổ',
    'có tâm', 'truyền cảm hứng', 'gần gũi', 'thoải mái', 'hào hứng', 'vui',
    'hòa đồng', 'thương học trò', 'thực tế', 'thực tiễn'
]

KEYWORDS_CAU15 = [
    'kiểm tra', 'đánh giá', 'thi', 'bài tập', 'điểm', 'chấm', 'đề thi',
    'công bằng', 'minh bạch', 'nghiêm túc', 'phù hợp', 'thực lực',
    'khách quan', 'công tâm', 'đề kiểm tra', 'giữa kỳ', 'cuối kỳ',
    'bài kiểm tra', 'cho điểm', 'công khai', 'đảm bảo tính công bằng',
    'nghiêm ngặt', 'đánh giá đúng', 'phản ánh đúng', 'thuyết phục',
    'chính xác', 'kỹ càng', 'chỉnh chu', 'đa dạng hình thức'
]

KEYWORDS_CAU16 = [
    'không', 'ko', 'ok', 'oki', 'ổn', 'được', 'không có', 'không ạ',
    'dạ không', 'không có ý kiến', 'hết', 'xong', 'cảm ơn', 'thanks',
    'k', 'không góp ý', 'không có góp ý', 'không góp ý gì', 'không ý kiến',
    'em không có', 'dạ không có', 'ko có', 'không gì', 'không có gì',
    'không còn góp ý', 'mãi yêu cô', 'yêu cô', 'cảm ơn cô', 'cảm ơn thầy',
    'tuyệt vời', 'quá ok', 'rất ok', 'ổn hết'
]

# Giá trị đặc biệt cho Cau16 (chỉ lấy phần tử cuối)
CAU16_SPECIAL_VALUES = {'không', 'k', 'KHÔNG'}


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


def is_special_character(text):
    """Kiểm tra xem text có phải là ký tự đặc biệt cần gán None không"""
    if not text or not isinstance(text, str):
        return True
    
    text = text.strip()
    if text == "":
        return True
    
    special_patterns = [
        r'^\.+$', r'^[,\.]+$', r'^[,]+$', r'^[mnkzjx]$',
        r'^[mnkzjx]{2,5}$', r'^[0-9]+$', r'^[a-zA-Z]{1,3}$',
        r'^[,/]+$', r'^[.,/]+$', r'^[!@#$%^&*()]+$'
    ]
    
    for pattern in special_patterns:
        if re.match(pattern, text):
            return True
    
    meaningful_words = ['ok', 'ko', 'kh', 'không', 'cô', 'thầy', 'dạy', 'hay', 'tốt']
    if len(text) <= 2 and not any(kw in text.lower() for kw in meaningful_words):
        return True
    
    garbage_patterns = [r'^nhm', r'^bdv', r'^ebq', r'^dbq', r'^zswej', r'^dsjfr', r'^sdhsd']
    for pattern in garbage_patterns:
        if re.match(pattern, text.lower()):
            return True
    
    return False


def split_by_condition_1(text):
    """Cấp 1: Tách nếu trước và sau dấu phẩy đều không có khoảng trắng"""
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
    """Cấp 2: Tách nếu sau dấu phẩy không có khoảng trắng"""
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
    """Cấp 3: Tách nếu sau dấu phẩy không có khoảng trắng VÀ chữ hoa"""
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
    """Thử tạo cột thứ 4 từ 3 phần tử"""
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


def classify_parts_by_rules(parts):
    """
    Phân loại các phần tử vào 4 cột dựa trên logic đã định nghĩa:
    - Duyệt từ trái sang phải
    - current_col bắt đầu từ Cau13
    - Mỗi phần tử chỉ có thể ở cột hiện tại hoặc cột kế tiếp
    """
    if not parts:
        return "", "", "", ""
    
    # Lọc bỏ phần tử rỗng và ký tự đặc biệt
    valid_parts = [p for p in parts if p and not is_special_character(p)]
    
    if not valid_parts:
        return "", "", "", ""
    
    # Khởi tạo kết quả
    cau13 = ""
    cau14 = ""
    cau15 = ""
    cau16 = ""
    
    # P1 luôn vào Cau13
    cau13 = valid_parts[0]
    
    # Xác định phần tử cuối và xử lý đặc biệt
    last_part = valid_parts[-1]
    is_special_last = last_part.strip() in CAU16_SPECIAL_VALUES
    
    # Các phần tử còn lại (từ P2 đến P_{n-1} hoặc đến P_n nếu không phải special)
    if is_special_last:
        # TH1: Phần tử cuối là "không", "k", "KHÔNG"
        cau16 = last_part
        remaining_parts = valid_parts[1:-1]  # Bỏ qua P1 và P_last
    else:
        # TH2: Phần tử cuối không phải special, đưa vào xử lý chung
        remaining_parts = valid_parts[1:]  # Bỏ qua P1
        cau16 = ""  # Sẽ được xác định sau
    
    # current_col: 1=Cau13, 2=Cau14, 3=Cau15, 4=Cau16
    current_col = 1
    
    for part in remaining_parts:
        # Xác định từ khóa của phần tử
        has_cau13 = has_keyword(part, KEYWORDS_CAU13)
        has_cau14 = has_keyword(part, KEYWORDS_CAU14)
        has_cau15 = has_keyword(part, KEYWORDS_CAU15)
        has_cau16 = has_keyword(part, KEYWORDS_CAU16)
        
        # Xử lý dựa trên current_col
        if current_col == 1:  # Đang ở Cau13
            if has_cau13:
                # Có từ khóa Cau13, gán vào Cau13
                cau13 = f"{cau13}, {part}" if cau13 else part
            elif has_cau14:
                # Có từ khóa Cau14, chuyển sang Cau14 và gán vào Cau14
                current_col = 2
                cau14 = part if not cau14 else f"{cau14}, {part}"
            else:
                # Không có từ khóa, gán vào Cau13
                cau13 = f"{cau13}, {part}" if cau13 else part
        
        elif current_col == 2:  # Đang ở Cau14
            if has_cau15:
                # Có từ khóa Cau15, chuyển sang Cau15 và gán vào Cau15
                current_col = 3
                cau15 = part if not cau15 else f"{cau15}, {part}"
            else:
                # Không có từ khóa Cau15, gán vào Cau14
                cau14 = part if not cau14 else f"{cau14}, {part}"
        
        elif current_col == 3:  # Đang ở Cau15
            if has_cau16:
                # Có từ khóa Cau16, chuyển sang Cau16 và gán vào Cau16
                current_col = 4
                cau16 = part if not cau16 else f"{cau16}, {part}"
            else:
                # Không có từ khóa Cau16, gán vào Cau15
                cau15 = part if not cau15 else f"{cau15}, {part}"
        
        else:  # current_col == 4, đang ở Cau16
            cau16 = part if not cau16 else f"{cau16}, {part}"
    
    # Xử lý TH2: Nếu chưa có Cau16 và vẫn còn cột, gán giá trị mặc định
    if not is_special_last and not cau16:
        cau16 = ""
    
    # Đảm bảo không có cột nào bị rỗng
    if not cau13:
        cau13 = "Không có đánh giá"
    if not cau14:
        cau14 = "Không có đánh giá"
    if not cau15:
        cau15 = "Không có đánh giá"
    if not cau16:
        cau16 = "Không có góp ý"
    
    return cau13, cau14, cau15, cau16


def split_after_null_by_rules(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL:
    1. Dùng 3 cấp rule-based để tách
    2. Phân loại theo vị trí + từ khóa
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    # CẤP 1
    parts = split_by_condition_1(original_text)
    if len(parts) == 4:
        return classify_parts_by_rules(parts), None
    if len(parts) == 3:
        success, new_parts = try_create_4th_column(parts)
        if success:
            return classify_parts_by_rules(new_parts), None
    
    # CẤP 2
    parts = split_by_condition_2(original_text)
    if len(parts) == 4:
        return classify_parts_by_rules(parts), None
    if len(parts) == 3:
        success, new_parts = try_create_4th_column(parts)
        if success:
            return classify_parts_by_rules(new_parts), None
    
    # CẤP 3
    parts = split_by_condition_3(original_text)
    if len(parts) == 4:
        return classify_parts_by_rules(parts), None
    if len(parts) == 3:
        success, new_parts = try_create_4th_column(parts)
        if success:
            return classify_parts_by_rules(new_parts), None
    
    # Nếu không phân loại được -> để toàn bộ vào cột đầu
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'split_parts': parts,
        'message': f'Tách được {len(parts)} cột, không phân loại được'
    }
    return [original_text, '', '', ''], error_info


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
            split_result, error = split_after_null_by_rules(after_null, row_number)
            
            if split_result:
                cau13, cau14, cau15, cau16 = split_result
            
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
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                row = line.split(',')
                row = [col.strip() for col in row]
                rows.append(row)
                if line_num % 10000 == 0:
                    print(f"Đã đọc {line_num} dòng...")
        print(f"Đã đọc xong file: {len(rows)} dòng")
        return rows, []
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
    rows, _ = read_csv_manual(SURVEY_FILE)
    
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
        
        if idx % 10000 == 0:
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
        
        split_error_df = pd.DataFrame(split_errors)
        split_error_filename = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(split_error_filename, index=False, encoding='utf-8-sig')
        print(f"Đã lưu {len(split_errors)} dòng lỗi vào file: {split_error_filename}")
    
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
