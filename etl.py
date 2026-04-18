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


# ==================== CÁC HÀM TÁCH CẤP 1,2,3 (GIỮ NGUYÊN) ====================
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
    if not after_null_list:
        return ['', '', '', ''], None

    original_text = ','.join(after_null_list)

    # Cấp 1
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success:
            return new_parts[:4], None

    # Cấp 2
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success:
            return new_parts[:4], None

    # Cấp 3
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success:
            return new_parts[:4], None

    # Nếu vẫn không đủ 4 cột → để toàn bộ vào cột đầu
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'level1_result': parts_level1,
        'level2_result': parts_level2,
        'level3_result': parts_level3,
        'final_count': len(parts_level3),
        'message': f'Sau 3 cấp có {len(parts_level3)} cột - để TOÀN BỘ vào cột đầu'
    }
    return [original_text, '', '', ''], error_info


# ==================== HÀM XỬ LÝ THÊM CHO PHẦN SAU NULL ====================
def is_only_special_chars(text: str) -> bool:
    """Kiểm tra chuỗi chỉ chứa ký tự đặc biệt (m, n, dấu câu, khoảng trắng...)"""
    if not text or len(text.strip()) == 0:
        return True

    cleaned = text.strip()
    # Loại bỏ chữ cái tiếng Việt + tiếng Anh
    no_letters = re.sub(r'[a-zA-ZÀ-ỹạ-ỹ]', '', cleaned)

    if len(cleaned) <= 8 and len(no_letters) / max(len(cleaned), 1) > 0.7:
        return True

    # Chỉ còn dấu câu, m, n, số, khoảng trắng
    if re.match(r'^[.,;:!?_\-\s|*/\\mMnN0-9]+$', cleaned):
        return True

    return False


def post_process_cau13(cau13_text: str):
    """
    Xử lý chuỗi nằm toàn bộ trong Cau13:
    - Nếu chỉ ký tự đặc biệt → trả về rỗng hết 4 cột
    - Tách phần cuối là góp ý → Cau16
    - Gán theo từ khóa theo thứ tự Cau13 → Cau14 → Cau15
    """
    if not cau13_text or len(cau13_text.strip()) < 2:
        return '', '', '', ''

    text = cau13_text.strip()

    # Bước 1: Kiểm tra ký tự đặc biệt
    if is_only_special_chars(text):
        return '', '', '', ''

    # Từ khóa theo từng cột
    keywords = {
        'cau13': ['chuẩn đầu ra', 'nội dung', 'đầy đủ', 'phù hợp', 'bám sát', 'rõ ràng', 'hợp lý', 'chuẩn', 'sát ngành'],
        'cau14': ['dạy', 'giảng', 'nhiệt tình', 'dễ hiểu', 'tận tâm', 'vui vẻ', 'hay', 'thú vị', 'thầy', 'cô dạy', 'giảng viên'],
        'cau15': ['kiểm tra', 'đánh giá', 'công bằng', 'minh bạch', 'nghiêm túc', 'chấm điểm', 'onl', 'kiểm tra onl'],
    }

    special_ends = ['không', 'ko', 'không có', 'dạ không', 'em không', 'không có ạ', 'không ạ',
                    'ok', 'ko có', 'không có góp ý', 'ổn', 'tốt']

    # Tách theo dấu phẩy
    parts = [p.strip() for p in re.split(r'\s*,\s*', text) if p.strip()]

    # Bước 2: Tách phần cuối làm Cau16
    cau16 = ''
    if parts and any(parts[-1].lower().startswith(s.lower()) or parts[-1].lower() == s.lower() for s in special_ends):
        cau16 = parts.pop()

    # Bước 3: Gán theo từ khóa
    result = ['', '', '', cau16]

    for part in parts:
        assigned = False
        for i, key_list in enumerate([keywords['cau13'], keywords['cau14'], keywords['cau15']]):
            if any(kw.lower() in part.lower() for kw in key_list):
                if not result[i]:
                    result[i] = part
                    assigned = True
                    break
        if not assigned:
            # Gán vào cột trống theo thứ tự
            for j in range(3):
                if not result[j]:
                    result[j] = part
                    break
            else:
                # Nếu đầy → gộp vào Cau15
                if result[2]:
                    result[2] += " | " + part
                else:
                    result[2] = part

    # Bước 4: Clean up ký tự đặc biệt
    for i in range(3):
        if is_only_special_chars(result[i]):
            result[i] = ''

    return result[0], result[1], result[2], result[3]


# ==================== XỬ LÝ MỘT DÒNG ====================
def process_row(row, row_number=None):
    if not row or len(row) < 2:
        return None, None, []

    try:
        # ==================== PHẦN 1: TRƯỚC NULL ====================
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''

        # Tìm NgaySinh
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break

        # Tách HoDem - Ten
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join(p.strip() for p in ho_dem_ten_parts if p.strip())
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                ten = parts[-1]
                ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''

        # MaHP
        ma_hp = row[ngay_sinh_index + 1].strip() if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row) else ''

        # Tìm MaGV
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break

        # TenHP
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join(p.strip() for p in ten_hp_parts if p.strip())

        # Các cột sau MaGV
        ho_dem_gv = row[ma_gv_index + 1].strip() if ma_gv_index >= 0 and ma_gv_index + 1 < len(row) else ''
        ten_gv = row[ma_gv_index + 2].strip() if ma_gv_index >= 0 and ma_gv_index + 2 < len(row) else ''
        lop_hp = row[ma_gv_index + 3].strip() if ma_gv_index >= 0 and ma_gv_index + 3 < len(row) else ''
        cau_hoi = row[ma_gv_index + 4].strip() if ma_gv_index >= 0 and ma_gv_index + 4 < len(row) else ''
        gia_tri = row[ma_gv_index + 5].strip() if ma_gv_index >= 0 and ma_gv_index + 5 < len(row) else ''

        # Tìm NULL
        null_index = -1
        null_value = ''
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
                null_value = potential_null if potential_null else 'NULL'

        # ==================== PHẦN 2: SAU NULL ====================
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

            # === ÁP DỤNG POST PROCESS NẾU TOÀN BỘ VẪN NẰM Ở CAU13 ===
            if cau13 and not cau14 and not cau15 and not cau16:
                cau13, cau14, cau15, cau16 = post_process_cau13(cau13)

            if error:
                split_errors.append(error)

        # Tạo dict kết quả 18 cột
        result = {
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
            'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri,
            'NULL': null_value,
            'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
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
                row = [col.strip() for col in line.split(',')]
                rows.append(row)

                if line_num % 1000 == 0:
                    print(f"Đã đọc {line_num} dòng...")
        print(f"Đã đọc xong: {len(rows)} dòng")
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
        print("Không có dữ liệu")
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
            process_errors.append({'line_number': idx, 'error': error})

        if split_errs:
            split_errors.extend(split_errs)

        if idx % 1000 == 0:
            print(f"Đã xử lý {idx}/{len(rows)} dòng...")

    result_df = pd.DataFrame(processed_rows)

    print(f"\n{'='*60}")
    print("BÁO CÁO XỬ LÝ")
    print(f"{'='*60}")
    print(f"Tổng dòng: {len(rows)} | Thành công: {len(processed_rows)} | Lỗi: {len(process_errors)}")

    if split_errors:
        print(f"\nCó {len(split_errors)} dòng cần kiểm tra thủ công (đã để toàn bộ vào Cau13)")
        split_error_df = pd.DataFrame(split_errors)
        err_file = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(err_file, index=False, encoding='utf-8-sig')
        print(f"Đã lưu file lỗi: {err_file}")

    if processed_rows:
        output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_path = f"{SEMESTER}/{output_filename}"

        if upload_to_blob(blob_service, result_df, output_path):
            print(f"\nTHÀNH CÔNG! File kết quả: {output_path}")
        else:
            print("Upload thất bại!")
    else:
        print("Không có dòng nào được xử lý!")


if __name__ == "__main__":
    main()
