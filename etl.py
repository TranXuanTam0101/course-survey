import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ====================== CẤU HÌNH ======================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ====================== TẢI FILE TỪ BLOB ======================
def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        print(f"Đã tải file: {SURVEY_FILE}")
        return True
    except Exception as e:
        print(f"Lỗi tải file từ Blob: {e}")
        sys.exit(1)


# ====================== LOGIC XỬ LÝ CHÍNH ======================
def is_date_format(value: str) -> bool:
    """Kiểm tra định dạng ngày xx/xx/xxxx"""
    if not value or len(value) < 10:
        return False
    parts = value.strip().split('/')
    return len(parts) == 3 and all(p.isdigit() for p in parts) and len(parts[0]) == 2 and len(parts[1]) == 2 and len(parts[2]) == 4


def is_mgv(value: str) -> bool:
    """Kiểm tra MaGV: đúng 7 ký tự và toàn số"""
    return value and len(value.strip()) == 7 and value.strip().isdigit()


def process_row(row: list) -> dict:
    """
    Xử lý một dòng (đã split theo dấu phẩy)
    Trả về dictionary với 18 cột chuẩn hoặc None nếu lỗi nghiêm trọng
    """
    if len(row) < 2:
        return None

    result = {
        'Lop': '', 'MaSV': '', 'HoDem': '', 'Ten': '', 'NgaySinh': '', 'MaHP': '', 'TenHP': '',
        'MaGV': '', 'HoDemGV': '', 'TenGV': '', 'LopHP': '', 'CauHoi': '', 'GiaTri': '',
        'NULL': 'NULL', 'Cau13': '', 'Cau14': '', 'Cau15': '', 'Cau16': ''
    }

    # ==================== PHẦN 1: XỬ LÝ TRƯỚC CỘT NULL ====================
    try:
        result['Lop'] = row[0].strip()
        result['MaSV'] = row[1].strip()

        # Bước 2: Tìm NgaySinh
        ngay_sinh_idx = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh_idx = i
                break
        if ngay_sinh_idx == -1:
            return None  # Không tìm thấy ngày sinh -> bỏ qua dòng

        result['NgaySinh'] = row[ngay_sinh_idx].strip()

        # Bước 3: HoDem và Ten
        ho_dem_ten_parts = []
        for i in range(2, ngay_sinh_idx):
            ho_dem_ten_parts.append(row[i].strip())
        
        ho_dem_ten_str = " ".join(ho_dem_ten_parts).strip()
        if ho_dem_ten_str:
            name_parts = ho_dem_ten_str.split()
            result['Ten'] = name_parts[-1]
            result['HoDem'] = " ".join(name_parts[:-1]) if len(name_parts) > 1 else ""

        # Bước 4: MaHP - ngay sau NgaySinh
        if ngay_sinh_idx + 1 < len(row):
            result['MaHP'] = row[ngay_sinh_idx + 1].strip()

        # Bước 5: Tìm MaGV (đúng 7 số)
        ma_gv_idx = -1
        for i in range(ngay_sinh_idx + 2, len(row)):
            if is_mgv(row[i]):
                ma_gv_idx = i
                break
        if ma_gv_idx == -1:
            return None

        result['MaGV'] = row[ma_gv_idx].strip()

        # Bước 6: TenHP - các cột giữa MaHP và MaGV
        ten_hp_parts = []
        for i in range(ngay_sinh_idx + 2, ma_gv_idx):
            ten_hp_parts.append(row[i].strip())
        result['TenHP'] = " ".join(ten_hp_parts).strip() if ten_hp_parts else ""

        # Bước 7-11: Các cột tiếp theo
        current_idx = ma_gv_idx + 1
        if current_idx < len(row):
            result['HoDemGV'] = row[current_idx].strip()
            current_idx += 1
        if current_idx < len(row):
            result['TenGV'] = row[current_idx].strip()
            current_idx += 1
        if current_idx < len(row):
            result['LopHP'] = row[current_idx].strip()
            current_idx += 1
        if current_idx < len(row):
            result['CauHoi'] = row[current_idx].strip()
            current_idx += 1
        if current_idx < len(row):
            result['GiaTri'] = row[current_idx].strip()
            current_idx += 1

        # Bước 12: Tìm vị trí NULL
        null_idx = current_idx
        if null_idx >= len(row) or row[null_idx].strip().upper() not in ['NULL', '']:
            # Nếu không phải NULL thì tìm tiếp
            for i in range(current_idx, len(row)):
                if row[i].strip().upper() in ['NULL', '']:
                    null_idx = i
                    break
            else:
                null_idx = -1

    except Exception:
        return None

    # ==================== PHẦN 2: XỬ LÝ SAU CỘT NULL ====================
    if null_idx == -1 or null_idx + 1 >= len(row):
        return result  # Không có dữ liệu sau NULL

    # Lấy phần sau NULL
    after_null = [col.strip() for col in row[null_idx + 1:] if col.strip() != '']

    # === Quy tắc 1: Ngay sau dấu phẩy không có khoảng trắng ===
    def split_by_no_space(parts):
        result_list = []
        for p in parts:
            if ',' in p and not p.startswith(' '):  # Có dấu phẩy và ngay sau không có space
                # Tách theo dấu phẩy không có space
                subparts = []
                current = ''
                for char in p:
                    if char == ',' and not current.endswith(' '):  # không có space trước dấu phẩy? Wait, logic là sau phẩy không space
                        if current:
                            subparts.append(current.strip())
                        current = ''
                    else:
                        current += char
                if current:
                    subparts.append(current.strip())
                result_list.extend(subparts)
            else:
                result_list.append(p)
        return result_list

    processed = split_by_no_space(after_null)

    if len(processed) == 4:
        result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16'] = processed
        return result

    # === Quy tắc 2: Không có space sau phẩy + chữ cái đầu viết hoa ===
    def split_by_no_space_and_upper(parts):
        result_list = []
        for p in parts:
            if ',' in p:
                subparts = p.split(',')
                cleaned = []
                for sp in subparts:
                    sp = sp.strip()
                    if sp and sp[0].isupper():  # chữ cái đầu viết hoa
                        cleaned.append(sp)
                    elif sp:
                        # Nếu không viết hoa nhưng là phần tiếp theo, có thể gộp hoặc tách tùy
                        if cleaned:
                            cleaned[-1] += " " + sp
                        else:
                            cleaned.append(sp)
                result_list.extend(cleaned)
            else:
                result_list.append(p)
        return result_list

    processed2 = split_by_no_space_and_upper(after_null)

    if len(processed2) == 4:
        result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16'] = processed2
        return result

    # Nếu vẫn > 4 cột sau cả 2 quy tắc → cần kiểm tra thủ công
    result['Cau13'] = " ".join(after_null) if after_null else ""
    # Đánh dấu dòng lỗi
    result['__ERROR__'] = True
    result['__RAW_AFTER_NULL__'] = "|".join(after_null)

    return result


# ====================== XỬ LÝ TOÀN BỘ FILE ======================
def process_file(input_file: str):
    error_lines = []
    output_rows = []

    print(f"Đang xử lý file: {input_file}")

    with open(input_file, 'r', encoding='utf-8-sig') as f:
        # Giả sử dòng đầu là header, bỏ qua hoặc xử lý tùy file
        header = f.readline().strip()
        
        for line_num, line in enumerate(f, start=2):
            line = line.strip()
            if not line:
                continue

            # Split theo dấu phẩy, nhưng giữ nguyên các phần có dấu phẩy trong ngoặc (nếu có)
            # Ở đây dùng split đơn giản, nếu có trường hợp tên có dấu phẩy phức tạp hơn thì cần csv.reader
            row = [x.strip() for x in line.split(',')]

            processed = process_row(row)

            if processed is None:
                error_lines.append(f"Dòng {line_num}: Không xử lý được (thiếu thông tin cơ bản)")
                continue

            # Tạo list theo thứ tự cột chuẩn
            output_row = [
                processed['Lop'], processed['MaSV'], processed['HoDem'], processed['Ten'],
                processed['NgaySinh'], processed['MaHP'], processed['TenHP'], processed['MaGV'],
                processed['HoDemGV'], processed['TenGV'], processed['LopHP'], processed['CauHoi'],
                processed['GiaTri'], processed['NULL'],
                processed['Cau13'], processed['Cau14'], processed['Cau15'], processed['Cau16']
            ]

            output_rows.append(output_row)

            if processed.get('__ERROR__'):
                error_lines.append(f"Dòng {line_num}: Sau NULL vẫn còn {len(processed.get('__RAW_AFTER_NULL__', '').split('|'))} phần -> {processed.get('__RAW_AFTER_NULL__', '')}")

    # Tạo DataFrame
    columns = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV',
               'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'GiaTri', 'NULL',
               'Cau13', 'Cau14', 'Cau15', 'Cau16']

    df = pd.DataFrame(output_rows, columns=columns)

    # Lưu các dòng lỗi ra file txt
    if error_lines:
        error_file = f"{FILE_NAME}_ERROR_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        with open(error_file, 'w', encoding='utf-8') as f:
            f.write("\n".join(error_lines))
        print(f"Đã lưu {len(error_lines)} dòng cần kiểm tra thủ công vào: {error_file}")

    print(f"Hoàn thành xử lý. Tổng số dòng output: {len(df)}")
    return df


# ====================== UPLOAD LÊN BLOB ======================
def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        
        blob_client = processed_container.get_blob_client(output_path)
        blob_client.upload_blob(output, overwrite=True)
        print(f"Đã upload file xử lý lên Blob: {output_path}")
        return True
    except Exception as e:
        print(f"Lỗi upload Blob: {e}")
        return False


# ====================== MAIN ======================
if __name__ == "__main__":
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    # Tải file từ Blob
    download_from_blob(blob_service)

    # Xử lý file
    df_result = process_file(SURVEY_FILE)

    # Tên file output
    output_filename = f"{FILE_NAME}_PROCESSED_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    # Upload kết quả
    success = upload_to_blob(blob_service, df_result, f"{SEMESTER}/{output_filename}")

    if success:
        print("Hoàn tất toàn bộ quá trình xử lý!")
    else:
        print("Có lỗi khi upload kết quả.")
