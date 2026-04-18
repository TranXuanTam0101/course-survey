import os
import sys
import re
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
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
    """Kiểm tra định dạng ngày tháng xx/xx/xxxx"""
    if not isinstance(value, str):
        return False
    pattern = r'^\d{2}/\d{2}/\d{4}$'
    return bool(re.match(pattern, value.strip()))

def is_7_digit_number(value):
    """Kiểm tra cột có đúng 7 ký tự toàn số"""
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{7}$', value.strip()))

def find_column_index(row, start_idx, condition_func):
    """Dò tìm cột thỏa mãn điều kiện từ vị trí start_idx trở đi"""
    for i in range(start_idx, len(row)):
        if condition_func(row[i]):
            return i
    return -1

def get_values_between(row, start_idx, end_idx):
    """Lấy giá trị giữa 2 index (không bao gồm start_idx và end_idx)"""
    if start_idx + 1 >= end_idx:
        return []
    return row[start_idx + 1:end_idx]

def split_hodemten(hodemten_str):
    """Tách HoDemTen thành HoDem và Ten"""
    if not hodemten_str or not isinstance(hodemten_str, str):
        return "", ""
    parts = hodemten_str.strip().split()
    if len(parts) == 0:
        return "", ""
    ten = parts[-1]
    hodem = " ".join(parts[:-1])
    return hodem, ten

def process_th1(row):
    """Xử lý TH1: index 13 = NULL, gán trực tiếp theo index"""
    result = {
        'Lop': row[0] if len(row) > 0 else '',
        'MaSV': row[1] if len(row) > 1 else '',
        'HoDem': row[2] if len(row) > 2 else '',
        'Ten': row[3] if len(row) > 3 else '',
        'NgaySinh': row[4] if len(row) > 4 else '',
        'MaHP': row[5] if len(row) > 5 else '',
        'TenHP': row[6] if len(row) > 6 else '',
        'MaGV': row[7] if len(row) > 7 else '',
        'HoDemGV': row[8] if len(row) > 8 else '',
        'TenGV': row[9] if len(row) > 9 else '',
        'LopHP': row[10] if len(row) > 10 else '',
        'CauHoi': row[11] if len(row) > 11 else '',
        'GiaTri': row[12] if len(row) > 12 else '',
        'NULL': row[13] if len(row) > 13 else '',
        'Cau13': row[14] if len(row) > 14 else '',
        'Cau14': row[15] if len(row) > 15 else '',
        'Cau15': row[16] if len(row) > 16 else '',
        'Cau16': row[17] if len(row) > 17 else ''
    }
    return result

def process_th2(row):
    """Xử lý TH2: index 13 != NULL và index 14 != NULL"""
    result = {col: '' for col in [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV',
        'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'GiaTri', 'NULL',
        'Cau13', 'Cau14', 'Cau15', 'Cau16'
    ]}
    
    # Bước 1: Lấy cột cố định
    result['Lop'] = row[0] if len(row) > 0 else ''
    result['MaSV'] = row[1] if len(row) > 1 else ''
    
    # Bước 2: Dò tìm NgaySinh
    ngaysinh_idx = find_column_index(row, 2, is_date_format)
    if ngaysinh_idx == -1:
        return result
    result['NgaySinh'] = row[ngaysinh_idx]
    
    # Bước 3: Tạo HoDemTen từ giữa MaSV và NgaySinh
    # MaSV ở index 1, NgaySinh ở ngaysinh_idx
    between_ma_and_dob = get_values_between(row, 1, ngaysinh_idx)
    hodemten_str = ' '.join(str(v) for v in between_ma_and_dob if v is not None and str(v) != 'nan')
    hodem, ten = split_hodemten(hodemten_str)
    result['HoDem'] = hodem
    result['Ten'] = ten
    
    # Bước 4: MaHP là cột ngay sau NgaySinh
    mahp_idx = ngaysinh_idx + 1
    if mahp_idx < len(row):
        result['MaHP'] = row[mahp_idx]
    
    # Bước 5: Dò tìm MaGV (7 số) từ sau MaHP
    magv_idx = find_column_index(row, mahp_idx + 1, is_7_digit_number)
    if magv_idx == -1:
        return result
    result['MaGV'] = row[magv_idx]
    
    # Bước 6: Tạo TenHP là giá trị các cột giữa MaHP và MaGV
    between_mahp_magv = get_values_between(row, mahp_idx, magv_idx)
    if between_mahp_magv:
        # Nếu có nhiều cột, gộp thành chuỗi có khoảng trắng
        result['TenHP'] = ' '.join(str(v) for v in between_mahp_magv if v is not None and str(v) != 'nan')
    
    # Bước 7: HoDemGV là cột ngay sau TenHP (sau MaGV? Cần xác định)
    # Theo logic: cột ngay sau TenHP (TenHP nằm giữa MaHP và MaGV, nên sau TenHP là MaGV)
    # Nhưng theo mô tả: Cột ngay sau TenHP là HoDemGV
    # Thực tế: HoDemGV nằm sau MaGV
    hodemgv_idx = magv_idx + 1
    if hodemgv_idx < len(row):
        result['HoDemGV'] = row[hodemgv_idx]
    
    # Bước 8: TenGV là cột ngay sau HoDemGV
    tengv_idx = hodemgv_idx + 1
    if tengv_idx < len(row):
        result['TenGV'] = row[tengv_idx]
    
    # Bước 9: LopHP là cột ngay sau TenGV
    lophp_idx = tengv_idx + 1
    if lophp_idx < len(row):
        result['LopHP'] = row[lophp_idx]
    
    # Bước 10: CauHoi là cột ngay sau LopHP
    cauhoi_idx = lophp_idx + 1
    if cauhoi_idx < len(row):
        result['CauHoi'] = row[cauhoi_idx]
    
    # Bước 11: GiaTri là cột ngay sau CauHoi
    giatri_idx = cauhoi_idx + 1
    if giatri_idx < len(row):
        result['GiaTri'] = row[giatri_idx]
    
    # Bước 12: Cột NULL là cột ngay sau GiaTri
    null_idx = giatri_idx + 1
    if null_idx < len(row):
        result['NULL'] = row[null_idx]
    
    # Bước 13: Các câu hỏi từ Cau13 đến Cau16
    cau13_idx = null_idx + 1
    if cau13_idx < len(row):
        result['Cau13'] = row[cau13_idx]
    cau14_idx = cau13_idx + 1
    if cau14_idx < len(row):
        result['Cau14'] = row[cau14_idx]
    cau15_idx = cau14_idx + 1
    if cau15_idx < len(row):
        result['Cau15'] = row[cau15_idx]
    cau16_idx = cau15_idx + 1
    if cau16_idx < len(row):
        result['Cau16'] = row[cau16_idx]
    
    return result

def process_th3(row):
    """Xử lý TH3: index 13 != NULL và index 14 = NULL"""
    # TH3 xử lý giống TH2 đến bước cột NULL
    result = process_th2(row)
    
    # Khác biệt: sau cột NULL có Cau13, Cau14, Cau15, Cau16
    # Nhưng trong TH2 đã xử lý rồi, nên kết quả giống nhau
    # Chỉ khác điều kiện xét ban đầu
    
    return result

def detect_case(row):
    """Phát hiện trường hợp dựa vào index 13 và index 14"""
    if len(row) <= 13:
        return 'TH1'
    
    index_13 = row[13] if len(row) > 13 else None
    index_14 = row[14] if len(row) > 14 else None
    
    # Kiểm tra NULL (có thể là None, 'NULL', 'null', hoặc chuỗi rỗng)
    def is_null(val):
        return val is None or str(val).strip() == '' or str(val).upper() == 'NULL'
    
    if is_null(index_13):
        return 'TH1'
    elif not is_null(index_13) and not is_null(index_14):
        return 'TH2'
    elif not is_null(index_13) and is_null(index_14):
        return 'TH3'
    else:
        return 'TH1'

def process_data(df):
    """Xử lý toàn bộ dữ liệu"""
    results = []
    
    for idx, row in df.iterrows():
        # Chuyển row thành list để xử lý index
        row_list = [str(v) if pd.notna(v) else '' for v in row.values]
        
        # Phát hiện trường hợp
        case = detect_case(row_list)
        
        # Xử lý theo từng trường hợp
        if case == 'TH1':
            processed_row = process_th1(row_list)
        elif case == 'TH2':
            processed_row = process_th2(row_list)
        elif case == 'TH3':
            processed_row = process_th3(row_list)
        else:
            processed_row = process_th1(row_list)
        
        results.append(processed_row)
    
    return pd.DataFrame(results)

def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # Đọc file CSV
    df = pd.read_csv(SURVEY_FILE, header=None)
    
    # Xử lý dữ liệu
    processed_df = process_data(df)
    
    # Lưu kết quả
    output_path = f"{SEMESTER}/{FILE_NAME}_processed.csv"
    upload_to_blob(blob_service, processed_df, output_path)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
