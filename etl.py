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
    """Kiểm tra định dạng ngày tháng xx/xx/xxxx"""
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))

def is_ma_gv_format(value):
    """Kiểm tra định dạng MaGV: đúng 7 ký tự và toàn số"""
    if not isinstance(value, str):
        return False
    value = value.strip()
    return len(value) == 7 and value.isdigit()

def split_after_null_by_rules(after_null_list):
    """
    Áp dụng quy tắc tách cho phần sau cột NULL
    Đầu vào: list các phần tử sau NULL (đã được split bằng dấu phẩy)
    Trả về: list 4 cột Cau13, Cau14, Cau15, Cau16
    """
    if not after_null_list:
        return ['', '', '', '']
    
    # Lưu lại bản gốc để xử lý
    original_text = ','.join(after_null_list)
    
    # Quy tắc 1: Ngay sau dấu phẩy không có khoảng trắng
    parts_rule1 = []
    current = []
    i = 0
    
    while i < len(original_text):
        if original_text[i] == ',':
            # Kiểm tra ký tự sau dấu phẩy
            if i + 1 < len(original_text) and original_text[i + 1] == ' ':
                # Có khoảng trắng -> không tách, giữ nguyên dấu phẩy
                current.append(',')
            else:
                # Không có khoảng trắng -> tách thành cột mới
                if current:
                    parts_rule1.append(''.join(current).strip())
                    current = []
        else:
            current.append(original_text[i])
        i += 1
    
    if current:
        parts_rule1.append(''.join(current).strip())
    
    # Loại bỏ phần tử rỗng
    parts_rule1 = [p for p in parts_rule1 if p]
    
    # Nếu tách được đúng 4 cột thì trả về
    if len(parts_rule1) == 4:
        return parts_rule1
    
    # Quy tắc 2: Ngay sau dấu phẩy không có khoảng trắng VÀ chữ cái đầu tiên viết hoa
    parts_rule2 = []
    current = []
    i = 0
    
    while i < len(original_text):
        if original_text[i] == ',':
            if i + 1 < len(original_text):
                next_char = original_text[i + 1]
                # Kiểm tra: không có khoảng trắng và ký tự tiếp theo là chữ hoa
                if next_char != ' ' and next_char.isupper():
                    # Tách thành cột mới
                    if current:
                        parts_rule2.append(''.join(current).strip())
                        current = []
                else:
                    # Không tách, giữ nguyên dấu phẩy
                    current.append(',')
            else:
                current.append(',')
        else:
            current.append(original_text[i])
        i += 1
    
    if current:
        parts_rule2.append(''.join(current).strip())
    
    parts_rule2 = [p for p in parts_rule2 if p]
    
    # Nếu tách được đúng 4 cột thì trả về
    if len(parts_rule2) == 4:
        return parts_rule2
    
    # Trường hợp không tách được thành 4 cột
    if len(parts_rule2) > 4:
        # Lấy 4 cột đầu, các cột còn lại sẽ được ghi nhận để kiểm tra
        print(f"CẢNH BÁO: Tách được {len(parts_rule2)} cột (>4), chỉ lấy 4 cột đầu")
        return parts_rule2[:4]
    else:
        # Thiếu cột, thêm string rỗng
        while len(parts_rule2) < 4:
            parts_rule2.append('')
        return parts_rule2

def process_row(row):
    """
    Xử lý một dòng CSV theo logic:
    1. Xử lý các cột trước NULL trước
    2. Sau đó xử lý các cột sau NULL
    """
    if not row or len(row) < 2:
        return None, []
    
    try:
        # ========== PHẦN 1: XỬ LÝ CÁC CỘT TRƯỚC CỘT NULL ==========
        
        # Bước 1: Lấy cột cố định theo index
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''
        
        # Bước 2: Dò tìm NgaySinh (từ index 2 trở đi)
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break
        
        # Bước 3: Tạo HoDem và Ten
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            # Lấy tất cả các cột nằm giữa MaSV (index 1) và NgaySinh
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p.strip() for p in ho_dem_ten_parts if p and p.strip()])
            
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]  # Từ cuối cùng là Tên
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''  # Phần còn lại là Họ đệm
        
        # Bước 4: Xác định MaHP (cột ngay sau NgaySinh)
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        
        # Bước 5: Dò tìm MaGV (có đúng 7 ký tự và toàn số)
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break
        
        # Bước 6: Xác định TenHP (các cột nằm giữa MaHP và MaGV)
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        
        # Bước 7: Gán các cột tiếp theo
        ho_dem_gv = ''
        ten_gv = ''
        lop_hp = ''
        cau_hoi = ''
        gia_tri = ''
        
        if ma_gv_index >= 0:
            if ma_gv_index + 1 < len(row):
                ho_dem_gv = row[ma_gv_index + 1].strip()
            if ma_gv_index + 2 < len(row):
                ten_gv = row[ma_gv_index + 2].strip()
            if ma_gv_index + 3 < len(row):
                lop_hp = row[ma_gv_index + 3].strip()
            if ma_gv_index + 4 < len(row):
                cau_hoi = row[ma_gv_index + 4].strip()
            if ma_gv_index + 5 < len(row):
                gia_tri = row[ma_gv_index + 5].strip()
        
        # Bước 8: Xác định cột NULL (cột ngay sau GiaTri)
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
        error_rows = []
        
        if null_index >= 0 and null_index + 1 < len(row):
            # Lấy phần sau cột NULL
            after_null = row[null_index + 1:]
            
            # Áp dụng quy tắc tách
            split_result = split_after_null_by_rules(after_null)
            
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            
            # Ghi nhận nếu số cột tách được không đúng
            if len(split_result) != 4:
                error_rows.append({
                    'row_index': 'current',
                    'after_null_original': after_null,
                    'split_result': split_result,
                    'split_count': len(split_result)
                })
        
        # Tạo kết quả với đúng 18 cột
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
        
        return result, error_rows
        
    except Exception as e:
        print(f"Lỗi xử lý dòng: {e}")
        print(f"Dòng bị lỗi: {row}")
        return None, []

def main():
    # Khởi tạo Blob Service
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("Kết nối blob storage thành công")
    except Exception as e:
        print(f"Lỗi kết nối blob: {e}")
        sys.exit(1)
    
    # Download file
    download_from_blob(blob_service)
    
    # Đọc file CSV
    try:
        df = pd.read_csv(SURVEY_FILE, header=None, dtype=str)
        print(f"Đã đọc file CSV, số dòng: {len(df)}")
    except Exception as e:
        print(f"Lỗi đọc file CSV: {e}")
        sys.exit(1)
    
    # Xử lý từng dòng
    processed_rows = []
    all_errors = []
    error_count = 0
    
    for idx, row in df.iterrows():
        row_list = [str(val) if pd.notna(val) else '' for val in row.values]
        
        result, errors = process_row(row_list)
        
        if result:
            processed_rows.append(result)
        
        if errors:
            for error in errors:
                error['row_number'] = idx + 2  # +2 vì header và index từ 0
                all_errors.append(error)
            error_count += len(errors)
        
        # Log tiến độ
        if (idx + 1) % 1000 == 0:
            print(f"Đã xử lý {idx + 1} dòng...")
    
    # Tạo DataFrame kết quả
    result_df = pd.DataFrame(processed_rows)
    
    # In ra các dòng lỗi cần kiểm tra thủ công
    if all_errors:
        print(f"\n{'='*60}")
        print(f"CẢNH BÁO: Có {len(all_errors)} dòng cần kiểm tra thủ công")
        print(f"{'='*60}")
        
        for i, error in enumerate(all_errors[:10]):  # In 10 dòng đầu
            print(f"\n--- Dòng lỗi {i+1} (Dòng số {error.get('row_number', '?')}) ---")
            print(f"After NULL: {error.get('after_null_original', '')[:200]}")
            print(f"Số cột tách được: {error.get('split_count', 0)}")
            print(f"Kết quả tách: {error.get('split_result', [])}")
        
        # Lưu file lỗi để kiểm tra
        error_df = pd.DataFrame(all_errors)
        error_filename = f"{FILE_NAME}_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        error_df.to_csv(error_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu {len(all_errors)} dòng lỗi vào file: {error_filename}")
    else:
        print("\nKhông có dòng lỗi nào cần kiểm tra thủ công!")
    
    # Xuất file kết quả
    output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = f"{SEMESTER}/{output_filename}"
    
    # Upload lên blob
    if upload_to_blob(blob_service, result_df, output_path):
        print(f"\n{'='*60}")
        print(f"THÀNH CÔNG!")
        print(f"{'='*60}")
        print(f"Số dòng đã xử lý: {len(processed_rows)}")
        print(f"Số dòng có lỗi: {len(all_errors)}")
        print(f"File kết quả: {output_path}")
        print(f"{'='*60}")
    else:
        print("Upload file thất bại!")
        sys.exit(1)

if __name__ == "__main__":
    main()
