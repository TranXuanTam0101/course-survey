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

# Danh sách toàn cục để lưu các dòng có >4 cột sau khi xử lý
rows_with_more_than_4_columns = []

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

def split_after_null_by_rules(after_null_list, row_number=0):
    """
    Xử lý các cột sau cột NULL theo logic:
    Bước 1: Tách bằng dấu phẩy (sau dấu phẩy không khoảng trắng → tách)
    - Nếu kết quả tách được đúng 4 cột → trả về
    - Nếu kết quả tách được lớn hơn 4 cột → chuyển Bước 2
    
    Bước 2: Tách với điều kiện (sau dấu phẩy không khoảng trắng + chữ viết hoa)
    - Nếu kết quả tách được đúng 4 cột → trả về
    - Nếu kết quả tách được lớn hơn 4 cột → KHÔNG tách, để hết vào cột đầu tiên
      và IN RA CÁC DÒNG DỮ LIỆU này
    """
    if not after_null_list:
        return ['', '', '', '']
    
    # Ghép lại thành chuỗi để xử lý
    original_text = ','.join(after_null_list)
    
    # ===== BƯỚC 1: Tách bằng dấu phẩy (sau dấu phẩy không khoảng trắng → tách) =====
    parts_step1 = []
    current = []
    i = 0
    
    while i < len(original_text):
        if original_text[i] == ',':
            # Kiểm tra ký tự sau dấu phẩy
            if i + 1 < len(original_text) and original_text[i + 1] == ' ':
                # Có khoảng trắng → KHÔNG tách, giữ nguyên dấu phẩy
                current.append(',')
            else:
                # Không có khoảng trắng → TÁCH thành cột mới
                if current:
                    parts_step1.append(''.join(current).strip())
                    current = []
        else:
            current.append(original_text[i])
        i += 1
    
    if current:
        parts_step1.append(''.join(current).strip())
    
    # Loại bỏ phần tử rỗng
    parts_step1 = [p for p in parts_step1 if p]
    
    # Nếu kết quả tách được đúng 4 cột → trả về
    if len(parts_step1) == 4:
        return parts_step1
    
    # Nếu kết quả tách được lớn hơn 4 cột → chuyển Bước 2
    if len(parts_step1) > 4:
        # ===== BƯỚC 2: Tách với điều kiện thêm (sau dấu phẩy không khoảng trắng + chữ viết hoa) =====
        parts_step2 = []
        current = []
        i = 0
        
        while i < len(original_text):
            if original_text[i] == ',':
                if i + 1 < len(original_text):
                    next_char = original_text[i + 1]
                    # Điều kiện: không có khoảng trắng VÀ ký tự tiếp theo là chữ viết hoa
                    if next_char != ' ' and next_char.isupper():
                        # Tách thành cột mới
                        if current:
                            parts_step2.append(''.join(current).strip())
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
            parts_step2.append(''.join(current).strip())
        
        # Loại bỏ phần tử rỗng
        parts_step2 = [p for p in parts_step2 if p]
        
        # Nếu kết quả tách được đúng 4 cột → trả về
        if len(parts_step2) == 4:
            return parts_step2
        
        # Nếu kết quả tách được lớn hơn 4 cột → IN RA CẢNH BÁO và để hết vào cột đầu tiên
        if len(parts_step2) > 4:
            # Lưu thông tin dòng lỗi để in ra sau
            rows_with_more_than_4_columns.append({
                'row_number': row_number,
                'original_after_null': original_text,
                'split_result_step2': parts_step2,
                'number_of_columns': len(parts_step2)
            })
            # Không tách, để hết vào cột đầu tiên
            return [original_text, '', '', '']
        else:
            # Thiếu cột, thêm string rỗng
            while len(parts_step2) < 4:
                parts_step2.append('')
            return parts_step2[:4]
    else:
        # Thiếu cột, thêm string rỗng
        while len(parts_step1) < 4:
            parts_step1.append('')
        return parts_step1[:4]

def process_row(row, row_number):
    """
    Xử lý một dòng CSV theo logic mới:
    PHẦN 1: Xử lý các cột trước cột NULL (từ phải sang trái)
    PHẦN 2: Xử lý các cột sau cột NULL
    """
    if not row or len(row) < 2:
        return None
    
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
        
        # Bước 5: Xác định cột NULL (cột có giá trị 'NULL')
        null_index = -1
        null_value = ''
        for i in range(len(row)):
            if row[i].strip().upper() == 'NULL':
                null_index = i
                null_value = row[i].strip()
                break
        
        # Nếu không tìm thấy cột NULL, bỏ qua xử lý
        if null_index == -1:
            print(f"Cảnh báo dòng {row_number}: Không tìm thấy cột NULL")
            return None
        
        # Bước 6: Lấy các cột từ phải sang trái (ngược từ cột NULL)
        # GiaTri = cột ngay trước cột NULL
        gia_tri = ''
        cau_hoi = ''
        lop_hp = ''
        ten_gv = ''
        ho_dem_gv = ''
        ma_gv = ''
        
        if null_index - 1 >= 0:
            gia_tri = row[null_index - 1].strip()
        if null_index - 2 >= 0:
            cau_hoi = row[null_index - 2].strip()
        if null_index - 3 >= 0:
            lop_hp = row[null_index - 3].strip()
        if null_index - 4 >= 0:
            ten_gv = row[null_index - 4].strip()
        if null_index - 5 >= 0:
            ho_dem_gv = row[null_index - 5].strip()
        if null_index - 6 >= 0:
            ma_gv = row[null_index - 6].strip()
        
        # Bước 7: Xác định TenHP (các cột nằm giữa MaHP và MaGV)
        ten_hp = ''
        ma_hp_index = ngay_sinh_index + 1 if ngay_sinh_index >= 0 else -1
        ma_gv_index = null_index - 6 if null_index - 6 >= 0 else -1
        
        if ma_hp_index >= 0 and ma_gv_index > ma_hp_index + 1:
            ten_hp_parts = row[ma_hp_index + 1:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        
        # ========== PHẦN 2: XỬ LÝ CÁC CỘT SAU CỘT NULL ==========
        cau13 = cau14 = cau15 = cau16 = ''
        
        if null_index >= 0 and null_index + 1 < len(row):
            # Lấy phần sau cột NULL
            after_null = row[null_index + 1:]
            
            # Áp dụng quy tắc tách
            split_result = split_after_null_by_rules(after_null, row_number)
            
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
        
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
        
        return result
        
    except Exception as e:
        print(f"Lỗi xử lý dòng {row_number}: {e}")
        return None

def read_csv_manual(filename):
    """
    Đọc file CSV thủ công để xử lý các dòng có số cột không đồng đều
    """
    rows = []
    
    try:
        with open(filename, 'r', encoding='utf-8-sig') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                
                # Split bằng dấu phẩy
                row = line.split(',')
                
                # Loại bỏ khoảng trắng thừa ở đầu/cuối mỗi phần tử
                row = [col.strip() for col in row]
                
                rows.append(row)
                
                if line_num % 1000 == 0:
                    print(f"Đã đọc {line_num} dòng...")
        
        print(f"Đã đọc xong file: {len(rows)} dòng")
        return rows
        
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return []

def main():
    global rows_with_more_than_4_columns
    
    # Khởi tạo Blob Service
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("Kết nối blob storage thành công")
    except Exception as e:
        print(f"Lỗi kết nối blob: {e}")
        sys.exit(1)
    
    # Download file
    download_from_blob(blob_service)
    
    # Đọc file CSV thủ công
    print("Đang đọc file CSV...")
    rows = read_csv_manual(SURVEY_FILE)
    
    if not rows:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    # Xử lý từng dòng
    print(f"Bắt đầu xử lý {len(rows)} dòng...")
    processed_rows = []
    failed_rows = []
    
    for idx, row in enumerate(rows, 1):
        result = process_row(row, idx)
        
        if result:
            processed_rows.append(result)
        else:
            failed_rows.append({
                'line_number': idx,
                'row_length': len(row),
                'sample': ','.join(row[:10]) + '...' if len(row) > 10 else ','.join(row)
            })
        
        # Log progress
        if idx % 1000 == 0:
            print(f"Đã xử lý {idx}/{len(rows)} dòng...")
    
    # In ra các dòng có >4 cột sau Bước 2
    if rows_with_more_than_4_columns:
        print(f"\n{'='*60}")
        print("CẢNH BÁO: CÁC DÒNG CÓ >4 CỘT SAU KHI XỬ LÝ BƯỚC 2")
        print(f"{'='*60}")
        print(f"Tổng số dòng: {len(rows_with_more_than_4_columns)}")
        
        for item in rows_with_more_than_4_columns[:10]:  # In 10 dòng đầu
            print(f"\n--- Dòng số {item['row_number']} ---")
            print(f"Chuỗi sau NULL: {item['original_after_null'][:200]}...")
            print(f"Số cột tách được ở bước 2: {item['number_of_columns']}")
            print(f"Kết quả tách: {item['split_result_step2'][:5]}")  # In 5 cột đầu
        
        # Lưu vào file để kiểm tra
        warning_df = pd.DataFrame(rows_with_more_than_4_columns)
        warning_filename = f"{FILE_NAME}_warning_more_than_4_columns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        warning_df.to_csv(warning_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu chi tiết vào file: {warning_filename}")
    
    # Tạo DataFrame kết quả
    result_df = pd.DataFrame(processed_rows)
    
    # In báo cáo
    print(f"\n{'='*60}")
    print("BÁO CÁO XỬ LÝ")
    print(f"{'='*60}")
    print(f"Tổng số dòng đọc được: {len(rows)}")
    print(f"Số dòng xử lý thành công: {len(processed_rows)}")
    print(f"Số dòng xử lý thất bại: {len(failed_rows)}")
    print(f"Số dòng có >4 cột sau Bước 2: {len(rows_with_more_than_4_columns)}")
    
    if failed_rows:
        print(f"\nDÒNG XỬ LÝ THẤT BẠI:")
        for err in failed_rows[:5]:
            print(f"  - Dòng {err['line_number']}: có {err['row_length']} cột")
    
    # Xuất file kết quả
    if len(processed_rows) > 0:
        output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        
        # Upload lên blob
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
