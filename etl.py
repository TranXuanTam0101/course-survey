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
    """Kiểm tra định dạng ngày tháng xx/xx/xxxx"""
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))

def is_ma_gv_format(value):
    """
    Kiểm tra định dạng MaGV:
    - Có 7 ký tự và toàn số
    - Hoặc có 7 ký tự và bắt đầu bằng "TG"
    - Hoặc bằng "gvDacThu_TKTH"
    """
    if not isinstance(value, str):
        return False
    value = value.strip()
    
    # Trường hợp 1: 7 ký tự và toàn số
    if len(value) == 7 and value.isdigit():
        return True
    
    # Trường hợp 2: 7 ký tự và bắt đầu bằng "TG"
    if len(value) == 7 and value.startswith("TG"):
        return True
    
    # Trường hợp 3: Bằng "gvDacThu_TKTH"
    if value == "gvDacThu_TKTH":
        return True
    
    return False

def split_after_null_by_rules(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL theo logic:
    1. Kiểm tra phần tử cuối cùng có phải giá trị đặc biệt không
    2. Áp dụng Bước 1
    3. Nếu >4 cột, áp dụng Bước 2
    4. Nếu vẫn >4 cột, in ra để kiểm tra thủ công
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    # Ghép lại thành chuỗi để xử lý
    original_text = ','.join(after_null_list)
    
    # ===== KIỂM TRA PHẦN TỬ CUỐI CÙNG =====
    last_part = after_null_list[-1].strip() if after_null_list else ''
    special_values = ['không', 'tốt', 'khộng', 'ko', 'ok', 'KHÔNG']
    
    has_special_last = last_part in special_values
    
    # Tách phần còn lại nếu có giá trị đặc biệt ở cuối
    text_to_process = original_text
    special_last_value = ''
    
    if has_special_last:
        # Tìm vị trí của giá trị đặc biệt cuối cùng
        for special in special_values:
            if original_text.endswith(',' + special):
                text_to_process = original_text[:-(len(special)+1)]
                special_last_value = special
                break
            elif original_text.endswith(special):
                text_to_process = original_text[:-len(special)]
                special_last_value = special
                break
    
    # ===== BƯỚC 1: Tách bằng dấu phẩy (sau dấu phẩy không có khoảng trắng → tách) =====
    parts_step1 = []
    current = []
    i = 0
    
    while i < len(text_to_process):
        if text_to_process[i] == ',':
            if i + 1 < len(text_to_process) and text_to_process[i + 1] == ' ':
                # Có khoảng trắng → không tách
                current.append(',')
            else:
                # Không có khoảng trắng → tách
                if current:
                    parts_step1.append(''.join(current).strip())
                    current = []
        else:
            current.append(text_to_process[i])
        i += 1
    
    if current:
        parts_step1.append(''.join(current).strip())
    
    parts_step1 = [p for p in parts_step1 if p]
    
    # Thêm giá trị đặc biệt vào cuối nếu có
    if special_last_value:
        if len(parts_step1) > 0:
            parts_step1[-1] = parts_step1[-1] + ',' + special_last_value
        else:
            parts_step1.append(special_last_value)
    
    # Nếu kết quả có đúng 4 cột → trả về
    if len(parts_step1) == 4:
        return parts_step1[:4], None
    
    # Nếu kết quả có >4 cột, chuyển sang Bước 2
    if len(parts_step1) > 4:
        # ===== BƯỚC 2: Tách với điều kiện thêm (sau dấu phẩy không khoảng trắng + chữ viết hoa) =====
        parts_step2 = []
        current = []
        i = 0
        
        # Reset text_to_process cho bước 2
        text_to_process = original_text
        special_last_value = ''
        
        if has_special_last:
            for special in special_values:
                if original_text.endswith(',' + special):
                    text_to_process = original_text[:-(len(special)+1)]
                    special_last_value = special
                    break
                elif original_text.endswith(special):
                    text_to_process = original_text[:-len(special)]
                    special_last_value = special
                    break
        
        while i < len(text_to_process):
            if text_to_process[i] == ',':
                if i + 1 < len(text_to_process):
                    next_char = text_to_process[i + 1]
                    # Điều kiện: không có khoảng trắng VÀ chữ cái đầu tiên viết hoa
                    if next_char != ' ' and next_char.isupper():
                        if current:
                            parts_step2.append(''.join(current).strip())
                            current = []
                    else:
                        current.append(',')
                else:
                    current.append(',')
            else:
                current.append(text_to_process[i])
            i += 1
        
        if current:
            parts_step2.append(''.join(current).strip())
        
        parts_step2 = [p for p in parts_step2 if p]
        
        # Thêm giá trị đặc biệt vào cuối nếu có
        if special_last_value:
            if len(parts_step2) > 0:
                parts_step2[-1] = parts_step2[-1] + ',' + special_last_value
            else:
                parts_step2.append(special_last_value)
        
        # Nếu tách được đúng 4 cột → trả về
        if len(parts_step2) == 4:
            return parts_step2[:4], None
        
        # Nếu vẫn >4 cột, in ra để kiểm tra thủ công và để hết vào cột đầu tiên
        if len(parts_step2) > 4:
            error_info = {
                'row_number': row_number,
                'original_after_null': original_text,
                'split_result': parts_step2,
                'split_count': len(parts_step2)
            }
            # Để hết vào cột đầu tiên (Cau13)
            return [original_text, '', '', ''], error_info
    
    # Trường hợp còn lại: thiếu cột
    while len(parts_step1) < 4:
        parts_step1.append('')
    return parts_step1[:4], None

def process_row(row, row_number=None):
    """
    Xử lý một dòng CSV theo logic:
    PHẦN 1: Giữ nguyên (đã xử lý đúng)
    PHẦN 2: Đã điều chỉnh theo yêu cầu
    """
    if not row or len(row) < 2:
        return None, None, []
    
    try:
        # ========== PHẦN 1: XỬ LÝ CÁC CỘT TRƯỚC CỘT NULL (GIỮ NGUYÊN) ==========
        
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
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p.strip() for p in ho_dem_ten_parts if p and p.strip()])
            
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        
        # Bước 4: Xác định MaHP (cột ngay sau NgaySinh)
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        
        # Bước 5: Dò tìm MaGV
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
        
        # Bước 7-11: Gán các cột tiếp theo
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
        
        # Bước 12: Xác định cột NULL (cột ngay sau GiaTri)
        null_index = -1
        null_value = ''
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
                null_value = potential_null if potential_null else 'NULL'
        
        # ========== PHẦN 2: XỬ LÝ CÁC CỘT SAU CỘT NULL (ĐÃ ĐIỀU CHỈNH) ==========
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
        
        return result, None, split_errors
        
    except Exception as e:
        print(f"Lỗi xử lý dòng {row_number}: {e}")
        return None, str(e), []

def read_csv_manual(filename):
    """Đọc file CSV thủ công"""
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
        return rows
        
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return []

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
    
    # Đọc file CSV thủ công
    print("Đang đọc file CSV...")
    rows = read_csv_manual(SURVEY_FILE)
    
    if not rows:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    # Xử lý từng dòng
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
    
    # Tạo DataFrame kết quả
    result_df = pd.DataFrame(processed_rows)
    
    # In báo cáo
    print(f"\n{'='*60}")
    print("BÁO CÁO XỬ LÝ")
    print(f"{'='*60}")
    print(f"Tổng số dòng đọc được: {len(rows)}")
    print(f"Số dòng xử lý thành công: {len(processed_rows)}")
    print(f"Số dòng xử lý lỗi: {len(process_errors)}")
    
    # In các dòng có kết quả tách >4 cột ở Bước 2
    if split_errors:
        print(f"\n{'='*60}")
        print("CÁC DÒNG CÓ KẾT QUẢ TÁCH >4 CỘT (BƯỚC 2) - CẦN KIỂM TRA THỦ CÔNG")
        print(f"{'='*60}")
        for err in split_errors:
            print(f"\nDòng {err['row_number']}:")
            print(f"  Chuỗi sau NULL: {err['original_after_null']}")
            print(f"  Số cột tách được: {err['split_count']}")
            print(f"  Kết quả tách: {err['split_result']}")
        
        # Lưu file lỗi tách
        split_error_df = pd.DataFrame(split_errors)
        split_error_filename = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(split_error_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu {len(split_errors)} dòng lỗi tách vào file: {split_error_filename}")
    
    # Xuất file kết quả
    if len(processed_rows) > 0:
        output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        
        if upload_to_blob(blob_service, result_df, output_path):
            print(f"\n{'='*60}")
            print("THÀNH CÔNG!")
            print(f"{'='*60}")
            print(f"File kết quả: {output_path}")
            print(f"Số dòng đã xử lý: {len(processed_rows)}")
            print(f"Số dòng lỗi tách >4 cột: {len(split_errors)}")
            print(f"{'='*60}")
        else:
            print("Upload file thất bại!")
            sys.exit(1)
    else:
        print("Không có dòng nào được xử lý thành công!")
        sys.exit(1)

if __name__ == "__main__":
    main()
