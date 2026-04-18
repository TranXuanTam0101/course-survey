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

def split_after_null_by_rules(remaining_parts):
    """
    Áp dụng quy tắc tách cho phần sau cột NULL
    Trả về list các cột đã tách (tối đa 4 cột)
    """
    if not remaining_parts:
        return ['', '', '', '']
    
    original_text = ','.join(remaining_parts)
    
    # Quy tắc 1: Ngay sau dau phay khong co khoang trang
    parts_rule1 = []
    current = []
    i = 0
    while i < len(original_text):
        if original_text[i] == ',':
            # Kiem tra ky tu sau dau phay
            if i + 1 < len(original_text) and original_text[i + 1] == ' ':
                # Co khoang trang -> khong tach
                current.append(',')
            else:
                # Khong co khoang trang -> tach thanh cot moi
                parts_rule1.append(''.join(current))
                current = []
        else:
            current.append(original_text[i])
        i += 1
    if current:
        parts_rule1.append(''.join(current))
    
    # Loai bo cac phan tu rong
    parts_rule1 = [p for p in parts_rule1 if p.strip()]
    
    if len(parts_rule1) == 4:
        return parts_rule1[:4]
    
    # Quy tac 2: Ngay sau dau phay khong co khoang trang VA chu cai dau tien viet hoa
    parts_rule2 = []
    current = []
    i = 0
    while i < len(original_text):
        if original_text[i] == ',':
            if i + 1 < len(original_text):
                next_char = original_text[i + 1]
                # Kiem tra khong co khoang trang va ky tu tiep theo la chu hoa
                if next_char != ' ' and next_char.isupper():
                    parts_rule2.append(''.join(current))
                    current = []
                else:
                    current.append(',')
            else:
                current.append(',')
        else:
            current.append(original_text[i])
        i += 1
    if current:
        parts_rule2.append(''.join(current))
    
    parts_rule2 = [p for p in parts_rule2 if p.strip()]
    
    if len(parts_rule2) == 4:
        return parts_rule2[:4]
    
    # Truong hop dac biet: van > 4 cot hoac khong the tach
    if len(parts_rule2) > 4:
        # Tra ve 4 cot dau tien va luu lai de kiem tra thu cong
        return parts_rule2[:4]
    elif len(parts_rule2) < 4:
        # Them cac cot trong
        while len(parts_rule2) < 4:
            parts_rule2.append('')
        return parts_rule2
    
    return parts_rule2[:4]

def process_row(row):
    """
    Xu ly mot dong CSV theo logic da dinh nghia
    Tra ve dict cac cot hoac None neu co loi
    """
    if not row or len(row) < 2:
        return None
    
    try:
        # ========== PHAN 1: XU LY CAC COT TRUOC COT NULL ==========
        
        # Buoc 1: Lay cot co dinh theo index
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''
        
        # Buoc 2: Tim NgaySinh
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break
        
        # Buoc 3: Tao HoDem va Ten
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            # Lay cac cot giua MaSV (index 1) va NgaySinh
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p.strip() for p in ho_dem_ten_parts if p and p.strip()])
            
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        
        # Buoc 4: Xac dinh MaHP (cot ngay sau NgaySinh)
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        
        # Buoc 5: Tim MaGV (7 ky tu va toan so)
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break
        
        # Buoc 6: Xac dinh TenHP (cac cot giua MaHP va MaGV)
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        
        # Buoc 7: Gan cac cot con lai
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
        
        # Buoc 8: Xac dinh cot NULL (cot ngay sau GiaTri)
        null_index = -1
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            null_value = row[gia_tri_index + 1].strip()
            if null_value.upper() == 'NULL' or null_value == '':
                null_index = gia_tri_index + 1
        
        # ========== PHAN 2: XU LY CAC COT SAU COT NULL ==========
        cau13 = cau14 = cau15 = cau16 = ''
        error_rows = []
        
        if null_index >= 0 and null_index + 1 < len(row):
            # Lay phan sau cot NULL
            after_null = row[null_index + 1:]
            
            # Ap dung quy tac tach
            split_result = split_after_null_by_rules(after_null)
            
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            elif len(split_result) > 4:
                # Dua vao danh sach loi de kiem tra thu cong
                error_rows.append({
                    'original': ','.join(row),
                    'after_null': ','.join(after_null),
                    'split_result': split_result
                })
                # Van lay 4 cot dau
                cau13 = split_result[0] if len(split_result) > 0 else ''
                cau14 = split_result[1] if len(split_result) > 1 else ''
                cau15 = split_result[2] if len(split_result) > 2 else ''
                cau16 = split_result[3] if len(split_result) > 3 else ''
        
        # Tao ket qua
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
            'NULL': 'NULL' if null_index >= 0 else '',
            'Cau13': cau13,
            'Cau14': cau14,
            'Cau15': cau15,
            'Cau16': cau16
        }
        
        return result, error_rows
        
    except Exception as e:
        print(f"Loi xu ly dong: {e}")
        print(f"Dong bi loi: {row}")
        return None, []

def main():
    # Khoi tao Blob Service
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("Ket noi blob storage thanh cong")
    except Exception as e:
        print(f"Loi ket noi blob: {e}")
        sys.exit(1)
    
    # Download file
    download_from_blob(blob_service)
    
    # Doc file CSV
    try:
        df = pd.read_csv(SURVEY_FILE, header=None, dtype=str)
        print(f"Da doc file CSV, so dong: {len(df)}")
    except Exception as e:
        print(f"Loi doc file CSV: {e}")
        sys.exit(1)
    
    # Xu ly tung dong
    processed_rows = []
    all_errors = []
    
    for idx, row in df.iterrows():
        row_list = [str(val) if pd.notna(val) else '' for val in row.values]
        
        result, errors = process_row(row_list)
        
        if result:
            processed_rows.append(result)
        
        if errors:
            all_errors.extend(errors)
    
    # Tao DataFrame ket qua
    result_df = pd.DataFrame(processed_rows)
    
    # In ra cac dong loi can kiem tra thu cong
    if all_errors:
        print(f"\n=== CANH BAO: Co {len(all_errors)} dong can kiem tra thu cong ===")
        for i, error in enumerate(all_errors[:10]):  # In 10 dong dau
            print(f"\n--- Dong loi {i+1} ---")
            print(f"Original: {error['original'][:200]}...")
            print(f"After NULL: {error['after_null'][:200]}...")
            print(f"Split result ({len(error['split_result'])} cot): {error['split_result']}")
        
        # Luu file loi de kiem tra
        error_df = pd.DataFrame(all_errors)
        error_df.to_csv(f"{FILE_NAME}_errors.csv", index=False, encoding='utf-8-sig')
        print(f"\nDa luu {len(all_errors)} dong loi vao file {FILE_NAME}_errors.csv")
    
    # Xuat file ket qua
    output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = f"{SEMESTER}/{output_filename}"
    
    # Upload len blob
    if upload_to_blob(blob_service, result_df, output_path):
        print(f"\n=== THANH CONG ===")
        print(f"So dong da xu ly: {len(processed_rows)}")
        print(f"So dong loi: {len(all_errors)}")
        print(f"File ket qua: {output_path}")
    else:
        print("Upload file that bai")
        sys.exit(1)

if __name__ == "__main__":
    main()
