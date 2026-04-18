import os
import sys
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

def smart_split_by_comma_v2(line):
    """
    Tách dòng bằng dấu phẩy với logic 2 cấp:
    Cấp 1: Sau dấu phẩy KHÔNG có khoảng trắng + chữ cái đầu VIẾT HOA -> tách cột
    Cấp 2: Nếu vẫn >18 cột, áp dụng: Sau dấu phẩy KHÔNG có khoảng trắng -> tách cột
    """
    # Cấp 1: Tách với điều kiện (không khoảng trắng + viết hoa)
    parts_level1 = []
    current = []
    i = 0
    length = len(line)
    
    while i < length:
        if line[i] == ',':
            is_delimiter = False
            
            if i + 1 < length:
                next_char = line[i + 1]
                # Điều kiện cấp 1: Không khoảng trắng VÀ viết hoa
                if next_char != ' ' and next_char.isupper():
                    is_delimiter = True
                else:
                    is_delimiter = False
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                if current_str:
                    parts_level1.append(current_str)
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level1.append(current_str)
    
    # Nếu số cột đã <= 18, trả về luôn
    if len(parts_level1) <= 18:
        return parts_level1
    
    # Cấp 2: Tách với điều kiện (không khoảng trắng) - bỏ qua điều kiện viết hoa
    parts_level2 = []
    current = []
    i = 0
    
    while i < length:
        if line[i] == ',':
            is_delimiter = False
            
            if i + 1 < length:
                next_char = line[i + 1]
                # Điều kiện cấp 2: Chỉ cần không có khoảng trắng
                if next_char != ' ':
                    is_delimiter = True
                else:
                    is_delimiter = False
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                if current_str:
                    parts_level2.append(current_str)
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level2.append(current_str)
    
    return parts_level2

def inspect_error_lines(filepath):
    """
    Đọc file và CHỈ in ra các dòng có số cột > 18 sau khi tách
    """
    print("\n" + "="*80)
    print("KIỂM TRA CÁC DÒNG CÓ SỐ CỘT > 18")
    print("="*80)
    
    error_lines = []
    line_number = 0
    still_error_after_level2 = []
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            # Tách bằng logic 2 cấp
            parts = smart_split_by_comma_v2(line)
            num_cols = len(parts)
            
            # CHỈ lấy các dòng có số cột > 18
            if num_cols > 18:
                error_lines.append({
                    'line_number': line_number,
                    'num_cols': num_cols,
                    'content': line,
                    'parts': parts
                })
                
                still_error_after_level2.append({
                    'line_number': line_number,
                    'num_cols': num_cols,
                    'content': line,
                    'parts': parts
                })
    
    # In thống kê
    print(f"\nTổng số dòng trong file: {line_number}")
    print(f"Số dòng có số cột > 18: {len(error_lines)}")
    
    if len(error_lines) == 0:
        print("\n✅ KHÔNG có dòng nào có số cột > 18!")
        return
    
    # IN RA TẤT CẢ CÁC DÒNG CÓ SỐ CỘT > 18
    print("\n" + "="*80)
    print(f"CHI TIẾT CÁC DÒNG CÓ SỐ CỘT > 18 (Tổng số: {len(error_lines)} dòng)")
    print("="*80)
    
    for i, error in enumerate(error_lines):
        print(f"\n{'='*80}")
        print(f"DÒNG {i+1}/{len(error_lines)} | Số dòng gốc: {error['line_number']} | Số cột: {error['num_cols']}")
        print(f"{'='*80}")
        print(f"Nội dung gốc:")
        print(f"{error['content']}")
        
        print(f"\nCác cột sau khi tách (HIỂN THỊ TẤT CẢ CÁC CỘT):")
        for idx, part in enumerate(error['parts']):
            print(f"  Cột {idx}: {part}")
        
        print(f"\n{'#'*80}")
    
    # Ghi ra file log để lưu trữ
    log_file = f"error_lines_{FILE_NAME}.txt"
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"FILE: {SURVEY_FILE}\n")
        f.write(f"Tổng số dòng: {line_number}\n")
        f.write(f"Số dòng có số cột > 18: {len(error_lines)}\n")
        f.write("\n" + "="*80 + "\n")
        
        for error in error_lines:
            f.write(f"\n{'='*80}\n")
            f.write(f"DÒNG {error['line_number']} | Số cột: {error['num_cols']}\n")
            f.write(f"{'='*80}\n")
            f.write(f"Nội dung gốc:\n{error['content']}\n")
            f.write(f"\nTất cả các cột sau khi tách:\n")
            for idx, part in enumerate(error['parts']):
                f.write(f"  Cột {idx}: {part}\n")
            f.write("\n" + "#"*80 + "\n")
    
    print(f"\n📁 Đã ghi chi tiết các dòng có số cột > 18 vào file: {log_file}")
    print(f"\n✅ Kiểm tra xong! Đã in ra {len(error_lines)} dòng có số cột > 18.")

def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # KIỂM TRA VÀ CHỈ IN RA CÁC DÒNG CÓ SỐ CỘT > 18
    inspect_error_lines(SURVEY_FILE)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
