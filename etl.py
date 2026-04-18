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

def smart_split_by_comma_level1(line):
    """
    Cấp độ 1: Tách dòng bằng dấu phẩy với logic:
    - Nếu sau dấu phẩy KHÔNG có khoảng trắng -> TÁCH CỘT (DELIMITER)
    - Nếu sau dấu phẩy CÓ khoảng trắng -> KHÔNG TÁCH (dấu phẩy trong nội dung)
    """
    parts = []
    current = []
    i = 0
    length = len(line)
    
    while i < length:
        if line[i] == ',':
            # Kiểm tra xem có phải delimiter không
            is_delimiter = False
            
            if i + 1 < length:
                next_char = line[i + 1]
                # Nếu sau dấu phẩy KHÔNG có khoảng trắng -> delimiter
                if next_char != ' ':
                    is_delimiter = True
                else:
                    is_delimiter = False
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                parts.append(current_str if current_str else '')
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        parts.append(current_str if current_str else '')
    
    return parts

def smart_split_by_comma_level2(line):
    """
    Cấp độ 2: Tách dòng với logic:
    - Sau dấu phẩy KHÔNG có khoảng trắng + chữ cái đầu viết hoa -> TÁCH CỘT
    """
    parts = []
    current = []
    i = 0
    length = len(line)
    
    while i < length:
        if line[i] == ',':
            is_delimiter = False
            
            if i + 1 < length:
                next_char = line[i + 1]
                # Kiểm tra: không khoảng trắng VÀ chữ cái đầu viết hoa
                if next_char != ' ' and next_char.isupper():
                    is_delimiter = True
                else:
                    is_delimiter = False
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                parts.append(current_str if current_str else '')
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        parts.append(current_str if current_str else '')
    
    return parts

def process_and_split_line(line):
    """
    Xử lý tách cột với 2 cấp độ:
    1. Áp dụng level1
    2. Nếu số cột > 18, áp dụng level2
    3. Nếu vẫn > 18, trả về None để báo lỗi
    """
    # Cấp độ 1
    parts = smart_split_by_comma_level1(line)
    
    if len(parts) <= 18:
        return parts
    
    # Cấp độ 2
    parts = smart_split_by_comma_level2(line)
    
    if len(parts) <= 18:
        return parts
    
    # Vẫn > 18 cột -> cần xem xét
    return None

def inspect_error_lines(filepath):
    """
    Đọc file và xử lý tách cột theo 2 cấp độ
    In ra các dòng vẫn còn > 18 cột sau khi xử lý
    """
    print("\n" + "="*80)
    print("XỬ LÝ TÁCH CỘT VỚI 2 CẤP ĐỘ")
    print("Cấp 1: Sau dấu phẩy không khoảng trắng -> tách")
    print("Cấp 2: Nếu >18 cột, sau dấu phẩy không khoảng trắng + viết hoa -> tách")
    print("="*80)
    
    still_error_lines = []
    line_number = 0
    total_lines = 0
    level1_success = 0
    level2_success = 0
    still_error = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            total_lines += 1
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            result = process_and_split_line(line)
            
            if result is not None:
                if len(result) <= 18:
                    # Thành công
                    if len(result) == 18:
                        level1_success += 1
                    else:
                        level2_success += 1
                else:
                    still_error += 1
                    still_error_lines.append({
                        'line_number': line_number,
                        'num_cols': len(result),
                        'content': line,
                        'parts': result
                    })
            else:
                still_error += 1
                # Thử tách bằng level2 để lấy parts
                parts = smart_split_by_comma_level2(line)
                still_error_lines.append({
                    'line_number': line_number,
                    'num_cols': len(parts),
                    'content': line,
                    'parts': parts
                })
    
    # In thống kê
    print(f"\n📊 THỐNG KÊ:")
    print(f"  - Tổng số dòng: {total_lines}")
    print(f"  - Thành công sau cấp 1 (đúng 18 cột): {level1_success}")
    print(f"  - Thành công sau cấp 2 (đúng 18 cột): {level2_success}")
    print(f"  - VẪN LỖI (>18 cột): {still_error}")
    
    # In các dòng vẫn lỗi
    if still_error_lines:
        print("\n" + "="*80)
        print(f"CÁC DÒNG VẪN CÒN > 18 CỘT SAU KHI XỬ LÝ (Tổng số: {len(still_error_lines)} dòng)")
        print("="*80)
        
        for i, error in enumerate(still_error_lines):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(still_error_lines)} | Số dòng gốc: {error['line_number']} | Số cột: {error['num_cols']}")
            print(f"{'='*80}")
            print(f"Nội dung gốc:")
            print(f"{error['content'][:500]}{'...' if len(error['content']) > 500 else ''}")
            
            print(f"\nCác cột sau khi tách (HIỂN THỊ TẤT CẢ CÁC CỘT):")
            for idx, part in enumerate(error['parts']):
                print(f"  Cột {idx}: {part[:200]}{'...' if len(part) > 200 else ''}")
            
            print(f"\n{'#'*80}")
        
        # Ghi ra file log
        log_file = f"still_error_lines_{FILE_NAME}.txt"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"FILE: {SURVEY_FILE}\n")
            f.write(f"Tổng số dòng vẫn lỗi (>18 cột): {len(still_error_lines)}\n")
            f.write("\n" + "="*80 + "\n")
            
            for error in still_error_lines:
                f.write(f"\n{'='*80}\n")
                f.write(f"DÒNG {error['line_number']} | Số cột: {error['num_cols']}\n")
                f.write(f"{'='*80}\n")
                f.write(f"Nội dung gốc:\n{error['content']}\n")
                f.write(f"\nTất cả các cột sau khi tách:\n")
                for idx, part in enumerate(error['parts']):
                    f.write(f"  Cột {idx}: {part}\n")
                f.write("\n" + "#"*80 + "\n")
        
        print(f"\n📁 Đã ghi chi tiết các dòng lỗi vào file: {log_file}")
    else:
        print("\n✅ TẤT CẢ các dòng đã được xử lý thành công! Không còn dòng nào >18 cột.")

def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # XỬ LÝ VÀ KIỂM TRA CÁC DÒNG LỖI
    inspect_error_lines(SURVEY_FILE)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
