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
    Đọc file và in ra TẤT CẢ các dòng đã được xử lý (sau khi tách cột)
    """
    print("\n" + "="*80)
    print("DANH SÁCH CÁC DÒNG DỮ LIỆU ĐÃ ĐƯỢC XỬ LÝ (SAU KHI TÁCH CỘT)")
    print("="*80)
    
    all_processed_lines = []
    line_number = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            # Tách bằng logic 2 cấp
            parts = smart_split_by_comma_v2(line)
            num_cols = len(parts)
            
            all_processed_lines.append({
                'line_number': line_number,
                'num_cols': num_cols,
                'original': line,
                'parts': parts
            })
    
    # In thống kê tổng quan
    print(f"\n📊 TỔNG QUAN:")
    print(f"  - Tổng số dòng trong file: {line_number}")
    print(f"  - Số dòng có 18 cột: {len([l for l in all_processed_lines if l['num_cols'] == 18])}")
    print(f"  - Số dòng có < 18 cột: {len([l for l in all_processed_lines if l['num_cols'] < 18])}")
    print(f"  - Số dòng có > 18 cột: {len([l for l in all_processed_lines if l['num_cols'] > 18])}")
    
    # IN RA TẤT CẢ CÁC DÒNG ĐÃ XỬ LÝ
    print("\n" + "="*80)
    print("CHI TIẾT TỪNG DÒNG ĐÃ ĐƯỢC XỬ LÝ")
    print("="*80)
    
    for idx, item in enumerate(all_processed_lines):
        print(f"\n{'='*80}")
        print(f"DÒNG {idx+1}/{len(all_processed_lines)} | Số dòng gốc: {item['line_number']} | Số cột sau tách: {item['num_cols']}")
        print(f"{'='*80}")
        
        # In dấu hiệu nhận biết nếu số cột không đúng
        if item['num_cols'] < 18:
            print(f"⚠️ THIẾU CỘT: Cần 18 cột nhưng chỉ có {item['num_cols']} cột")
        elif item['num_cols'] > 18:
            print(f"⚠️ THỪA CỘT: Cần 18 cột nhưng có {item['num_cols']} cột")
        else:
            print(f"✅ ĐÚNG: Đủ 18 cột")
        
        print(f"\nNội dung gốc:")
        print(f"{item['original'][:500]}{'...' if len(item['original']) > 500 else ''}")
        
        print(f"\nCác cột sau khi tách (HIỂN THỊ TẤT CẢ CÁC CỘT):")
        for col_idx, part in enumerate(item['parts']):
            # Đánh dấu các cột quan trọng
            if col_idx == 13:
                print(f"  Cột {col_idx} (NULL): {part[:200]}")
            elif col_idx >= 14:
                print(f"  Cột {col_idx} (Cau{col_idx-11}): {part[:200]}")
            else:
                print(f"  Cột {col_idx}: {part[:200]}")
        
        print(f"\n{'#'*80}")
    
    # Ghi ra file log để lưu trữ
    log_file = f"processed_lines_{FILE_NAME}.txt"
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"FILE: {SURVEY_FILE}\n")
        f.write(f"Tổng số dòng: {line_number}\n")
        f.write(f"Số dòng có 18 cột: {len([l for l in all_processed_lines if l['num_cols'] == 18])}\n")
        f.write(f"Số dòng có < 18 cột: {len([l for l in all_processed_lines if l['num_cols'] < 18])}\n")
        f.write(f"Số dòng có > 18 cột: {len([l for l in all_processed_lines if l['num_cols'] > 18])}\n")
        f.write("\n" + "="*80 + "\n")
        
        for item in all_processed_lines:
            f.write(f"\n{'='*80}\n")
            f.write(f"DÒNG {item['line_number']} | Số cột: {item['num_cols']}\n")
            f.write(f"{'='*80}\n")
            f.write(f"Nội dung gốc:\n{item['original']}\n")
            f.write(f"\nCác cột sau khi tách:\n")
            for col_idx, part in enumerate(item['parts']):
                f.write(f"  Cột {col_idx}: {part}\n")
            f.write("\n" + "#"*80 + "\n")
    
    print(f"\n📁 Đã ghi chi tiết TẤT CẢ các dòng đã xử lý vào file: {log_file}")
    print(f"\n✅ XỬ LÝ XONG! Đã xử lý {len(all_processed_lines)} dòng.")

def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # XỬ LÝ VÀ IN RA TẤT CẢ CÁC DÒNG ĐÃ ĐƯỢC XỬ LÝ
    inspect_error_lines(SURVEY_FILE)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
