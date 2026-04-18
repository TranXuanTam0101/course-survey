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

def inspect_error_lines(filepath):
    """
    Đọc file và in ra TẤT CẢ các dòng có số cột khác 18 sau khi tách bằng dấu phẩy
    """
    print("\n" + "="*80)
    print("KIỂM TRA CÁC DÒNG BỊ LỖI (SỐ CỘT KHÁC 18)")
    print("="*80)
    
    error_lines = []
    line_number = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            # Tách bằng dấu phẩy
            parts = line.split(',')
            num_cols = len(parts)
            
            if num_cols != 18:
                error_lines.append({
                    'line_number': line_number,
                    'num_cols': num_cols,
                    'content': line,
                    'parts': parts
                })
    
    # In thống kê
    print(f"\nTổng số dòng trong file: {line_number}")
    print(f"Số dòng có số cột KHÁC 18: {len(error_lines)}")
    
    if len(error_lines) == 0:
        print("\n✅ KHÔNG có dòng lỗi nào! Tất cả các dòng đều có 18 cột.")
        return
    
    # Phân loại lỗi
    less_than_18 = [e for e in error_lines if e['num_cols'] < 18]
    more_than_18 = [e for e in error_lines if e['num_cols'] > 18]
    
    print(f"  - Số cột < 18: {len(less_than_18)} dòng")
    print(f"  - Số cột > 18: {len(more_than_18)} dòng")
    
    # IN RA TẤT CẢ CÁC DÒNG CÓ SỐ CỘT > 18
    if more_than_18:
        print("\n" + "="*80)
        print(f"CHI TIẾT TẤT CẢ CÁC DÒNG CÓ SỐ CỘT > 18 (Tổng số: {len(more_than_18)} dòng)")
        print("="*80)
        
        for i, error in enumerate(more_than_18):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(more_than_18)} | Số dòng gốc: {error['line_number']} | Số cột: {error['num_cols']}")
            print(f"{'='*80}")
            print(f"Nội dung gốc:")
            print(f"{error['content']}")
            
            print(f"\nCác cột sau khi tách (HIỂN THỊ TẤT CẢ CÁC CỘT):")
            for idx, part in enumerate(error['parts']):
                print(f"  Cột {idx}: {part}")
            
            print(f"\n{'#'*80}")
    
    # IN RA TẤT CẢ CÁC DÒNG CÓ SỐ CỘT < 18
    if less_than_18:
        print("\n" + "="*80)
        print(f"CHI TIẾT TẤT CẢ CÁC DÒNG CÓ SỐ CỘT < 18 (Tổng số: {len(less_than_18)} dòng)")
        print("="*80)
        
        for i, error in enumerate(less_than_18):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(less_than_18)} | Số dòng gốc: {error['line_number']} | Số cột: {error['num_cols']}")
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
        f.write(f"Số dòng lỗi: {len(error_lines)}\n")
        f.write(f"  - Cột < 18: {len(less_than_18)}\n")
        f.write(f"  - Cột > 18: {len(more_than_18)}\n")
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
    
    print(f"\n📁 Đã ghi chi tiết TẤT CẢ các dòng lỗi vào file: {log_file}")
    print(f"\n✅ Kiểm tra xong! Đã in ra {len(error_lines)} dòng lỗi.")

def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # KIỂM TRA VÀ IN RA TẤT CẢ CÁC DÒNG LỖI
    inspect_error_lines(SURVEY_FILE)
    
    # Tiếp tục xử lý dữ liệu bình thường (nếu cần)
    try:
        # Thử đọc file bằng pandas với error handling
        df = pd.read_csv(SURVEY_FILE, header=None, on_bad_lines='skip')
        print(f"\n✅ Đã đọc được {len(df)} dòng bằng pandas (bỏ qua các dòng lỗi)")
        
    except Exception as e:
        print(f"\n❌ Lỗi khi đọc file: {e}")
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
