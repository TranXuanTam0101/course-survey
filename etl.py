import os
import sys
import pandas as pd
from azure.storage.blob import BlobServiceClient

# --- Giữ nguyên các biến môi trường của bạn ---
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

def check_and_print_error_lines(file_path):
    print(f"--- ĐANG KIỂM TRA CÁC DÒNG CÓ SỐ CỘT > 18 TRONG FILE: {file_path} ---")
    error_count = 0
    
    try:
        # Đọc file bằng context manager để đảm bảo hiệu suất
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            for line_number, line in enumerate(f, 1):
                # Loại bỏ ký tự xuống dòng và tách bằng dấu phẩy
                parts = line.strip().split(',')
                num_columns = len(parts)
                
                # Kiểm tra nếu số cột lớn hơn 18
                if num_columns > 18:
                    error_count += 1
                    print("-" * 50)
                    print(f"LỖI TẠI DÒNG: {line_number} | SỐ CỘT ĐẾM ĐƯỢC: {num_columns}")
                    print(f"DỮ LIỆU THÔ:\n{line.strip()}")
                    
        print("-" * 50)
        print(f"TỔNG CỘNG: Tìm thấy {error_count} dòng bị lỗi định dạng cột.")
        
    except Exception as e:
        print(f"Không thể đọc file để kiểm tra lỗi: {e}")

def main():
    # Giả sử file đã được tải về bằng hàm download_from_blob của bạn
    # download_from_blob(blob_service)
    
    if os.path.exists(SURVEY_FILE):
        # Gọi hàm kiểm tra dòng lỗi
        check_and_print_error_lines(SURVEY_FILE)
    else:
        print("File không tồn tại cục bộ để kiểm tra.")

if __name__ == "__main__":
    # Lưu ý: Cần đảm bảo các biến môi trường đã được set trước khi chạy
    if CONNECTION_STRING and SEMESTER and SURVEY_FILE:
        main()
    else:
        print("Thiếu biến môi trường.")
