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


def split_after_null_by_logic(after_null_str):
    """
    Tách phần sau NULL thành 4 câu trả lời (Cau13, Cau14, Cau15, Cau16)
    
    Logic cấp 1:
    - Ngay sau dấu phẩy KHÔNG có khoảng trắng => tách cột
    
    Logic cấp 2 (nếu vẫn >4 câu):
    - Ngay sau dấu phẩy KHÔNG có khoảng trắng + chữ cái đầu VIẾT HOA => tách cột
    """
    if not after_null_str:
        return ['', '', '', '']
    
    # Cấp 1: Tách theo logic "sau dấu phẩy không có khoảng trắng"
    parts_level1 = []
    current = []
    i = 0
    length = len(after_null_str)
    
    while i < length:
        if after_null_str[i] == ',':
            is_delimiter = False
            if i + 1 < length:
                next_char = after_null_str[i + 1]
                # Sau dấu phẩy KHÔNG có khoảng trắng => delimiter
                if next_char != ' ':
                    is_delimiter = True
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
            current.append(after_null_str[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level1.append(current_str)
    
    # Nếu đã có 4 câu hoặc ít hơn, trả về kết quả
    if len(parts_level1) <= 4:
        while len(parts_level1) < 4:
            parts_level1.append('')
        return parts_level1[:4]
    
    # Cấp 2: Vẫn còn >4 câu, áp dụng thêm điều kiện "chữ cái đầu viết hoa"
    parts_level2 = []
    current = []
    i = 0
    length = len(after_null_str)
    
    while i < length:
        if after_null_str[i] == ',':
            is_delimiter = False
            if i + 1 < length:
                next_char = after_null_str[i + 1]
                # Điều kiện cấp 2: không khoảng trắng + chữ cái đầu viết hoa
                if next_char != ' ' and next_char.isupper():
                    is_delimiter = True
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
            current.append(after_null_str[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level2.append(current_str)
    
    # Đảm bảo có đúng 4 câu trả lời
    while len(parts_level2) < 4:
        parts_level2.append('')
    
    if len(parts_level2) > 4:
        # Gộp phần dư vào câu cuối
        parts_level2[3] = ','.join(parts_level2[3:])
        parts_level2 = parts_level2[:4]
    
    return parts_level2


def process_line_fixed(line):
    """
    Xử lý một dòng:
    - Tìm vị trí cột NULL
    - Giữ nguyên phần từ đầu đến NULL
    - Phần sau NULL: áp dụng logic tách 2 cấp
    - Trả về 18 cột
    """
    line_str = line.strip()
    if not line_str:
        return [''] * 18
    
    # Tách tạm thời để tìm vị trí NULL
    temp_parts = line_str.split(',')
    
    # Tìm vị trí cột NULL
    null_index = -1
    for i, p in enumerate(temp_parts):
        if p == '' or p.upper() == 'NULL':
            null_index = i
            break
    
    if null_index == -1:
        # Không tìm thấy NULL, giữ nguyên
        while len(temp_parts) < 18:
            temp_parts.append('')
        return temp_parts[:18]
    
    # Tìm vị trí bắt đầu của phần sau NULL trong dòng gốc
    comma_count = 0
    start_pos = 0
    for i, ch in enumerate(line_str):
        if ch == ',':
            if comma_count == null_index:
                start_pos = i + 1
                break
            comma_count += 1
    
    after_null_str = line_str[start_pos:] if start_pos < len(line_str) else ''
    
    # Áp dụng logic tách 2 cấp cho phần sau NULL
    answers = split_after_null_by_logic(after_null_str)
    
    # Phần từ đầu đến NULL (bao gồm cả NULL)
    before_null = temp_parts[:null_index + 1]
    
    # Đảm bảo before_null có đúng 14 cột (index 0-13)
    while len(before_null) < 14:
        before_null.append('')
    before_null = before_null[:14]
    
    # Kết quả: 14 cột đầu + 4 câu trả lời = 18 cột
    return before_null + answers


def inspect_error_lines(filepath):
    """
    Đọc file và in ra các dòng có số cột khác 18 sau khi xử lý
    """
    print("\n" + "="*80)
    print("KIỂM TRA CÁC DÒNG BỊ LỖI (SỐ CỘT KHÁC 18)")
    print("="*80)
    
    error_lines = []
    still_error_lines = []  # Dòng vẫn lỗi sau khi xử lý
    line_number = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line_number += 1
            line = line.strip()
            if not line:
                continue
            
            # Xử lý dòng
            processed = process_line_fixed(line)
            
            if len(processed) != 18:
                error_lines.append({
                    'line_number': line_number,
                    'num_cols': len(processed),
                    'content': line,
                    'processed': processed
                })
            
            # Kiểm tra phần sau NULL có đúng 4 câu không
            # Tìm vị trí NULL trong processed
            null_idx = -1
            for i, p in enumerate(processed):
                if p == '' or p.upper() == 'NULL':
                    null_idx = i
                    break
            
            if null_idx != -1 and null_idx + 4 < len(processed):
                answers_after_null = processed[null_idx + 1:null_idx + 5]
                # Nếu câu cuối cùng có dấu phẩy (chưa xử lý hết)
                if any(',' in str(ans) for ans in answers_after_null):
                    still_error_lines.append({
                        'line_number': line_number,
                        'content': line,
                        'answers': answers_after_null
                    })
    
    # In thống kê
    print(f"\nTổng số dòng trong file: {line_number}")
    print(f"Số dòng có số cột KHÁC 18 sau xử lý: {len(error_lines)}")
    print(f"Số dòng vẫn còn dấu phẩy trong câu trả lời: {len(still_error_lines)}")
    
    # In các dòng vẫn còn lỗi (có dấu phẩy trong câu trả lời)
    if still_error_lines:
        print("\n" + "="*80)
        print("CÁC DÒNG VẪN CÒN DẤU PHẨY TRONG CÂU TRẢ LỜI (CẦN XEM XÉT THÊM)")
        print("="*80)
        
        for i, error in enumerate(still_error_lines):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(still_error_lines)} | Số dòng gốc: {error['line_number']}")
            print(f"{'='*80}")
            print(f"Nội dung gốc:")
            print(f"{error['content'][:500]}{'...' if len(error['content']) > 500 else ''}")
            print(f"\nCác câu trả lời sau NULL:")
            print(f"  Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}")
            print(f"  Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}")
            print(f"  Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}")
            print(f"  Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}")
            print(f"\n{'#'*80}")
    
    # Ghi ra file log
    if still_error_lines:
        log_file = f"still_error_lines_{FILE_NAME}.txt"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"FILE: {SURVEY_FILE}\n")
            f.write(f"Các dòng vẫn còn dấu phẩy trong câu trả lời sau khi xử lý\n")
            f.write("="*80 + "\n")
            
            for error in still_error_lines:
                f.write(f"\n{'='*80}\n")
                f.write(f"DÒNG {error['line_number']}\n")
                f.write(f"{'='*80}\n")
                f.write(f"Nội dung gốc:\n{error['content']}\n")
                f.write(f"\nCác câu trả lời sau NULL:\n")
                f.write(f"Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}\n")
                f.write(f"Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}\n")
                f.write(f"Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}\n")
                f.write(f"Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}\n")
                f.write("\n" + "#"*80 + "\n")
        
        print(f"\n📁 Đã ghi chi tiết các dòng còn lỗi vào file: {log_file}")
    
    if len(error_lines) == 0 and len(still_error_lines) == 0:
        print("\n✅ TẤT CẢ các dòng đều được xử lý thành công!")
    else:
        print(f"\n⚠️ Còn {len(still_error_lines)} dòng cần xem xét thủ công.")


def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # KIỂM TRA VÀ IN RA CÁC DÒNG LỖI
    inspect_error_lines(SURVEY_FILE)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)


if __name__ == "__main__":
    main()
