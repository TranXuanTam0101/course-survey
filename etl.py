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
    
    LOGIC:
    - Ngay sau dấu phẩy ko có khoảng trắng => tách cột
    - Nếu vẫn tạo thành >4 câu thì tiếp tục xét thêm điều kiện:
       - Ngay sau dấu phẩy ko có khoảng trắng + chữ viết cái đầu Viết Hoa => tách cột
    """
    if not after_null_str:
        return ['', '', '', '']
    
    # Lưu lại chuỗi gốc để xử lý cấp 2 nếu cần
    original_str = after_null_str
    
    # ========== CẤP 1: Ngay sau dấu phẩy ko có khoảng trắng => tách cột ==========
    parts_level1 = []
    current = []
    i = 0
    length = len(after_null_str)
    
    while i < length:
        if after_null_str[i] == ',':
            # Kiểm tra: Ngay sau dấu phẩy có khoảng trắng không?
            is_delimiter = False
            if i + 1 < length:
                next_char = after_null_str[i + 1]
                # Ngay sau dấu phẩy ko có khoảng trắng => tách cột
                if next_char != ' ':
                    is_delimiter = True
            else:
                # Dấu phẩy cuối dòng => tách cột
                is_delimiter = True
            
            if is_delimiter:
                # Tách cột
                current_str = ''.join(current).strip()
                if current_str:
                    parts_level1.append(current_str)
                current = []
            else:
                # Không tách, giữ dấu phẩy trong nội dung
                current.append(',')
        else:
            current.append(after_null_str[i])
        i += 1
    
    # Thêm phần tử cuối cùng
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level1.append(current_str)
    
    # Nếu đã có 4 câu hoặc ít hơn, trả về kết quả
    if len(parts_level1) <= 4:
        while len(parts_level1) < 4:
            parts_level1.append('')
        return parts_level1[:4]
    
    # ========== CẤP 2: Vẫn tạo thành >4 câu, xét thêm điều kiện ==========
    # Điều kiện: Ngay sau dấu phẩy ko có khoảng trắng + chữ viết cái đầu Viết Hoa => tách cột
    parts_level2 = []
    current = []
    i = 0
    length = len(original_str)
    
    while i < length:
        if original_str[i] == ',':
            is_delimiter = False
            if i + 1 < length:
                next_char = original_str[i + 1]
                # Ngay sau dấu phẩy ko có khoảng trắng + chữ cái đầu Viết Hoa => tách cột
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
            current.append(original_str[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts_level2.append(current_str)
    
    # Đảm bảo có đúng 4 câu trả lời
    while len(parts_level2) < 4:
        parts_level2.append('')
    
    if len(parts_level2) > 4:
        # Gộp phần dư vào câu cuối cùng (Cau16)
        parts_level2[3] = ','.join(parts_level2[3:])
        parts_level2 = parts_level2[:4]
    
    return parts_level2


def process_line_fixed(line):
    """
    Xử lý một dòng:
    - Chỉ xử lý tách cột sau cột có giá trị NULL
    - Giữ nguyên phần từ đầu đến NULL
    - Phần sau NULL: áp dụng logic split_after_null_by_logic
    - Trả về 18 cột
    """
    line_str = line.strip()
    if not line_str:
        return [''] * 18
    
    # Tách tạm thời để tìm vị trí cột NULL
    temp_parts = line_str.split(',')
    
    # Tìm vị trí cột NULL (cột có giá trị NULL)
    null_index = -1
    for i, p in enumerate(temp_parts):
        if p == '' or p.upper() == 'NULL':
            null_index = i
            break
    
    if null_index == -1:
        # Không tìm thấy cột NULL, giữ nguyên
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
    
    # Lấy phần chuỗi sau cột NULL
    after_null_str = line_str[start_pos:] if start_pos < len(line_str) else ''
    
    # Áp dụng logic tách cột cho phần sau NULL
    answers = split_after_null_by_logic(after_null_str)
    
    # Phần từ đầu đến NULL (bao gồm cả cột NULL)
    before_null = temp_parts[:null_index + 1]
    
    # Đảm bảo before_null có đúng 14 cột (index 0 đến 13)
    while len(before_null) < 14:
        before_null.append('')
    before_null = before_null[:14]
    
    # Kết quả: 14 cột đầu + 4 câu trả lời = 18 cột
    return before_null + answers


def inspect_error_lines(filepath):
    """
    Đọc file, xử lý theo logic, in ra các dòng vẫn còn tạo thành >18 cột
    """
    print("\n" + "="*80)
    print("KIỂM TRA CÁC DÒNG DỮ LIỆU")
    print("="*80)
    
    error_lines = []  # Các dòng có số cột != 18 sau khi xử lý
    line_number = 0
    total_lines = 0
    
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            total_lines += 1
            line = line.strip()
            if not line:
                continue
            
            line_number += 1
            
            # Xử lý dòng theo logic
            processed = process_line_fixed(line)
            
            # Kiểm tra số cột sau khi xử lý
            if len(processed) != 18:
                error_lines.append({
                    'line_number': line_number,
                    'num_cols': len(processed),
                    'content': line,
                    'processed': processed
                })
    
    # In thống kê
    print(f"\nTổng số dòng trong file: {total_lines}")
    print(f"Số dòng có số cột KHÁC 18 sau khi xử lý: {len(error_lines)}")
    
    # In ra tất cả các dòng vẫn còn tạo thành >18 cột
    if error_lines:
        print("\n" + "="*80)
        print("CÁC DÒNG VẪN CÒN TẠO THÀNH >18 CỘT (CẦN XEM XÉT THỦ CÔNG)")
        print("="*80)
        
        for i, error in enumerate(error_lines):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(error_lines)} | Số dòng gốc: {error['line_number']} | Số cột: {error['num_cols']}")
            print(f"{'='*80}")
            print(f"Nội dung gốc:")
            print(f"{error['content']}")
            
            print(f"\nCác cột sau khi xử lý:")
            for idx, part in enumerate(error['processed']):
                print(f"  Cột {idx}: {part}")
            
            print(f"\n{'#'*80}")
        
        # Ghi ra file log
        log_file = f"error_lines_{FILE_NAME}.txt"
        with open(log_file, 'w', encoding='utf-8') as f:
            f.write(f"FILE: {SURVEY_FILE}\n")
            f.write(f"Tổng số dòng: {total_lines}\n")
            f.write(f"Số dòng có số cột KHÁC 18: {len(error_lines)}\n")
            f.write("\n" + "="*80 + "\n")
            
            for error in error_lines:
                f.write(f"\n{'='*80}\n")
                f.write(f"DÒNG {error['line_number']} | Số cột: {error['num_cols']}\n")
                f.write(f"{'='*80}\n")
                f.write(f"Nội dung gốc:\n{error['content']}\n")
                f.write(f"\nCác cột sau khi xử lý:\n")
                for idx, part in enumerate(error['processed']):
                    f.write(f"  Cột {idx}: {part}\n")
                f.write("\n" + "#"*80 + "\n")
        
        print(f"\n📁 Đã ghi chi tiết vào file: {log_file}")
    else:
        print("\n✅ TẤT CẢ các dòng đều được xử lý thành công thành 18 cột!")


def main():
    # Kết nối Azure Blob
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Tải file từ blob
    download_from_blob(blob_service)
    
    # Xử lý và in ra các dòng lỗi
    inspect_error_lines(SURVEY_FILE)
    
    # Xóa file tạm
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)


if __name__ == "__main__":
    main()
