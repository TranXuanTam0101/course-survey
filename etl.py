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

def is_special_char_only(s):
    """Kiểm tra chuỗi chỉ chứa ký tự đặc biệt (không phải chữ cái, không phải số, không phải khoảng trắng)"""
    if not s or s.strip() == '':
        return True
    for ch in s:
        if ch.isalnum() or ch.isspace():
            return False
    return True

def smart_split_by_comma_advanced(line):
    """
    Tách dòng bằng dấu phẩy với logic nâng cao:
    1. Ngay sau dấu phẩy không có khoảng trắng -> TÁCH CỘT
    2. Nếu vẫn >18 cột -> xét thêm: sau dấu phẩy không khoảng trắng + chữ cái đầu viết hoa -> TÁCH CỘT
    3. Xóa cột rỗng
    """
    # Lần 1: Chỉ xét điều kiện "sau dấu phẩy không có khoảng trắng"
    parts = []
    current = []
    i = 0
    length = len(line)
    
    while i < length:
        if line[i] == ',':
            is_delimiter = False
            if i + 1 < length:
                next_char = line[i + 1]
                if next_char != ' ':
                    is_delimiter = True
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                if current_str:
                    parts.append(current_str)
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts.append(current_str)
    
    # Nếu số cột <= 18, trả về luôn
    if len(parts) <= 18:
        return parts
    
    # Lần 2: Nếu vẫn >18 cột, áp dụng thêm điều kiện "chữ cái đầu viết hoa"
    parts2 = []
    current = []
    i = 0
    
    while i < length:
        if line[i] == ',':
            is_delimiter = False
            if i + 1 < length:
                next_char = line[i + 1]
                # Điều kiện: không khoảng trắng VÀ (chữ hoa hoặc số)
                if next_char != ' ' and (next_char.isupper() or next_char.isdigit()):
                    is_delimiter = True
            else:
                is_delimiter = True
            
            if is_delimiter:
                current_str = ''.join(current).strip()
                if current_str:
                    parts2.append(current_str)
                current = []
            else:
                current.append(',')
        else:
            current.append(line[i])
        i += 1
    
    if current:
        current_str = ''.join(current).strip()
        if current_str:
            parts2.append(current_str)
    
    return parts2

def find_null_position(line):
    """Tìm vị trí bắt đầu của từ NULL trong dòng (không dùng regex)"""
    line_upper = line.upper()
    null_index = line_upper.find('NULL')
    
    if null_index == -1:
        return -1
    
    # Kiểm tra NULL phải là từ độc lập (không phải một phần của từ khác)
    # Kiểm tra ký tự trước NULL
    if null_index > 0:
        prev_char = line[null_index - 1]
        if prev_char.isalnum():
            # Tìm lại NULL khác
            next_null = line_upper.find('NULL', null_index + 4)
            if next_null != -1:
                return find_null_position_after(line, next_null)
            return -1
    
    # Kiểm tra ký tự sau NULL
    after_null_index = null_index + 4
    if after_null_index < len(line):
        next_char = line[after_null_index]
        if next_char.isalnum():
            # Tìm lại NULL khác
            next_null = line_upper.find('NULL', after_null_index)
            if next_null != -1:
                return find_null_position_after(line, next_null)
            return -1
    
    return null_index

def find_null_position_after(line, start_pos):
    """Tìm NULL từ vị trí start_pos trở đi"""
    line_upper = line.upper()
    null_index = line_upper.find('NULL', start_pos)
    
    if null_index == -1:
        return -1
    
    if null_index > 0:
        prev_char = line[null_index - 1]
        if prev_char.isalnum():
            return find_null_position_after(line, null_index + 4)
    
    after_null_index = null_index + 4
    if after_null_index < len(line):
        next_char = line[after_null_index]
        if next_char.isalnum():
            return find_null_position_after(line, after_null_index)
    
    return null_index

def extract_answers_after_null(line):
    """
    Tách phần sau cột NULL thành 4 câu trả lời Cau13, Cau14, Cau15, Cau16
    """
    # Tìm vị trí của NULL trong dòng
    null_pos = find_null_position(line)
    
    if null_pos == -1:
        return ['', '', '', ''], []
    
    # Tìm vị trí dấu phẩy sau NULL
    after_null_start = null_pos + 4  # len('NULL') = 4
    
    # Bỏ qua dấu phẩy nếu có
    while after_null_start < len(line) and line[after_null_start] != ',':
        after_null_start += 1
    after_null_start += 1  # Bỏ qua dấu phẩy
    
    after_null_str = line[after_null_start:] if after_null_start < len(line) else ''
    
    if not after_null_str or after_null_str.strip() == '':
        return ['', '', '', ''], []
    
    # Tách phần sau NULL bằng logic nâng cao
    answer_parts = smart_split_by_comma_advanced(after_null_str)
    
    # Lưu lại số cột ban đầu để kiểm tra
    original_answer_count = len(answer_parts)
    
    # Kiểm tra nếu tất cả các phần chỉ là ký tự đặc biệt -> trả về rỗng
    all_special = all(is_special_char_only(p) for p in answer_parts)
    if all_special and len(answer_parts) > 0:
        return ['', '', '', ''], answer_parts
    
    # Xóa các cột rỗng
    answer_parts = [p for p in answer_parts if p and p.strip()]
    
    # Đảm bảo có đúng 4 câu trả lời
    while len(answer_parts) < 4:
        answer_parts.append('')
    
    if len(answer_parts) > 4:
        # Gộp phần dư vào câu cuối
        answer_parts[3] = ','.join(answer_parts[3:])
        answer_parts = answer_parts[:4]
    
    return answer_parts[:4], answer_parts

def inspect_error_lines(filepath):
    """
    Đọc file và in ra TẤT CẢ các dòng có số cột khác 18 sau khi tách
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
            
            # Tách toàn bộ dòng bằng dấu phẩy đơn giản để kiểm tra
            temp_parts = line.split(',')
            num_cols_temp = len(temp_parts)
            
            # Lấy 4 câu trả lời từ phần sau NULL
            answers, raw_answers = extract_answers_after_null(line)
            
            # Lấy 14 cột đầu (0-13)
            before_null = temp_parts[:14] if len(temp_parts) >= 14 else temp_parts + [''] * (14 - len(temp_parts))
            
            # Ghép lại thành 18 cột
            full_parts = before_null[:14] + answers
            
            # Kiểm tra sau khi xử lý vẫn còn lỗi
            if len(full_parts) != 18:
                still_error_lines.append({
                    'line_number': line_number,
                    'num_cols_original': num_cols_temp,
                    'num_cols_processed': len(full_parts),
                    'content': line,
                    'before_null': before_null,
                    'raw_answers': raw_answers,
                    'answers': answers,
                    'full_parts': full_parts
                })
            
            # Nếu số cột sau tách đơn giản khác 18, ghi nhận lỗi
            if num_cols_temp != 18:
                error_lines.append({
                    'line_number': line_number,
                    'num_cols': num_cols_temp,
                    'content': line,
                    'answers': answers
                })
    
    # In thống kê
    print(f"\nTổng số dòng trong file: {line_number}")
    print(f"Số dòng có số cột KHÁC 18 (trước xử lý): {len(error_lines)}")
    print(f"Số dòng vẫn KHÁC 18 (sau xử lý): {len(still_error_lines)}")
    
    if len(error_lines) == 0:
        print("\n✅ KHÔNG có dòng lỗi nào! Tất cả các dòng đều có 18 cột.")
        return
    
    # IN RA TẤT CẢ CÁC DÒNG LỖI TRƯỚC XỬ LÝ
    print("\n" + "="*80)
    print(f"CHI TIẾT CÁC DÒNG LỖI TRƯỚC XỬ LÝ (Tổng số: {len(error_lines)} dòng)")
    print("="*80)
    
    for i, error in enumerate(error_lines[:10]):  # Chỉ in 10 dòng đầu để tránh tràn màn hình
        print(f"\n{'='*80}")
        print(f"DÒNG {i+1}/{len(error_lines)} | Số dòng gốc: {error['line_number']} | Số cột ban đầu: {error['num_cols']}")
        print(f"{'='*80}")
        print(f"Nội dung gốc:")
        print(f"{error['content'][:500]}{'...' if len(error['content']) > 500 else ''}")
        
        print(f"\n📌 4 câu trả lời sau khi xử lý (Cau13 -> Cau16):")
        print(f"  Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}")
        print(f"  Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}")
        print(f"  Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}")
        print(f"  Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}")
        
        print(f"\n{'#'*80}")
    
    if len(error_lines) > 10:
        print(f"\n... và {len(error_lines) - 10} dòng lỗi khác (xem file log để biết chi tiết)")
    
    # IN RA TẤT CẢ CÁC DÒNG VẪN LỖI SAU KHI XỬ LÝ
    if still_error_lines:
        print("\n" + "="*80)
        print(f"⚠️ CÁC DÒNG VẪN LỖI SAU KHI XỬ LÝ (Tổng số: {len(still_error_lines)} dòng)")
        print("="*80)
        
        for i, error in enumerate(still_error_lines):
            print(f"\n{'='*80}")
            print(f"DÒNG {i+1}/{len(still_error_lines)} | Số dòng gốc: {error['line_number']}")
            print(f"Số cột ban đầu: {error['num_cols_original']} | Số cột sau xử lý: {error['num_cols_processed']}")
            print(f"{'='*80}")
            print(f"Nội dung gốc:")
            print(f"{error['content'][:500]}{'...' if len(error['content']) > 500 else ''}")
            
            print(f"\n📌 14 cột đầu (0-13):")
            for idx, part in enumerate(error['before_null'][:14]):
                print(f"  Cột {idx}: {part[:100]}{'...' if len(part) > 100 else ''}")
            
            print(f"\n📌 Phần sau NULL (raw answers): {len(error['raw_answers'])} phần tử")
            for idx, part in enumerate(error['raw_answers'][:10]):
                print(f"  Raw {idx}: {part[:100]}{'...' if len(part) > 100 else ''}")
            
            print(f"\n📌 4 câu trả lời sau xử lý:")
            print(f"  Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}")
            print(f"  Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}")
            print(f"  Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}")
            print(f"  Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}")
            
            print(f"\n📌 FULL PARTS ({len(error['full_parts'])} cột):")
            for idx, part in enumerate(error['full_parts'][:20]):
                print(f"  Part {idx}: {part[:100]}{'...' if len(part) > 100 else ''}")
            
            print(f"\n{'#'*80}")
    
    # Ghi ra file log
    log_file = f"error_lines_{FILE_NAME}.txt"
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"FILE: {SURVEY_FILE}\n")
        f.write(f"Tổng số dòng: {line_number}\n")
        f.write(f"Số dòng lỗi trước xử lý: {len(error_lines)}\n")
        f.write(f"Số dòng vẫn lỗi sau xử lý: {len(still_error_lines)}\n")
        f.write("\n" + "="*80 + "\n")
        
        # Ghi tất cả dòng lỗi trước xử lý
        f.write("\n=== DÒNG LỖI TRƯỚC XỬ LÝ ===\n")
        for error in error_lines:
            f.write(f"\n{'='*80}\n")
            f.write(f"DÒNG {error['line_number']} | Số cột: {error['num_cols']}\n")
            f.write(f"{'='*80}\n")
            f.write(f"Nội dung gốc:\n{error['content']}\n")
            f.write(f"\n4 câu trả lời (Cau13 -> Cau16):\n")
            f.write(f"Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}\n")
            f.write(f"Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}\n")
            f.write(f"Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}\n")
            f.write(f"Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}\n")
            f.write("\n" + "#"*80 + "\n")
        
        # Ghi tất cả dòng vẫn lỗi sau xử lý
        if still_error_lines:
            f.write("\n\n=== DÒNG VẪN LỖI SAU XỬ LÝ ===\n")
            for error in still_error_lines:
                f.write(f"\n{'='*80}\n")
                f.write(f"DÒNG {error['line_number']}\n")
                f.write(f"Số cột ban đầu: {error['num_cols_original']}\n")
                f.write(f"Số cột sau xử lý: {error['num_cols_processed']}\n")
                f.write(f"{'='*80}\n")
                f.write(f"Nội dung gốc:\n{error['content']}\n")
                f.write(f"\n14 cột đầu:\n")
                for idx, part in enumerate(error['before_null'][:14]):
                    f.write(f"  Cột {idx}: {part}\n")
                f.write(f"\nRaw answers ({len(error['raw_answers'])} phần tử):\n")
                for idx, part in enumerate(error['raw_answers']):
                    f.write(f"  Raw {idx}: {part}\n")
                f.write(f"\n4 câu trả lời:\n")
                f.write(f"  Cau13: {error['answers'][0] if len(error['answers']) > 0 else ''}\n")
                f.write(f"  Cau14: {error['answers'][1] if len(error['answers']) > 1 else ''}\n")
                f.write(f"  Cau15: {error['answers'][2] if len(error['answers']) > 2 else ''}\n")
                f.write(f"  Cau16: {error['answers'][3] if len(error['answers']) > 3 else ''}\n")
                f.write(f"\nFull parts ({len(error['full_parts'])} cột):\n")
                for idx, part in enumerate(error['full_parts']):
                    f.write(f"  Part {idx}: {part}\n")
                f.write("\n" + "#"*80 + "\n")
    
    print(f"\n📁 Đã ghi chi tiết TẤT CẢ các dòng lỗi vào file: {log_file}")
    print(f"\n✅ Kiểm tra xong! Đã xử lý {len(error_lines)} dòng lỗi.")

def main():
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    download_from_blob(blob_service)
    
    inspect_error_lines(SURVEY_FILE)
    
    try:
        df = pd.read_csv(SURVEY_FILE, header=None, on_bad_lines='skip')
        print(f"\n✅ Đã đọc được {len(df)} dòng bằng pandas (bỏ qua các dòng lỗi)")
    except Exception as e:
        print(f"\n❌ Lỗi khi đọc file: {e}")
    
    if os.path.exists(SURVEY_FILE):
        os.remove(SURVEY_FILE)

if __name__ == "__main__":
    main()
