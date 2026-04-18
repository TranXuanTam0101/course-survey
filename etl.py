import os
import sys
import re
import csv
from datetime import datetime
from typing import List, Tuple, Optional
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
   
    if len(value) == 7 and value.isdigit():
        return True
   
    if len(value) == 7 and value.startswith("TG"):
        return True
   
    if value == "gvDacThu_TKTH":
        return True
   
    return False

def split_by_condition_1(text):
    """Cấp 1: Tách với điều kiện trước và sau dấu phẩy đều không có khoảng trắng"""
    parts = []
    current = []
    i = 0
   
    while i < len(text):
        if text[i] == ',':
            has_space_before = (i > 0 and text[i-1] == ' ')
            has_space_after = (i + 1 < len(text) and text[i+1] == ' ')
           
            if not has_space_before and not has_space_after:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
   
    if current:
        parts.append(''.join(current).strip())
   
    return [p for p in parts if p]

def split_by_condition_2(text):
    """Cấp 2: Tách với điều kiện sau dấu phẩy không có khoảng trắng"""
    parts = []
    current = []
    i = 0
   
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text) and text[i+1] == ' ':
                current.append(',')
            else:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
        else:
            current.append(text[i])
        i += 1
   
    if current:
        parts.append(''.join(current).strip())
   
    return [p for p in parts if p]

def split_by_condition_3(text):
    """Cấp 3: Tách với điều kiện sau dấu phẩy không có khoảng trắng VÀ chữ in hoa đầu tiên"""
    parts = []
    current = []
    i = 0
   
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text):
                next_char = text[i + 1]
                if next_char != ' ' and next_char.isupper():
                    if current:
                        parts.append(''.join(current).strip())
                        current = []
                else:
                    current.append(',')
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
   
    if current:
        parts.append(''.join(current).strip())
   
    return [p for p in parts if p]

def try_create_4th_column(parts):
    """Thử lấy phần tử cuối cùng sau dấu phẩy của cột cuối để tạo cột thứ 4
       Trả về: (success, new_parts)
       success = True nếu tạo được 4 cột, False nếu không
    """
    if len(parts) == 3:
        last_col = parts[-1]
        if ',' in last_col:
            sub_parts = last_col.split(',')
            if len(sub_parts) >= 2:
                last_element = sub_parts[-1].strip()
                parts[-1] = ','.join(sub_parts[:-1]).strip()
                parts.append(last_element)
                return True, parts
    return False, parts

# ==================== PHẦN XỬ LÝ THÔNG MINH MỚI ====================

def classify_by_keywords(text: str, keyword_sets: List[Tuple[str, List[str]]]) -> str:
    """
    Phân loại văn bản dựa trên từ khóa
    
    Args:
        text: Văn bản cần phân loại
        keyword_sets: Danh sách các bộ (tên_category, list_từ_khóa)
    
    Returns:
        Tên category phù hợp nhất
    """
    if not text or text.strip() == '':
        return ''
    
    text_lower = text.lower()
    best_match = ''
    max_score = 0
    
    for category, keywords in keyword_sets:
        score = 0
        for keyword in keywords:
            if keyword in text_lower:
                # Từ khóa càng dài càng có trọng số cao
                score += len(keyword)
        if score > max_score:
            max_score = score
            best_match = category
    
    return best_match


def extract_categories_from_text(original_text: str) -> Tuple[str, str, str, str]:
    """
    Trích xuất 4 phần từ chuỗi gốc dựa trên nội dung ngữ nghĩa
    
    Chiến lược:
    1. Nếu có dấu phẩy rõ ràng → tách thủ công
    2. Nếu không → phân loại dựa trên từ khóa
    """
    if not original_text or original_text.strip() == '':
        return ('', '', '', '')
    
    text = original_text.strip()
    
    # Xử lý chuỗi chỉ toàn ký tự đặc biệt
    if re.match(r'^[.,;:/\\mkn\s]+$', text, re.IGNORECASE):
        return ('', '', '', '')
    
    # === BƯỚC 1: THỬ TÁCH BẰNG DẤU PHẨY (GIỮ NGUYÊN NGỮ NGHĨA) ===
    # Tách cơ bản
    raw_parts = [p.strip() for p in text.split(',') if p.strip()]
    
    # Nếu có 4 phần rõ ràng → trả về
    if len(raw_parts) == 4:
        return (raw_parts[0], raw_parts[1], raw_parts[2], raw_parts[3])
    
    # Nếu có 5 phần → thử ghép 2 phần cuối hoặc phần cuối là "không"
    if len(raw_parts) == 5:
        last_part = raw_parts[-1].lower()
        if any(kw in last_part for kw in ['không', 'ko', 'khong']):
            # "không" là câu 16
            return (raw_parts[0], raw_parts[1], raw_parts[2], raw_parts[3] + ', ' + raw_parts[4])
        else:
            # Ghép 2 phần cuối
            combined_last = raw_parts[3] + ', ' + raw_parts[4]
            return (raw_parts[0], raw_parts[1], raw_parts[2], combined_last)
    
    # Nếu có 6 phần trở lên → lấy 3 phần đầu, phần cuối gộp vào câu 16
    if len(raw_parts) >= 6:
        cau16 = ', '.join(raw_parts[3:])
        return (raw_parts[0], raw_parts[1], raw_parts[2], cau16)
    
    # === BƯỚC 2: PHÂN LOẠI DỰA TRÊN TỪ KHÓA ===
    
    # Định nghĩa từ khóa cho từng cột
    keywords_cau13 = [
        'nội dung', 'chuẩn đầu ra', 'chương trình', 'giáo trình',
        'đầy đủ', 'phù hợp', 'bám sát', 'rõ ràng', 'chuẩn', 'hợp lý',
        'chi tiết', 'cụ thể', 'đúng', 'sát', 'trọng tâm', 'thực tế',
        'kiến thức', 'học phần', 'môn học', 'cập nhật', 'đa dạng',
        'nội dung học', 'đầu ra', 'chương trình học'
    ]
    
    keywords_cau14 = [
        'giảng dạy', 'phương pháp', 'hoạt động', 'truyền đạt',
        'dễ hiểu', 'nhiệt tình', 'tận tâm', 'tận tình', 'tâm huyết',
        'vui vẻ', 'sôi nổi', 'thú vị', 'hấp dẫn', 'sinh động',
        'tương tác', 'thực hành', 'ví dụ', 'thực tế', 'gần gũi',
        'thân thiện', 'dễ thương', 'năng động', 'linh hoạt',
        'cô dạy', 'thầy dạy', 'giảng viên dạy'
    ]
    
    keywords_cau15 = [
        'kiểm tra', 'đánh giá', 'thi cử', 'chấm điểm',
        'công bằng', 'minh bạch', 'công khai', 'khách quan',
        'nghiêm túc', 'đúng thực lực', 'phù hợp', 'rõ ràng',
        'công tâm', 'liêm minh', 'chính xác', 'đáng tin cậy',
        'kiểm tra đánh giá', 'đánh giá công bằng'
    ]
    
    keywords_cau16 = [
        'góp ý', 'cảm ơn', 'yêu', 'thích', 'mong muốn', 'hy vọng',
        'không có', 'ko có', 'không ạ', 'ko ạ', 'ok', 'ổn',
        'tốt', 'hay', 'tuyệt vời', 'cảm nhận', 'cảm xúc',
        'không', 'ko', 'khong', 'không có ý kiến'
    ]
    
    # Nếu chỉ có 1 phần → phân loại toàn bộ vào cột phù hợp nhất
    if len(raw_parts) == 1:
        single_text = raw_parts[0]
        
        # Phát hiện từ khóa "không" ở cuối
        if any(kw in single_text.lower() for kw in ['không', 'ko', 'khong']):
            return ('', '', '', single_text)
        
        # Phân loại dựa trên nội dung
        cat13 = classify_by_keywords(single_text, [('cau13', keywords_cau13)])
        cat14 = classify_by_keywords(single_text, [('cau14', keywords_cau14)])
        cat15 = classify_by_keywords(single_text, [('cau15', keywords_cau15)])
        cat16 = classify_by_keywords(single_text, [('cau16', keywords_cau16)])
        
        # Ưu tiên theo thứ tự: cau16 > cau14 > cau13 > cau15
        if cat16 == 'cau16':
            return ('', '', '', single_text)
        elif cat14 == 'cau14':
            return ('', single_text, '', '')
        elif cat13 == 'cau13':
            return (single_text, '', '', '')
        elif cat15 == 'cau15':
            return ('', '', single_text, '')
        else:
            # Mặc định vào câu 16
            return ('', '', '', single_text)
    
    # Nếu có 2-3 phần → phân phối dựa trên từ khóa
    result = ['', '', '', '']
    
    for i, part in enumerate(raw_parts):
        part_lower = part.lower()
        
        # Ưu tiên: "không" vào câu 16
        if any(kw in part_lower for kw in ['không', 'ko', 'khong']):
            result[3] = part if not result[3] else result[3] + ', ' + part
            continue
        
        # Phân loại nội dung
        scores = {
            0: sum(1 for kw in keywords_cau13 if kw in part_lower),  # cau13
            1: sum(1 for kw in keywords_cau14 if kw in part_lower),  # cau14
            2: sum(1 for kw in keywords_cau15 if kw in part_lower),  # cau15
            3: sum(1 for kw in keywords_cau16 if kw in part_lower),  # cau16
        }
        
        # Tìm cột có điểm cao nhất
        best_col = max(scores, key=scores.get)
        
        # Nếu điểm = 0 (không có từ khóa nào), ưu tiên theo thứ tự vị trí
        if scores[best_col] == 0:
            # Dựa vào vị trí trong mảng
            if i == 0:
                best_col = 0  # cau13
            elif i == 1:
                best_col = 1  # cau14
            elif i == 2:
                best_col = 2  # cau15
            else:
                best_col = 3  # cau16
        
        if result[best_col]:
            result[best_col] += ', ' + part
        else:
            result[best_col] = part
    
    # Đảm bảo thứ tự: cau13, cau14, cau15, cau16
    return (result[0], result[1], result[2], result[3])


def split_after_null_advanced(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL - PHIÊN BẢN NÂNG CAO
    Kết hợp 3 cấp độ cũ + xử lý thông minh cho các trường hợp đặc biệt
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    # ===== CẤP 1: Tách với điều kiện trước và sau dấu phẩy đều không có khoảng trắng =====
    parts_level1 = split_by_condition_1(original_text)
    
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success:
            return new_parts[:4], None
    
    # ===== CẤP 2: Tách với điều kiện sau dấu phẩy không có khoảng trắng =====
    parts_level2 = split_by_condition_2(original_text)
    
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success:
            return new_parts[:4], None
    
    # ===== CẤP 3: Tách với điều kiện sau dấu phẩy không có khoảng trắng VÀ chữ in hoa đầu tiên =====
    parts_level3 = split_by_condition_3(original_text)
    
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success:
            return new_parts[:4], None
    
    # ===== NẾU VẪN CHƯA ĐƯỢC, DÙNG XỬ LÝ THÔNG MINH =====
    cau13, cau14, cau15, cau16 = extract_categories_from_text(original_text)
    
    # Ghi nhận lỗi để báo cáo (nhưng đã xử lý thành công)
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'level1_result': parts_level1,
        'level2_result': parts_level2,
        'level3_result': parts_level3,
        'final_count': len(parts_level3),
        'message': f'Sau 3 cấp có {len(parts_level3)} cột - Đã xử lý thông minh thành 4 cột',
        'smart_result': [cau13, cau14, cau15, cau16]
    }
    
    return [cau13, cau14, cau15, cau16], error_info

# ==================== KẾT THÚC PHẦN XỬ LÝ THÔNG MINH ====================

def process_row(row, row_number=None):
    """
    Xử lý một dòng CSV theo logic
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
       
        # Bước 6: Xác định TenHP
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
       
        # Bước 12: Xác định cột NULL
        null_index = -1
        null_value = ''
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
       
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
                null_value = potential_null if potential_null else 'NULL'
       
        # ========== PHẦN 2: XỬ LÝ CÁC CỘT SAU CỘT NULL ==========
        cau13 = cau14 = cau15 = cau16 = ''
        split_errors = []
       
        if null_index >= 0 and null_index + 1 < len(row):
            after_null = row[null_index + 1:]
            # SỬ DỤNG HÀM NÂNG CẤP MỚI
            split_result, error = split_after_null_advanced(after_null, row_number)
           
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
    error_rows = []
   
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
        return rows, error_rows
       
    except Exception as e:
        print(f"Lỗi đọc file: {e}")
        return [], []

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
    rows, read_errors = read_csv_manual(SURVEY_FILE)
   
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
   
    # In các dòng có lỗi tách (không đủ 4 cột sau 3 cấp - đã được xử lý thông minh)
    if split_errors:
        print(f"\n{'='*60}")
        print("CÁC DÒNG ĐÃ ĐƯỢC XỬ LÝ THÔNG MINH (KHÔNG ĐỦ 4 CỘT SAU 3 CẤP)")
        print(f"{'='*60}")
        for err in split_errors:
            print(f"\nDòng {err['row_number']}:")
            print(f" Chuỗi sau NULL: {err['original_after_null'][:200]}")
            print(f" Số cột sau Cấp 3: {err['final_count']}")
            print(f" Kết quả xử lý thông minh: {err.get('smart_result', 'N/A')}")
            print(f" Kết quả Cấp 1: {err['level1_result']}")
            print(f" Kết quả Cấp 2: {err['level2_result']}")
            print(f" Kết quả Cấp 3: {err['level3_result']}")
       
        # Lưu file lỗi tách (để kiểm tra thủ công)
        split_error_df = pd.DataFrame(split_errors)
        split_error_filename = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(split_error_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu {len(split_errors)} dòng đã xử lý thông minh vào file: {split_error_filename}")
   
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
            print(f"{'='*60}")
        else:
            print("Upload file thất bại!")
            sys.exit(1)
    else:
        print("Không có dòng nào được xử lý thành công!")
        sys.exit(1)

if __name__ == "__main__":
    main()
