import os
import sys
import re
import csv
import json
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient
import google.generativeai as genai

# ========== CẤU HÌNH ==========
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# Khởi tạo Gemini (nếu có API key)
USE_AI = False
model = None

if GEMINI_API_KEY:
    try:
        # Cấu hình Gemini với API key
        genai.configure(api_key=GEMINI_API_KEY)
        # Sử dụng model đúng tên
        model = genai.GenerativeModel('gemini-1.5-flash')
        USE_AI = True
        print("Đã khởi tạo Google Gemini API")
    except Exception as e:
        print(f"Lỗi khởi tạo Gemini: {e}")
        USE_AI = False
else:
    print("CẢNH BÁO: Thiếu GEMINI_API_KEY, sẽ không xử lý được các dòng >4 cột bằng AI")

# ========== CÁC HÀM TIỆN ÍCH ==========
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
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))

def is_ma_gv_format(value):
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
    """Cấp 1: Tách nếu trước và sau dấu phẩy đều không có khoảng trắng"""
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
    """Cấp 2: Tách nếu sau dấu phẩy không có khoảng trắng"""
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
    """Cấp 3: Tách nếu sau dấu phẩy không có khoảng trắng VÀ chữ hoa"""
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
    """Thử lấy phần tử cuối cùng sau dấu phẩy của cột cuối để tạo cột thứ 4"""
    if len(parts) == 3:
        last_col = parts[-1]
        if ',' in last_col:
            sub_parts = last_col.split(',')
            if len(sub_parts) >= 2:
                last_element = sub_parts[-1].strip()
                parts[-1] = ','.join(sub_parts[:-1]).strip()
                parts.append(last_element)
                return parts
    return parts

def split_with_gemini_for_fallback(text, row_number):
    """
    CHỈ DÙNG AI CHO DỮ LIỆU ĐƯỢC GÁN VÀO CỘT ĐẦU TIÊN SAU NULL
    (tức là những dòng mà rule-based không thể xử lý được)
    """
    if not USE_AI or not model:
        return None, "AI không khả dụng"
    
    prompt = f"""
    Bạn là chuyên gia xử lý dữ liệu khảo sát sinh viên.

    CHUỖI DỮ LIỆU CẦN XỬ LÝ: "{text}"

    Đây là dữ liệu từ câu hỏi khảo sát, hãy phân tích và trả về KẾT QUẢ DƯỚI DẠNG JSON với 4 trường:
    - cau13: đánh giá về NỘI DUNG HỌC PHẦN / CHUẨN ĐẦU RA
    - cau14: đánh giá về HOẠT ĐỘNG DẠY - HỌC  
    - cau15: đánh giá về KIỂM TRA - ĐÁNH GIÁ
    - cau16: GÓP Ý KHÁC (thường là "không", "ko", "ok" nếu có)

    Nếu không có nội dung cho một cột, để chuỗi rỗng "".

    CHỈ TRẢ VỀ JSON, KHÔNG THÊM BẤT KỲ TEXT NÀO KHÁC.
    Ví dụ: {{"cau13": "nội dung", "cau14": "nội dung", "cau15": "nội dung", "cau16": "nội dung"}}
    """
    
    try:
        response = model.generate_content(prompt)
        result_text = response.text.strip()
        
        # Tìm JSON trong response
        json_match = re.search(r'\{[^{}]*\}', result_text)
        if json_match:
            result = json.loads(json_match.group())
            return result, None
        else:
            json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                return result, None
            return None, f"Không parse được JSON: {result_text[:200]}"
            
    except Exception as e:
        return None, str(e)

def split_after_null_by_rules(after_null_list, row_number=None):
    """
    Xử lý các cột sau cột NULL:
    - Dùng 3 cấp rule-based để tách
    - Nếu vẫn không được 4 cột, CHỈ KHI ĐÓ mới dùng AI để xử lý
    """
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    # ===== CẤP 1 =====
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        parts_level1 = try_create_4th_column(parts_level1)
        if len(parts_level1) == 4:
            return parts_level1[:4], None
    
    # ===== CẤP 2 =====
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        parts_level2 = try_create_4th_column(parts_level2)
        if len(parts_level2) == 4:
            return parts_level2[:4], None
    
    # ===== CẤP 3 =====
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        parts_level3 = try_create_4th_column(parts_level3)
        if len(parts_level3) == 4:
            return parts_level3[:4], None
    
    # ===== NẾU VẪN CHƯA ĐƯỢC 4 CỘT, DÙNG AI =====
    if USE_AI:
        print(f"Dòng {row_number}: Rule-based không xử lý được, dùng AI để phân tách...")
        ai_result, error = split_with_gemini_for_fallback(original_text, row_number)
        
        if ai_result:
            result = [
                ai_result.get('cau13', ''),
                ai_result.get('cau14', ''),
                ai_result.get('cau15', ''),
                ai_result.get('cau16', '')
            ]
            print(f"Dòng {row_number}: AI xử lý thành công!")
            return result, None
        else:
            print(f"Dòng {row_number}: AI xử lý thất bại - {error}")
    
    # Vẫn không được -> lưu lỗi để kiểm tra thủ công
    error_info = {
        'row_number': row_number,
        'original_after_null': original_text,
        'level1_result': parts_level1,
        'level2_result': parts_level2,
        'level3_result': parts_level3,
        'final_count': len(parts_level3),
        'message': f'Sau 3 cấp có {len(parts_level3)} cột, AI không xử lý được - cần kiểm tra thủ công'
    }
    return [original_text, '', '', ''], error_info

def process_row(row, row_number=None):
    """
    Xử lý một dòng CSV theo logic
    """
    if not row or len(row) < 2:
        return None, None, []
    
    try:
        # ========== PHẦN 1: XỬ LÝ CÁC CỘT TRƯỚC CỘT NULL ==========
        
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''
        
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break
        
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
        
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break
        
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        
        ho_dem_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 1 < len(row):
            ho_dem_gv = row[ma_gv_index + 1].strip()
        
        ten_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 2 < len(row):
            ten_gv = row[ma_gv_index + 2].strip()
        
        lop_hp = ''
        if ma_gv_index >= 0 and ma_gv_index + 3 < len(row):
            lop_hp = row[ma_gv_index + 3].strip()
        
        cau_hoi = ''
        if ma_gv_index >= 0 and ma_gv_index + 4 < len(row):
            cau_hoi = row[ma_gv_index + 4].strip()
        
        gia_tri = ''
        if ma_gv_index >= 0 and ma_gv_index + 5 < len(row):
            gia_tri = row[ma_gv_index + 5].strip()
        
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
            split_result, error = split_after_null_by_rules(after_null, row_number)
            
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            
            if error:
                split_errors.append(error)
        
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
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("Kết nối blob storage thành công")
    except Exception as e:
        print(f"Lỗi kết nối blob: {e}")
        sys.exit(1)
    
    download_from_blob(blob_service)
    
    print("Đang đọc file CSV...")
    rows, read_errors = read_csv_manual(SURVEY_FILE)
    
    if not rows:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    print(f"Bắt đầu xử lý {len(rows)} dòng...")
    print(f"Sử dụng AI cho dữ liệu fallback: {'CÓ' if USE_AI else 'KHÔNG'}")
    
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
        
        if idx % 500 == 0:
            print(f"Đã xử lý {idx}/{len(rows)} dòng...")
    
    result_df = pd.DataFrame(processed_rows)
    
    print(f"\n{'='*60}")
    print("BÁO CÁO XỬ LÝ")
    print(f"{'='*60}")
    print(f"Tổng số dòng đọc được: {len(rows)}")
    print(f"Số dòng xử lý thành công: {len(processed_rows)}")
    print(f"Số dòng xử lý lỗi: {len(process_errors)}")
    
    if split_errors:
        print(f"\n{'='*60}")
        print(f"CÁC DÒNG KHÔNG THỂ PHÂN TÁCH - CẦN KIỂM TRA THỦ CÔNG ({len(split_errors)} dòng)")
        print(f"{'='*60}")
        for err in split_errors[:10]:
            print(f"\nDòng {err.get('row_number', '?')}:")
            print(f"  Chuỗi sau NULL: {err.get('original_after_null', '')[:200]}")
            print(f"  Số cột sau 3 cấp: {err.get('final_count', 0)}")
        
        # Lưu file lỗi
        split_error_df = pd.DataFrame(split_errors)
        split_error_filename = f"{FILE_NAME}_split_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        split_error_df.to_csv(split_error_filename, index=False, encoding='utf-8-sig')
        print(f"\nĐã lưu {len(split_errors)} dòng lỗi vào file: {split_error_filename}")
    
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
