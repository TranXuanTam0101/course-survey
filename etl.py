import os
import sys
import re
import io
import time
import multiprocessing as mp
from datetime import datetime
import pandas as pd
import numpy as np
import pyodbc
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# Số CPU cores sử dụng
NUM_WORKERS = max(1, mp.cpu_count() - 1)
CHUNK_SIZE = 10000

print(f"🚀 Multiprocessing with {NUM_WORKERS} workers, chunk size: {CHUNK_SIZE}")

# ODBC Connection
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=120;"
    f"Command Timeout=300;"
)

BATCH_SIZE = 25000
CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_PATH = "tailieu"

# ========== TRỌNG SỐ ==========
WEIGHTS_CAU13 = {
    'chuẩn đầu ra': 5.0, 'mục tiêu môn học': 4.5, 'đáp ứng chương trình': 4.0,
    'nội dung': 3.0, 'học phần': 3.0, 'chương trình': 2.5, 'môn học': 2.5,
    'trang bị': 2.0, 'cung cấp': 2.0, 'đào tạo': 2.0, 'bám sát': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'đầy đủ': 1.0, 'hợp lý': 1.0, 
    'chất lượng': 1.0, 'bổ ích': 1.0, 'cần thiết': 1.0, 'quan trọng': 1.0, 
    'chi tiết': 1.0, 'cụ thể': 1.0, 'chuẩn': 1.0
}

WEIGHTS_CAU14 = {
    'giảng viên': 5.0, 'thầy giáo': 5.0, 'cô giáo': 5.0, 'tận tâm': 4.5,
    'nhiệt tình': 4.0, 'tận tình': 4.0, 'truyền cảm hứng': 4.0,
    'thầy': 3.0, 'cô': 3.0, 'gv': 3.0, 'dạy': 3.0, 'giảng': 3.0,
    'nhiệt huyết': 3.0, 'tâm huyết': 3.0, 'dễ hiểu': 3.0,
    'bài giảng': 2.0, 'truyền đạt': 2.0, 'giải thích': 2.0, 'hướng dẫn': 2.0,
    'sinh động': 2.0, 'linh hoạt': 2.0, 'đa dạng': 2.0, 'thu hút': 2.0,
    'tương tác': 2.0, 'sôi nổi': 2.0, 'thú vị': 2.0, 'hấp dẫn': 2.0,
    'vui vẻ': 1.0, 'thân thiện': 1.0, 'gần gũi': 1.0, 'thoải mái': 1.0,
    'hay': 1.0, 'tốt': 1.0
}

WEIGHTS_CAU15 = {
    'kiểm tra': 5.0, 'đánh giá': 5.0, 'công bằng': 4.5, 'minh bạch': 4.0,
    'đánh giá đúng': 4.0, 'phản ánh đúng': 4.0,
    'thi': 3.0, 'đề thi': 3.0, 'bài kiểm tra': 3.0, 'cho điểm': 3.0,
    'công khai': 3.0, 'nghiêm túc': 3.0, 'khách quan': 3.0,
    'điểm': 2.0, 'bài tập': 2.0, 'chấm': 2.0, 'giữa kỳ': 2.0, 'cuối kỳ': 2.0,
    'thực lực': 2.0, 'công tâm': 2.0, 'chính xác': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'kỹ càng': 1.0, 'chỉnh chu': 1.0
}

WEIGHTS_CAU16 = {
    'không có góp ý': 5.0, 'không ý kiến': 5.0, 'không góp ý': 4.5,
    'không': 3.0, 'ko': 3.0, 'k': 2.5, 'không có': 3.0,
    'tuyệt vời': 2.0, 'quá ok': 2.0, 'rất ok': 2.0, 'ổn hết': 2.0,
    'ok': 1.0, 'oki': 1.0, 'ổn': 1.0, 'được': 1.0, 'cảm ơn': 1.0, 'tốt hơn': 1.0
}

ALL_WEIGHTS = {
    'Cau13': WEIGHTS_CAU13,
    'Cau14': WEIGHTS_CAU14,
    'Cau15': WEIGHTS_CAU15,
    'Cau16': WEIGHTS_CAU16
}

COLUMN_ORDER = ['Cau13', 'Cau14', 'Cau15', 'Cau16']

# ========== PATTERNS ==========
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')

# ========== CÁC HÀM XỬ LÝ CÂU TRẢ LỜI ==========
def calculate_weighted_score(text, column_name):
    if not text or not isinstance(text, str):
        return 0.0
    text_lower = text.lower()
    total_score = 0.0
    weights = ALL_WEIGHTS.get(column_name, {})
    for keyword, weight in weights.items():
        if keyword in text_lower:
            count = text_lower.count(keyword)
            total_score += weight * (1 + 0.1 * (count - 1))
    length_score = min(len(text) * 0.03, 1.0)
    total_score += length_score
    return total_score

def get_phrase_bonus(segment_parts):
    if len(segment_parts) < 2:
        return 0.0
    merged_text = ' '.join(segment_parts).lower()
    bonus = 0.0
    meaningful_phrases = [
        ('nội dung', 'đầy đủ', 1.0), ('nội dung', 'chi tiết', 1.0),
        ('đầu ra', 'chuẩn', 1.0), ('đánh giá', 'cụ thể', 1.5),
        ('kiểm tra', 'cụ thể', 1.5), ('giảng viên', 'nhiệt tình', 1.0),
        ('bài giảng', 'dễ hiểu', 1.0), ('đánh giá', 'công bằng', 1.0),
        ('kiểm tra', 'công bằng', 1.0)
    ]
    for kw1, kw2, weight in meaningful_phrases:
        if kw1 in merged_text and kw2 in merged_text:
            bonus += weight
    return bonus

def split_by_condition_1(text):
    parts, current = [], []
    i, n = 0, len(text)
    while i < n:
        if text[i] == ',':
            has_space_before = (i > 0 and text[i-1] == ' ')
            has_space_after = (i + 1 < n and text[i+1] == ' ')
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
    parts, current = [], []
    i, n = 0, len(text)
    while i < n:
        if text[i] == ',':
            if i + 1 < n and text[i+1] == ' ':
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
    parts, current = [], []
    i, n = 0, len(text)
    while i < n:
        if text[i] == ',':
            if i + 1 < n:
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

def sequential_scoring_classification(parts):
    if not parts:
        return []
    n = len(parts)
    num_columns = 4
    dp = [[-1e9] * num_columns for _ in range(n + 1)]
    choice = [[None] * num_columns for _ in range(n + 1)]
    dp[0][0] = 0
    for i in range(n):
        for j in range(num_columns):
            if dp[i][j] < -1e8:
                continue
            remaining_columns = num_columns - j
            min_remaining_parts = remaining_columns - 1
            max_k = n - i - min_remaining_parts
            for k in range(1, max_k + 1):
                segment_parts = parts[i:i+k]
                merged_text = ', '.join(segment_parts)
                base_score = calculate_weighted_score(merged_text, COLUMN_ORDER[j])
                phrase_bonus = get_phrase_bonus(segment_parts)
                score = base_score + phrase_bonus
                if j + 1 < num_columns:
                    new_score = dp[i][j] + score
                    if new_score > dp[i + k][j + 1]:
                        dp[i + k][j + 1] = new_score
                        choice[i + k][j + 1] = (i, j, k, merged_text)
                else:
                    if i + k == n:
                        new_score = dp[i][j] + score
                        if new_score > dp[i + k][j]:
                            dp[i + k][j] = new_score
                            choice[i + k][j] = (i, j, k, merged_text)
    best_score = dp[n][num_columns - 1]
    if best_score < -1e8:
        return fallback_even_split(parts)
    assignments = []
    i, j = n, num_columns - 1
    while i > 0 and j >= 0:
        if choice[i][j] is None:
            break
        prev_i, prev_j, k, text = choice[i][j]
        assignments.insert(0, {'column': COLUMN_ORDER[prev_j], 'text': text, 'num_parts': k})
        i, j = prev_i, prev_j
    return assignments

def fallback_even_split(parts):
    n = len(parts)
    num_columns = 4
    sizes = [1] * num_columns
    remaining = n - num_columns
    for i in range(remaining):
        sizes[i % num_columns] += 1
    assignments = []
    start = 0
    for col_idx, size in enumerate(sizes):
        end = start + size
        merged_text = ', '.join(parts[start:end])
        assignments.append({'column': COLUMN_ORDER[col_idx], 'text': merged_text, 'num_parts': size})
        start = end
    return assignments

def split_after_null_by_scoring(after_null_list):
    if not after_null_list:
        return ['', '', '', '']
    original_text = ','.join(after_null_list)
    
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4]
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success and len(new_parts) == 4:
            return new_parts[:4]
    
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4]
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success and len(new_parts) == 4:
            return new_parts[:4]
    
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4]
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success and len(new_parts) == 4:
            return new_parts[:4]
    
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    if len(best_parts) < 4:
        return [original_text, '', '', '']
    
    assignments = sequential_scoring_classification(best_parts)
    result = {col: '' for col in COLUMN_ORDER}
    for assign in assignments:
        col = assign['column']
        text = assign['text']
        if result[col]:
            result[col] = f"{result[col]}, {text}"
        else:
            result[col] = text
    return [result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16']]

def is_date_format(value):
    if not isinstance(value, str):
        return False
    return bool(_date_pattern.match(value.strip()))

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    return bool(_ma_gv_pattern.match(value.strip()))

# ========== PROCESS ROW ==========
def parse_single_line(line: str) -> dict:
    if not line or not line.strip():
        return None
    
    row = [x.strip() for x in line.split(',')]
    
    try:
        lop = row[0] if len(row) > 0 else ''
        ma_sv = row[1] if len(row) > 1 else ''
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i]
                ngay_sinh_index = i
                break
        if ngay_sinh_index == -1:
            return None
        
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p for p in ho_dem_ten_parts if p])
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        
        ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < len(row) else ''
        
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i]
                ma_gv_index = i
                break
        if ma_gv_index == -1:
            ma_gv_index = len(row) - 4 if len(row) >= 4 else ngay_sinh_index + 2
        
        ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]) if ma_gv_index > ngay_sinh_index + 2 else ''
        ho_dem_gv = row[ma_gv_index + 1] if ma_gv_index + 1 < len(row) else ''
        ten_gv = row[ma_gv_index + 2] if ma_gv_index + 2 < len(row) else ''
        lop_hp = row[ma_gv_index + 3] if ma_gv_index + 3 < len(row) else ''
        
        null_index = -1
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1]
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
        
        cau13 = cau14 = cau15 = cau16 = ''
        if null_index >= 0 and null_index + 1 < len(row):
            after_null = row[null_index + 1:]
            if after_null:
                split_result = split_after_null_by_scoring(after_null)
                if len(split_result) >= 4:
                    cau13 = split_result[0]
                    cau14 = split_result[1]
                    cau15 = split_result[2]
                    cau16 = split_result[3]
        
        return {
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv, 'LopHP': lop_hp,
            'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
        }
    except:
        return None

def parse_lines_batch(lines_batch):
    results = []
    for line in lines_batch:
        result = parse_single_line(line)
        if result:
            results.append(result)
    return results

# ========== HELPER FUNCTIONS ==========
def to_int(val):
    if pd.isna(val):
        return None
    return int(val)

def to_float(val):
    if pd.isna(val):
        return None
    return float(val)

def create_ma_khoa(ten_khoa: str) -> str:
    """Lấy chữ cái đầu tiên của TẤT CẢ các từ"""
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "TĐHKT"
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = []
    for w in words:
        if w:
            first_char = w[0].upper() if w[0].isalpha() else ''
            if first_char:
                initials.append(first_char)
    return ''.join(initials) if initials else "TĐHKT"

def normalize_lop(lop: str) -> str:
    if not isinstance(lop, str):
        return ""
    if lop.upper().startswith('CTS-'):
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip()

def derive_ma_hoc_ky() -> str:
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

def determine_ma_chuyen_nganh(lop: str) -> tuple:
    """
    Returns: (MaChuyenNganh, TenKhoa_mac_dinh, MaKhoa_mac_dinh, IsCTS)
    """
    lop_upper = lop.upper()
    lop_normalized = normalize_lop(lop)
    is_cts = lop_upper.startswith('CTS-') or lop_upper.startswith('CTS')
    
    # TH1: Lop khớp pattern ^\d{2}K\d{2}$
    if _lop_pattern.match(lop_normalized):
        nn = lop_normalized[3:5]
        return f"K{nn}", None, None, is_cts
    
    # TH2a: CTS
    if is_cts:
        return "CTS", "Trường ĐHKT", "TĐHKT", True
    
    # TH2b: QT
    if 'QT' in lop_upper:
        return "QT", "Phòng Đào Tạo", "PĐT", is_cts
    
    # Mặc định
    return "TĐHKT", "Trường ĐHKT", "TĐHKT", is_cts

# ================= EXTRACT =================
def download_blob_to_string(blob_service: BlobServiceClient, blob_path: str) -> str:
    try:
        client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(blob_path)
        if not client.exists():
            return ""
        return client.download_blob().readall().decode('utf-8-sig')
    except Exception as e:
        print(f"  -> Lỗi download {blob_path}: {e}")
        return ""

def load_hp_master(blob_service: BlobServiceClient) -> pd.DataFrame:
    path = f"{TAILIEU_PATH}/HP-Khoa.csv"
    print(f"  -> Đọc HP-Khoa: {CONTAINER_NAME}/{path}")
    content = download_blob_to_string(blob_service, path)
    if not content:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 4:
        df = df.iloc[:, 1:4]
        df.columns = ['MaHP', 'TenKhoa', 'TenHP']
    df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
    print(f"  -> HP-Khoa: {len(df)} dòng")
    return df

def load_cn_master(blob_service: BlobServiceClient) -> pd.DataFrame:
    path = f"{TAILIEU_PATH}/TenChuyenNganh-Khoa.csv"
    print(f"  -> Đọc TenChuyenNganh-Khoa: {CONTAINER_NAME}/{path}")
    content = download_blob_to_string(blob_service, path)
    if not content:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 4:
        df = df.iloc[:, 1:4]
        df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
    df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
    print(f"  -> TenChuyenNganh-Khoa: {len(df)} dòng")
    return df

# ================= PARSE MULTIPROCESSING =================
def parse_survey_parallel(content: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    total_lines = len(lines)
    print(f"  -> Tổng số dòng: {total_lines:,}")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    print(f"  -> Chia thành {len(batches)} batches")
    
    all_results = []
    with mp.Pool(processes=NUM_WORKERS) as pool:
        for i, batch_results in enumerate(pool.imap_unordered(parse_lines_batch, batches)):
            all_results.extend(batch_results)
            if (i + 1) % 10 == 0:
                print(f"    -> Đã xong {i+1}/{len(batches)} batches, {len(all_results):,} dòng hợp lệ")
    
    df = pd.DataFrame(all_results)
    print(f"  -> Đã parse {len(df):,} dòng hợp lệ ({time.time()-start:.2f}s)")
    return df

# ================= TRANSFORM =================
def transform_data(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> dict:
    print("  -> Transform...")
    start = time.time()
    
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    chuyen_nganh_info = df['Lop'].apply(determine_ma_chuyen_nganh)
    df['MaChuyenNganh_TuLop'] = chuyen_nganh_info.apply(lambda x: x[0])
    df['TenKhoa_MacDinh'] = chuyen_nganh_info.apply(lambda x: x[1])
    df['MaKhoa_MacDinh'] = chuyen_nganh_info.apply(lambda x: x[2])
    df['IsCTS'] = chuyen_nganh_info.apply(lambda x: x[3])
    
    # ========== FIX 2: MaLop = Lop (giữ nguyên) ==========
    df['MaLop'] = df['Lop']  # ← SỬA Ở ĐÂY
    
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna(df['TenKhoa_MacDinh'])
        df['TenKhoa'] = df['TenKhoa'].fillna('Trường ĐHKT')
        df['MaKhoa'] = df['MaKhoa'].fillna(df['TenKhoa'].apply(create_ma_khoa))
        df['MaKhoa'] = df['MaKhoa'].fillna(df['MaKhoa_MacDinh'])
        df['MaKhoa'] = df['MaKhoa'].fillna('TĐHKT')
        df.drop(['TenHP_x', 'TenHP_y', 'TenKhoa_MacDinh', 'MaKhoa_MacDinh'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = df['MaKhoa_MacDinh'].fillna('TĐHKT')
        df['TenKhoa'] = df['TenKhoa_MacDinh'].fillna('Trường ĐHKT')
        df.drop(['TenKhoa_MacDinh', 'MaKhoa_MacDinh'], axis=1, inplace=True, errors='ignore')
    
    df['MaChuyenNganh'] = df['MaChuyenNganh_TuLop'].fillna(df['MaKhoa'])
    df.drop(['MaChuyenNganh_TuLop'], axis=1, inplace=True, errors='ignore')
    
    if not cn_master.empty:
        cn_mapping = cn_master[['MaChuyenNganh', 'TenChuyenNganh']].drop_duplicates(subset=['MaChuyenNganh'])
        cn_mapping = cn_mapping.set_index('MaChuyenNganh')['TenChuyenNganh'].to_dict()
        df['TenChuyenNganh'] = df['MaChuyenNganh'].map(cn_mapping)
    
    def get_default_ten_cn(ma_cn):
        if ma_cn == 'CTS':
            return 'Chuyên ngành CTS'
        elif ma_cn == 'QT':
            return 'Chuyên ngành QT'
        else:
            return 'Chuyên ngành ' + str(ma_cn)
    
    df['TenChuyenNganh'] = df['TenChuyenNganh'].fillna(df['MaChuyenNganh'].apply(get_default_ten_cn))
    
    print("  -> Tính điểm...")
    for col in COLUMN_ORDER:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_weighted_score(x, col))
    
    # ========== FIX 1: MaLopHP = LopHP (không ghép) ==========
    df['MaLopHP'] = df['LopHP']  # ← SỬA Ở ĐÂY
    
    print("  -> Tạo Dimensions...")
    
    dim_hoc_ky = pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}])
    
    dim_khoa = df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa'])
    dim_khoa = dim_khoa[dim_khoa['MaKhoa'] != '']
    dim_khoa['TenKhoa'] = dim_khoa['TenKhoa'].fillna('Trường ĐHKT')
    default_khoas = pd.DataFrame([
        {'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'},
        {'MaKhoa': 'PĐT', 'TenKhoa': 'Phòng Đào Tạo'}
    ])
    dim_khoa = pd.concat([dim_khoa, default_khoas]).drop_duplicates(subset=['MaKhoa']).reset_index(drop=True)
    
    dim_chuyen_nganh = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh'])
    dim_chuyen_nganh['MaCTDT'] = 'CTDT_CHINHQUY'
    
    dim_hoc_phan = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP'])
    dim_hoc_phan = dim_hoc_phan[dim_hoc_phan['MaHP'] != '']
    
    dim_giang_vien = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV'])
    dim_giang_vien = dim_giang_vien[dim_giang_vien['MaGV'] != '']
    
    # ========== DIM_LOP_HOC_PHAN với MaLopHP = LopHP ==========
    dim_lop_hp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates(subset=['MaLopHP'])
    dim_lop_hp = dim_lop_hp[dim_lop_hp['MaLopHP'] != '']
    dim_lop_hp['MaHocKy'] = ma_hoc_ky
    
    # ========== DIM_LOP_SINH_VIEN với MaLop = Lop ==========
    dim_lop_sv = df[['MaLop', 'Lop', 'MaChuyenNganh']].drop_duplicates(subset=['MaLop'])
    dim_lop_sv = dim_lop_sv[dim_lop_sv['MaLop'] != '']
    
    dim_sinh_vien = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']].drop_duplicates(subset=['MaSV'])
    
    print("  -> Tạo FACT...")
    df['SubmissionID'] = df['MaSV'] + df['LopHP'] + df['MaGV'] + '_' + FILE_NAME
    
    fact_rows = []
    for col, mc in [('Cau13', 13), ('Cau14', 14), ('Cau15', 15), ('Cau16', 16)]:
        temp = df[['SubmissionID', 'MaSV', 'MaLopHP', col, f'{col}_Score']].copy()
        temp.columns = ['SubmissionID', 'MaSV', 'MaLopHP', 'TraLoiText', 'TraLoiSo']
        temp['MaCauHoi'] = mc
        fact_rows.append(temp)
    
    fact_df = pd.concat(fact_rows, ignore_index=True)
    fact_df['TraLoiText'] = fact_df['TraLoiText'].fillna('').astype(str)
    
    print(f"  -> Fact: {len(fact_df):,} dòng")
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    return {
        'DIM_HOC_KY': dim_hoc_ky,
        'DIM_KHOA': dim_khoa,
        'DIM_CHUYEN_NGANH': dim_chuyen_nganh,
        'DIM_HOC_PHAN': dim_hoc_phan,
        'DIM_GIANG_VIEN': dim_giang_vien,
        'DIM_LOP_HOC_PHAN': dim_lop_hp,
        'DIM_LOP_SINH_VIEN': dim_lop_sv,
        'DIM_SINH_VIEN': dim_sinh_vien,
        'FACT': fact_df
    }

# ================= LOAD =================
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table: str, df: pd.DataFrame, columns: list, id_col: str) -> int:
    if df.empty:
        return 0
    df = df.fillna('')
    existing = get_existing_ids(cursor, table, id_col)
    new_data = df[~df[id_col].isin(existing)]
    if new_data.empty:
        return 0
    print(f"    -> Inserting {len(new_data)} new records into {table}...")
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    data = []
    for _, row in new_data.iterrows():
        tuple_data = []
        for c in columns:
            if c == 'NgaySinh':
                val = row[c]
                if pd.isna(val) or val == '':
                    tuple_data.append(None)
                else:
                    try:
                        dt = pd.to_datetime(val, format='%d/%m/%Y', errors='coerce')
                        tuple_data.append(dt.strftime('%Y-%m-%d') if pd.notna(dt) else None)
                    except:
                        tuple_data.append(None)
            else:
                val = row[c]
                tuple_data.append(str(val)[:500] if val else '')
        data.append(tuple(tuple_data))
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(new_data)

def load_fact(cursor, fact_df: pd.DataFrame) -> int:
    if fact_df.empty:
        return 0
    
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    # Debug: kiểm tra FK
    cursor.execute("SELECT COUNT(*) FROM DIM_SINH_VIEN")
    sv_count = cursor.fetchone()[0]
    print(f"  -> DIM_SINH_VIEN: {sv_count} records")
    
    cursor.execute("SELECT COUNT(*) FROM DIM_LOP_HOC_PHAN")
    lhp_count = cursor.fetchone()[0]
    print(f"  -> DIM_LOP_HOC_PHAN: {lhp_count} records")
    
    # Lấy danh sách FK hợp lệ
    valid_sv = set()
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    for row in cursor.fetchall():
        valid_sv.add(row[0])
    
    valid_lhp = set()
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    for row in cursor.fetchall():
        valid_lhp.add(row[0])
    
    # Lọc dòng hợp lệ
    fact_df_valid = fact_df[
        fact_df['MaSV'].isin(valid_sv) & 
        fact_df['MaLopHP'].isin(valid_lhp)
    ]
    
    skipped = len(fact_df) - len(fact_df_valid)
    if skipped > 0:
        print(f"  ⚠️ Bỏ {skipped:,} dòng do FK không hợp lệ")
        # In mẫu các dòng bị lỗi
        fact_df_invalid = fact_df[
            ~(fact_df['MaSV'].isin(valid_sv) & fact_df['MaLopHP'].isin(valid_lhp))
        ]
        if len(fact_df_invalid) > 0:
            sample = fact_df_invalid[['MaSV', 'MaLopHP']].head(5)
            print(f"  -> Mẫu dòng lỗi: {sample.to_dict()}")
    
    if fact_df_valid.empty:
        print("  ❌ KHÔNG CÓ DÒNG NÀO HỢP LỆ!")
        return 0
    
    fact_df = fact_df_valid
    fact_df = fact_df.fillna('')
    
    data = list(zip(
        fact_df['SubmissionID'].astype(str).str[:100],
        fact_df['MaCauHoi'].astype(int),
        fact_df['MaSV'].astype(str).str[:20],
        fact_df['MaLopHP'].astype(str).str[:50],
        fact_df['TraLoiSo'].fillna(0).astype(float),
        fact_df['TraLoiText'].astype(str)
    ))
    
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    total = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        cursor.executemany("""
            INSERT INTO FACT_TRA_LOI_KHAO_SAT 
            (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText)
            VALUES (?, ?, ?, ?, ?, ?)
        """, batch)
        cursor.connection.commit()
        total += len(batch)
        if (i // BATCH_SIZE + 1) % 10 == 0:
            print(f"    -> Đã insert {total:,}/{len(data):,} dòng")
    
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    print(f"  ✅ FACT done: {total:,} dòng ({time.time()-start:.2f}s)")
    return total

def load_to_database(dims: dict):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        count = load_dimension(cursor, 'DIM_HOC_KY', dims['DIM_HOC_KY'],
                               ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        
        count = load_dimension(cursor, 'DIM_KHOA', dims['DIM_KHOA'],
                               ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        print("  ✅ DIM_CTDT: ensured")
        
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims['DIM_CHUYEN_NGANH'],
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims['DIM_HOC_PHAN'],
                               ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims['DIM_GIANG_VIEN'],
                               ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims['DIM_LOP_HOC_PHAN'],
                               ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims['DIM_LOP_SINH_VIEN'],
                               ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims['DIM_SINH_VIEN'],
                               ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        
        count = load_fact(cursor, dims['FACT'])
        print(f"  ✅ FACT: {count:,} dòng")
        
        print(f"  ✅ Load: {time.time()-start:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 SURVEY ETL - MULTIPROCESSING + FIXED")
    print("=" * 60)
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print(f"Workers: {NUM_WORKERS}, Chunk: {CHUNK_SIZE}")
    print("=" * 60)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure: {e}")
        sys.exit(1)
    
    print("\n📥 1. EXTRACT")
    start = time.time()
    hp_master = load_hp_master(blob_service)
    cn_master = load_cn_master(blob_service)
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    print(f"  -> Đọc survey: {CONTAINER_NAME}/{survey_path}")
    survey_content = download_blob_to_string(blob_service, survey_path)
    print(f"  ✅ Extract: {time.time()-start:.2f}s")
    
    if not survey_content:
        print("❌ Không thể đọc file survey!")
        sys.exit(1)
    
    print("\n📝 2. PARSE (MULTIPROCESSING)")
    start = time.time()
    df = parse_survey_parallel(survey_content)
    print(f"  ✅ Parse: {time.time()-start:.2f}s")
    
    if df.empty:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    print("\n🔄 3. TRANSFORM")
    start = time.time()
    dims = transform_data(df, hp_master, cn_master)
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    print("\n💾 4. LOAD")
    start = time.time()
    load_to_database(dims)
    
    total = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 HOÀN THÀNH! Tổng thời gian: {total:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
