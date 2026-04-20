

import os
import sys
import re
import io  # ← THÊM
import time  # ← THÊM
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed  # ← THÊM
import pandas as pd
import numpy as np
import pyodbc
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ODBC Connection
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=60;"
)

BATCH_SIZE = 50000

# ========== TRỌNG SỐ CHO TỪNG CỘT (GIỮ NGUYÊN) ==========
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

# Cache regex patterns
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^(\d{2})K(\d{2})$')
_cts_pattern = re.compile(r'^CTS-', re.IGNORECASE)

# ========== HELPER FUNCTIONS ==========
def to_int(val):
    if pd.isna(val):
        return None
    return int(val)

def to_float(val):
    if pd.isna(val):
        return None
    return float(val)

def to_str(val, max_len=None):
    if pd.isna(val):
        return ''
    s = str(val)
    return s[:max_len] if max_len else s

def create_ma_khoa(ten_khoa: str) -> str:
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "TĐHKT"
    words = ten_khoa.split()
    initials = []
    for w in words:
        chars = [c.upper() for c in w if c.isalpha()]
        if chars:
            initials.append(chars[0])
    return ''.join(initials) if initials else "TĐHKT"

def normalize_lop(lop: str):
    if not isinstance(lop, str):
        return "", False
    is_cts = bool(_cts_pattern.match(lop))
    if is_cts:
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip(), is_cts

def derive_ma_hoc_ky() -> str:
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

def is_date_format(value):
    if not isinstance(value, str):
        return False
    return bool(_date_pattern.match(value.strip()))

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    return bool(_ma_gv_pattern.match(value.strip()))

# ========== CÁC HÀM XỬ LÝ CÂU TRẢ LỜI (GIỮ NGUYÊN) ==========
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

def split_after_null_by_scoring(after_null_list, row_number=None):
    if not after_null_list:
        return ['', '', '', ''], None
    original_text = ','.join(after_null_list)
    
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    if len(best_parts) < 4:
        return [original_text, '', '', ''], None
    
    assignments = sequential_scoring_classification(best_parts)
    result = {col: '' for col in COLUMN_ORDER}
    for assign in assignments:
        col = assign['column']
        text = assign['text']
        if result[col]:
            result[col] = f"{result[col]}, {text}"
        else:
            result[col] = text
    return [result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16']], None

# ========== PROCESS ROW (GIỮ NGUYÊN) ==========
def process_row(row, row_number=None):
    if not row or len(row) < 2:
        return None, None, []
    try:
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
        null_index = -1
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
        cau13 = cau14 = cau15 = cau16 = ''
        if null_index >= 0 and null_index + 1 < len(row):
            after_null = row[null_index + 1:]
            split_result, _ = split_after_null_by_scoring(after_null, row_number)
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
        }, None, []
    except Exception as e:
        return None, str(e), []

def read_csv_manual(content):
    rows = []
    for line in content.strip().split('\n'):
        if not line.strip():
            continue
        rows.append([col.strip() for col in line.split(',')])
    return rows

# ========== LOAD DATABASE ==========
def get_existing_ids(cursor, table, id_col):
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table, df, columns, id_col):
    if df.empty:
        return 0
    existing = get_existing_ids(cursor, table, id_col)
    new_data = df[~df[id_col].isin(existing)]
    if new_data.empty:
        return 0
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    data = []
    for _, row in new_data.iterrows():
        tuple_data = []
        for c in columns:
            if c == 'IsCTS':
                tuple_data.append(1 if row[c] else 0)
            elif c == 'NgaySinh':
                val = row[c]
                if pd.isna(val):
                    tuple_data.append(None)
                else:
                    try:
                        dt = pd.to_datetime(val, format='%d/%m/%Y', errors='coerce')
                        tuple_data.append(dt.strftime('%Y-%m-%d') if pd.notna(dt) else None)
                    except:
                        tuple_data.append(None)
            else:
                tuple_data.append(to_str(row[c]))
        data.append(tuple(tuple_data))
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(new_data)

def load_fact_all(cursor, fact_df):
    if fact_df.empty:
        return 0
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    data = list(zip(
        fact_df['SubmissionID'].astype(str).str[:500],
        fact_df['MaCauHoi'].fillna(0).astype(int),
        fact_df['MaSV'].astype(str).str[:50],
        fact_df['MaLopHP'].astype(str).str[:200],
        fact_df['TraLoiSo'].fillna(0).astype(float),
        fact_df['TraLoiText'].fillna('').astype(str).str[:1000],
        fact_df['IsCTS'].fillna(0).astype(int)
    ))
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    total = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        cursor.executemany("""
            INSERT INTO FACT_TRA_LOI_KHAO_SAT 
            (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText, IsCTS)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, batch)
        cursor.connection.commit()
        total += len(batch)
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    print(f"  ✅ FACT done: {total:,} dòng ({time.time()-start:.2f}s)")
    return total

def load_to_database(dims, fact_df):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    try:
        count = load_dimension(cursor, 'DIM_HOC_KY', dims['hoc_ky'], ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        count = load_dimension(cursor, 'DIM_KHOA', dims['khoa'], ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims['chuyen_nganh'], ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims['lop_sv'], ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims['giang_vien'], ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims['hoc_phan'], ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims['sinh_vien'], ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims['lop_hp'], ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        count = load_fact_all(cursor, fact_df)
        print(f"  ✅ FACT: {count:,} dòng")
        print(f"  ✅ Load: {time.time()-start:.2f}s")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        conn.close()

# ========== MAIN ==========
def main():
    import time
    total_start = time.time()
    print("=" * 60)
    print("🚀 SURVEY ETL - GIỮ NGUYÊN CODE GỐC + LOAD DB")
    print("=" * 60)
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print("=" * 60)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure: {e}")
        sys.exit(1)
    
    # ========== EXTRACT ==========
    print("\n📥 1. EXTRACT")
    start = time.time()
    
    # Đọc survey file
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    content = blob_client.download_blob().readall().decode('utf-8-sig')
    
    # Đọc master data
    hp_df = pd.DataFrame()
    cn_df = pd.DataFrame()
    try:
        client = blob_service.get_container_client("tailieu").get_blob_client(f"{SEMESTER}/HP-Khoa.csv")
        if client.exists():
            hp_df = pd.read_csv(io.StringIO(client.download_blob().readall().decode('utf-8')))
            if len(hp_df.columns) >= 4:
                hp_df = hp_df.iloc[:, 1:4]
                hp_df.columns = ['MaHP', 'TenKhoa', 'TenHP']
            hp_df['MaKhoa'] = hp_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> HP-Khoa: {len(hp_df)} dòng")
    except Exception as e:
        print(f"  -> Lỗi HP-Khoa: {e}")
    
    try:
        client = blob_service.get_container_client("tailieu").get_blob_client(f"{SEMESTER}/TenChuyenNganh-Khoa.csv")
        if client.exists():
            cn_df = pd.read_csv(io.StringIO(client.download_blob().readall().decode('utf-8')))
            if len(cn_df.columns) >= 4:
                cn_df = cn_df.iloc[:, 1:4]
                cn_df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
            cn_df['MaKhoa'] = cn_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> TenChuyenNganh-Khoa: {len(cn_df)} dòng")
    except Exception as e:
        print(f"  -> Lỗi TenChuyenNganh-Khoa: {e}")
    
    print(f"  ✅ Extract: {time.time()-start:.2f}s")
    
    # ========== PARSE ==========
    print("\n📝 2. PARSE")
    start = time.time()
    rows = read_csv_manual(content)
    processed_rows = []
    for idx, row in enumerate(rows, 1):
        result, error, _ = process_row(row, idx)
        if result:
            processed_rows.append(result)
    df = pd.DataFrame(processed_rows)
    print(f"  -> Đã parse {len(df):,} dòng hợp lệ")
    print(f"  ✅ Parse: {time.time()-start:.2f}s")
    
    if df.empty:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    # ========== TRANSFORM ==========
    print("\n🔄 3. TRANSFORM")
    start = time.time()
    
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    # Chuẩn hóa Lop
    norm = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm.apply(lambda x: x[0])
    df['IsCTS'] = norm.apply(lambda x: x[1])
    
    # Merge HP-Khoa
    if not hp_df.empty:
        df = df.merge(hp_df[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna('Trường ĐHKT')
        df['MaKhoa'] = df['MaKhoa'].fillna('TĐHKT')
        df.drop(['TenHP_x', 'TenHP_y'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'TĐHKT'
        df['TenKhoa'] = 'Trường ĐHKT'
    
    # Chuyên ngành
    def get_th1(lop):
        if not isinstance(lop, str):
            return None
        m = _lop_pattern.match(lop)
        return f"K{m.group(2)}" if m else None
    
    df['MaCN_TH1'] = df['LopChuanHoa'].apply(get_th1)
    df['MaChuyenNganh'] = df['MaCN_TH1'].fillna(df['MaKhoa'])
    df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    df.drop(columns=['MaCN_TH1'], inplace=True)
    
    # Tính điểm
    for col in COLUMN_ORDER:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_weighted_score(x, col))
    
    # Tạo Dimensions
    dims = {
        'hoc_ky': pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}]),
        'khoa': df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa']),
        'chuyen_nganh': df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh']),
        'lop_sv': df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates().rename(columns={'LopChuanHoa': 'MaLop'}),
        'sinh_vien': df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV']).rename(columns={'LopChuanHoa': 'MaLop'}),
        'giang_vien': df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).query("MaGV != ''"),
        'hoc_phan': df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP']).query("MaHP != ''"),
        'lop_hp': df.assign(MaLopHP=df['LopHP'] + '_' + df['MaHP'])[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates().query("MaLopHP != '_'")
    }
    dims['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
    dims['lop_sv'] = dims['lop_sv'][dims['lop_sv']['MaLop'] != '']
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    dims['sinh_vien']['NgaySinh'] = pd.to_datetime(dims['sinh_vien']['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    
    # Tạo Fact
    df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + FILE_NAME
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    fact_rows = []
    for col, mc in [('Cau13', 13), ('Cau14', 14), ('Cau15', 15), ('Cau16', 16)]:
        temp = df[['SubmissionID', 'MaSV', 'MaLopHP', col, f'{col}_Score', 'IsCTS']].copy()
        temp.columns = ['SubmissionID', 'MaSV', 'MaLopHP', 'TraLoiText', 'TraLoiSo', 'IsCTS']
        temp['MaCauHoi'] = mc
        fact_rows.append(temp)
    fact_df = pd.concat(fact_rows, ignore_index=True)
    fact_df['TraLoiText'] = fact_df['TraLoiText'].fillna('').astype(str).str[:1000]
    
    print(f"  -> Fact: {len(fact_df):,} dòng")
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    # ========== LOAD ==========
    print("\n💾 4. LOAD")
    start = time.time()
    load_to_database(dims, fact_df)
    
    total = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 HOÀN THÀNH! Tổng thời gian: {total:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
