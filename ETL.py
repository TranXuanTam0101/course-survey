#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - OPTIMIZED FOR SPEED
- Parse CSV với NULL làm mốc phân tách
- NLP tối giản
- Multiprocessing tối ưu
"""

import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import multiprocessing as mp
from multiprocessing import Pool

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=300;"
    f"Command Timeout=600;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_CONTAINER = "tailieu"

# Tối ưu workers
NUM_WORKERS = mp.cpu_count()
CHUNK_SIZE = 50000  # Tăng chunk size
BATCH_SIZE = 100000  # Tăng batch size DB

print("=" * 70)
print("🚀 SURVEY ETL - OPTIMIZED")
print(f"👷 Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
print("=" * 70)

# ================= PATTERNS (Pre-compiled) =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')

# ================= NLP - TỐI GIẢN =================
# Chỉ giữ keywords quan trọng nhất
TAG_KEYWORDS_FAST = {
    'Tag_HocPhan': ['nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức', 'chuẩn đầu ra', 'tài liệu', 'giáo trình', 'thực hành', 'lý thuyết', 'phù hợp', 'bổ ích', 'cần thiết', 'cập nhật', 'thực tế'],
    'Tag_DayHoc': ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng', 'truyền đạt', 'hướng dẫn', 'nhiệt tình', 'tận tâm', 'dễ hiểu', 'sinh động', 'thú vị', 'hấp dẫn', 'chuyên nghiệp'],
    'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'thi', 'đề thi', 'chấm điểm', 'công bằng', 'minh bạch', 'khách quan', 'nghiêm túc', 'chính xác'],
    'Tag_Khac': ['cơ sở vật chất', 'phòng học', 'máy chiếu', 'wifi', 'hỗ trợ', 'góp ý', 'đề xuất', 'cải thiện', 'không']
}

SENTIMENT_FAST = {
    'POSITIVE': ['tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả', 'chất lượng', 'tuyệt vời', 'xuất sắc', 'nhiệt tình', 'dễ hiểu', 'công bằng'],
    'NEGATIVE': ['tệ', 'kém', 'chán', 'dở', 'không tốt', 'khó hiểu', 'nhàm chán', 'thiếu', 'hạn chế', 'thất vọng', 'cần cải thiện'],
    'NEUTRAL': ['không có góp ý', 'không ý kiến', 'không có', 'bình thường']
}

# ================= MASTER DATA (Global) =================
_master_cn_dict = {}  # Dict lookup nhanh hơn DataFrame
_master_hp_dict = {}
_master_cn_fallback = {'MaChuyenNganh': '', 'TenChuyenNganh': '', 'MaNganh': '', 'TenNganh': '', 'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'}
_master_hp_fallback = {'TenHP': '', 'MaKhoa_HP': 'TĐHKT', 'TenKhoa_HP': 'Trường ĐHKT'}

# ================= UTILITY FUNCTIONS =================
def is_date_format(value):
    return isinstance(value, str) and bool(_date_pattern.match(value))

def is_ma_gv_format(value):
    if not isinstance(value, str): return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (v.startswith("TG") and len(v) == 7) or v == "gvDacThu_TKTH"

def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    nam_hoc = f"{nam_bat_dau}-{nam_ket_thuc}"
    year_part = f"{nam_bat_dau % 100}{nam_ket_thuc % 100}"
    ma_hoc_ky = f"HK{hoc_ky}_{year_part}"
    return ma_hoc_ky, nam_hoc, hoc_ky

def normalize_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def create_ma_khoa(ten_khoa):
    if not ten_khoa or not isinstance(ten_khoa, str): return "UNKNOWN"
    special_map = {'trường đhsp': 'TĐHSP', 'trường đhkt': 'TĐHKT', 'trường đhnn': 'TĐHNN', 'phòng đào tạo': 'PĐT'}
    ten_lower = ten_khoa.lower().strip()
    for key, value in special_map.items():
        if key in ten_lower: return value
    words = re.split(r'[\s\-]+', ten_khoa)
    return ''.join([w[0].upper() for w in words if w and w[0].isalpha()]) or "UNKNOWN"

# ================= BLOB =================
def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except:
        return ""

# ================= LOAD MASTER (Tối ưu: Dict thay vì DataFrame) =================
def load_master_data(blob_service):
    global _master_cn_dict, _master_hp_dict
    
    print("\n📚 LOAD MASTER DATA (Dict lookup)")
    
    # Load CN
    cn_content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if cn_content:
        df = pd.read_csv(io.StringIO(cn_content))
        df.columns = [c.strip() for c in df.columns]
        
        # Tìm cột
        col_ten_khoa = next((c for c in df.columns if 'khoa' in c.lower() and 'mã' not in c.lower()), None)
        col_ten_nganh = next((c for c in df.columns if 'ngành' in c.lower() and 'chuyên' not in c.lower() and 'khối' not in c.lower()), None)
        col_ten_cn = next((c for c in df.columns if 'chuyên' in c.lower()), None)
        col_ma_cn = next((c for c in df.columns if 'mã cn' in c.lower() or ('mã' in c.lower() and 'cn' in c.lower())), None)
        
        if col_ma_cn and col_ten_cn:
            df['MaKhoa'] = df[col_ten_khoa].apply(create_ma_khoa) if col_ten_khoa else 'TĐHKT'
            df['MaNganh'] = df[col_ten_nganh].apply(lambda x: ''.join([w[0].upper() for w in re.split(r'[\s\-]+', str(x)) if w and w[0].isalpha()])) if col_ten_nganh else ''
            
            # Tạo dict lookup
            for _, row in df.iterrows():
                key = str(row[col_ma_cn]).strip()
                _master_cn_dict[key] = {
                    'MaChuyenNganh': key,
                    'TenChuyenNganh': str(row.get(col_ten_cn, f'CN {key}')).strip(),
                    'MaNganh': str(row.get('MaNganh', '')).strip(),
                    'TenNganh': str(row.get(col_ten_nganh, '')).strip(),
                    'MaKhoa': str(row.get('MaKhoa', 'TĐHKT')).strip(),
                    'TenKhoa': str(row.get(col_ten_khoa, 'Trường ĐHKT')).strip()
                }
        print(f"     -> Loaded {len(_master_cn_dict)} CN records")
    
    # Load HP
    hp_content = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    if hp_content:
        df = pd.read_csv(io.StringIO(hp_content))
        df.columns = [c.strip() for c in df.columns]
        
        col_ma_hp = next((c for c in df.columns if 'mã' in c.lower() and 'hp' in c.lower() or 'mã học phần' in c.lower()), None)
        col_ten_hp = next((c for c in df.columns if 'tên' in c.lower() and 'hp' in c.lower() or 'tên học phần' in c.lower()), None)
        col_khoa = next((c for c in df.columns if 'khoa' in c.lower()), None)
        
        if col_ma_hp:
            # Đặc biệt: Ngữ Văn, Toán -> ĐHSP
            if col_khoa:
                mask = df[col_khoa].str.contains('Ngữ Văn|Toán', case=False, na=False)
                df.loc[mask, col_khoa] = 'Trường ĐHSP'
                df['MaKhoa'] = df[col_khoa].apply(create_ma_khoa)
            
            for _, row in df.iterrows():
                key = str(row[col_ma_hp]).strip()
                _master_hp_dict[key] = {
                    'TenHP': str(row.get(col_ten_hp, '')).strip() if col_ten_hp else '',
                    'TenKhoa_HP': str(row.get(col_khoa, 'Trường ĐHKT')).strip() if col_khoa else 'Trường ĐHKT',
                    'MaKhoa_HP': str(row.get('MaKhoa', 'TĐHKT')).strip()
                }
        print(f"     -> Loaded {len(_master_hp_dict)} HP records")

# ================= LOOKUP (Dict O(1)) =================
def lookup_chuyen_nganh(lop):
    lop_norm = normalize_lop(lop)
    
    if _lop_pattern.match(lop_norm):
        ma_cn = f"K{lop_norm[3:5]}"
        return _master_cn_dict.get(ma_cn, {
            'MaChuyenNganh': ma_cn, 'TenChuyenNganh': f'CN {ma_cn}',
            'MaNganh': f'N{ma_cn}', 'TenNganh': f'Ngành {ma_cn}',
            'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'
        })
    else:
        # Fallback
        for key, val in _master_cn_dict.items():
            if lop_norm.lower() in val.get('TenKhoa', '').lower() or lop_norm.lower() in val.get('TenChuyenNganh', '').lower():
                return val
        return {
            'MaChuyenNganh': lop_norm or lop, 'TenChuyenNganh': lop,
            'MaNganh': lop_norm or lop, 'TenNganh': lop,
            'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'
        }

def lookup_hoc_phan(ma_hp):
    if not ma_hp: return _master_hp_fallback
    return _master_hp_dict.get(str(ma_hp).strip(), _master_hp_fallback)

# ================= NLP FAST =================
def process_essay_nlp_fast(text):
    """NLP siêu nhanh"""
    if not text or not isinstance(text, str) or len(text.strip()) < 5:
        return {'Tag_HocPhan': 0, 'Tag_DayHoc': 0, 'Tag_KiemTra': 0, 'Tag_Khac': 0, 'Sentiment': 'NEUTRAL', 'Is_Valid': 0}
    
    text_lower = text.lower()
    
    # Tags
    tags = {}
    for tag_name, keywords in TAG_KEYWORDS_FAST.items():
        score = sum(text_lower.count(kw) for kw in keywords)
        tags[tag_name] = 1 if score >= 3 else 0
    
    # Sentiment
    pos = sum(text_lower.count(kw) for kw in SENTIMENT_FAST['POSITIVE'])
    neg = sum(text_lower.count(kw) for kw in SENTIMENT_FAST['NEGATIVE'])
    neu = sum(text_lower.count(kw) for kw in SENTIMENT_FAST['NEUTRAL'])
    
    if 'không' in text_lower:
        pos = max(0, pos - 1)
        neg += 1
    
    if pos > neg and pos > neu: sentiment = 'POSITIVE'
    elif neg > pos and neg > neu: sentiment = 'NEGATIVE'
    else: sentiment = 'NEUTRAL'
    
    is_valid = 1 if len(text_lower) > 10 else 0
    
    return {**tags, 'Sentiment': sentiment, 'Is_Valid': is_valid}

# ================= PARSE BATCH (Tối ưu) =================
def parse_lines_batch(lines_batch):
    """Parse batch - tối ưu không dùng try/except nhiều"""
    results = []
    
    for line in lines_batch:
        if not line: continue
        
        # Tìm NULL
        null_idx = line.upper().find('NULL')
        if null_idx >= 0:
            left = line[:null_idx].rstrip(', \t')
            right = line[null_idx + 4:].lstrip(', \t')
        else:
            left = line
            right = ''
        
        # Split left
        row = [x.strip() for x in left.split(',')]
        row_len = len(row)
        
        if row_len < 10: continue
        
        # Tìm ngày sinh
        ngay_sinh_idx = -1
        for i in range(2, min(12, row_len)):
            if _date_pattern.match(row[i]):
                ngay_sinh_idx = i
                break
        if ngay_sinh_idx == -1: continue
        
        # Tìm MaGV
        ma_gv_idx = -1
        for i in range(ngay_sinh_idx + 1, min(ngay_sinh_idx + 25, row_len)):
            if _ma_gv_pattern.match(row[i]):
                ma_gv_idx = i
                break
        if ma_gv_idx == -1:
            ma_gv_idx = min(row_len - 1, ngay_sinh_idx + 8)
        
        # Extract nhanh
        lop = row[0]
        ma_sv = row[1]
        ngay_sinh = row[ngay_sinh_idx]
        
        name_parts = row[2:ngay_sinh_idx]
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        ma_hp = row[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < row_len else ''
        ten_hp_raw = ' '.join(row[ngay_sinh_idx + 2:ma_gv_idx])
        
        ma_gv = row[ma_gv_idx] if ma_gv_idx < row_len else ''
        ho_dem_gv = row[ma_gv_idx + 1] if ma_gv_idx + 1 < row_len else ''
        ten_gv = row[ma_gv_idx + 2] if ma_gv_idx + 2 < row_len else ''
        lop_hp = row[ma_gv_idx + 3] if ma_gv_idx + 3 < row_len else ''
        cau_hoi = row[ma_gv_idx + 4] if ma_gv_idx + 4 < row_len else ''
        gia_tri = row[ma_gv_idx + 5] if ma_gv_idx + 5 < row_len else ''
        
        # Essay
        essay_text = right.replace(' , ', ', ').strip()
        
        # NLP
        nlp = process_essay_nlp_fast(essay_text) if essay_text else {'Tag_HocPhan': 0, 'Tag_DayHoc': 0, 'Tag_KiemTra': 0, 'Tag_Khac': 0, 'Sentiment': 'NEUTRAL', 'Is_Valid': 0}
        
        # Lookup
        cn = lookup_chuyen_nganh(lop)
        hp = lookup_hoc_phan(ma_hp)
        ten_hp = hp['TenHP'] or ten_hp_raw
        
        ma_lop = normalize_lop(lop)
        ma_lop_hp = lop_hp or f"{ma_hp}_{ma_gv}"
        submission_id = f"{ma_sv}_{ma_lop_hp}_{ma_gv}_{FILE_NAME}"
        
        # Kết quả (dùng list thay vì dict để nhanh hơn)
        results.append([
            submission_id, ma_sv, ho_dem, ten, ngay_sinh,
            ma_lop, lop, cn['MaChuyenNganh'], cn['TenChuyenNganh'],
            cn['MaNganh'], cn['TenNganh'], cn['MaKhoa'], cn['TenKhoa'],
            ma_hp, ten_hp, hp['MaKhoa_HP'], hp['TenKhoa_HP'],
            ma_gv, ho_dem_gv, ten_gv, ma_lop_hp, lop_hp,
            cau_hoi, gia_tri, essay_text,
            nlp['Tag_HocPhan'], nlp['Tag_DayHoc'], nlp['Tag_KiemTra'], nlp['Tag_Khac'],
            nlp['Sentiment'], nlp['Is_Valid']
        ])
    
    return results

# Column names
COLUMNS = [
    'SubmissionID', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
    'MaLop', 'Lop', 'MaChuyenNganh', 'TenChuyenNganh',
    'MaNganh', 'TenNganh', 'MaKhoa_CN', 'TenKhoa_CN',
    'MaHP', 'TenHP', 'MaKhoa_HP', 'TenKhoa_HP',
    'MaGV', 'HoDemGV', 'TenGV', 'MaLopHP', 'LopHP',
    'CauHoi', 'GiaTri', 'EssayText',
    'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac',
    'Sentiment', 'Is_Valid'
]

def parse_survey_parallel(content):
    """Parse với multiprocessing tối ưu"""
    print(f"  -> Parsing {NUM_WORKERS} workers, chunk {CHUNK_SIZE:,}...")
    start = time.time()
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"  -> {len(lines):,} lines")
    
    # Chia batches
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, batch_results in enumerate(pool.imap_unordered(parse_lines_batch, batches)):
            all_results.extend(batch_results)
            print(f"    -> Batch {i+1}/{len(batches)}: {len(batch_results):,} rows")
    
    # Tạo DataFrame 1 lần
    df = pd.DataFrame(all_results, columns=COLUMNS)
    print(f"  ✅ Parsed {len(df):,} rows ({time.time()-start:.1f}s)")
    return df

# ================= DATABASE LOAD =================
def load_dimension_bulk(cursor, table, df, columns, id_col):
    """Load dimension với bulk insert"""
    if df.empty: return 0
    
    df = df.drop_duplicates(id_col)
    
    # Get existing IDs 1 lần
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = df[~df[id_col].isin(existing)]
    if new_data.empty: return 0
    
    print(f"    -> {table}: {len(new_data)} new")
    
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    data = []
    for _, row in new_data.iterrows():
        tuple_data = []
        for c in columns:
            val = row[c]
            if c == 'NgaySinh':
                if pd.isna(val) or val == '':
                    tuple_data.append(None)
                else:
                    try:
                        dt = pd.to_datetime(val, format='%d/%m/%Y', errors='coerce')
                        tuple_data.append(dt.strftime('%Y-%m-%d') if pd.notna(dt) else None)
                    except:
                        tuple_data.append(None)
            elif c in ['HocKy', 'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac', 'Is_Valid']:
                tuple_data.append(int(val) if pd.notna(val) and val != '' else 0)
            else:
                tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
        data.append(tuple(tuple_data))
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(new_data)

def load_all_dimensions(cursor, df):
    print("\n  --- DIMENSIONS ---")
    total = 0
    
    # DIM_KHOA
    df_khoa = pd.concat([
        df[['MaKhoa_CN', 'TenKhoa_CN']].rename(columns={'MaKhoa_CN': 'MaKhoa', 'TenKhoa_CN': 'TenKhoa'}),
        df[['MaKhoa_HP', 'TenKhoa_HP']].rename(columns={'MaKhoa_HP': 'MaKhoa', 'TenKhoa_HP': 'TenKhoa'}),
        pd.DataFrame([{'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'}, {'MaKhoa': 'TĐHSP', 'TenKhoa': 'Trường ĐHSP'}])
    ]).drop_duplicates('MaKhoa')
    total += load_dimension_bulk(cursor, 'DIM_KHOA', df_khoa, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
    
    # DIM_NGANH
    df_nganh = df[['MaNganh', 'TenNganh', 'MaKhoa_CN']].rename(columns={'MaKhoa_CN': 'MaKhoa'}).drop_duplicates('MaNganh')
    total += load_dimension_bulk(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
    
    # DIM_CHUYEN_NGANH
    df_cn = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    total += load_dimension_bulk(cursor, 'DIM_CHUYEN_NGANH', df_cn, ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
    
    # DIM_LOP_SINH_VIEN
    df_lop = df[['MaLop', 'Lop', 'MaChuyenNganh']].drop_duplicates('MaLop')
    total += load_dimension_bulk(cursor, 'DIM_LOP_SINH_VIEN', df_lop, ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop')
    
    # DIM_SINH_VIEN
    df_sv = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']].drop_duplicates('MaSV')
    total += load_dimension_bulk(cursor, 'DIM_SINH_VIEN', df_sv, ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV')
    
    # DIM_GIANG_VIEN
    df_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
    total += load_dimension_bulk(cursor, 'DIM_GIANG_VIEN', df_gv, ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
    
    # DIM_HOC_PHAN
    df_hp = df[['MaHP', 'TenHP', 'MaKhoa_HP']].rename(columns={'MaKhoa_HP': 'MaKhoa'}).drop_duplicates('MaHP')
    total += load_dimension_bulk(cursor, 'DIM_HOC_PHAN', df_hp, ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
    
    # DIM_HOC_KY
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    df_hk = pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}])
    total += load_dimension_bulk(cursor, 'DIM_HOC_KY', df_hk, ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
    
    # DIM_LOP_HOC_PHAN
    df_lhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates('MaLopHP')
    df_lhp['MaHocKy'] = ma_hoc_ky
    total += load_dimension_bulk(cursor, 'DIM_LOP_HOC_PHAN', df_lhp, ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
    
    print(f"  📊 Total: {total} new")
    return total

def load_facts(cursor, df):
    """Load cả 2 FACT tables"""
    print("\n  --- FACTS ---")
    
    # FACT_GOP_Y
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    if not df_essay.empty:
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        existing = {row[0] for row in cursor.fetchall()}
        df_new = df_essay[~df_essay['SubmissionID'].isin(existing)]
        
        if not df_new.empty:
            data = [(str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                     str(r['EssayText']), str(r['Sentiment'])[:20], int(r['Is_Valid']),
                     int(r['Tag_HocPhan']), int(r['Tag_DayHoc']), int(r['Tag_KiemTra']), int(r['Tag_Khac']))
                    for _, r in df_new.iterrows()]
            
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i+BATCH_SIZE]
                cursor.executemany("""INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)""", batch)
                cursor.connection.commit()
            print(f"  ✅ FACT_GOP_Y: {len(data):,}")
        else:
            print(f"  ✅ FACT_GOP_Y: 0 new")
    
    # FACT_KET_QUA
    rows = []
    
    # Trắc nghiệm
    for _, r in df[(df['CauHoi'] != '') & (df['GiaTri'] != '')].iterrows():
        try:
            mc = int(float(r['CauHoi']))
            d = int(float(r['GiaTri']))
            if 1 <= mc <= 12 and 1 <= d <= 5:
                rows.append((str(r['SubmissionID'])[:150], mc, d))
        except: pass
    
    # Tự luận
    for _, r in df_essay.iterrows():
        s = r['Sentiment']
        d = 5 if s == 'POSITIVE' else (2 if s == 'NEGATIVE' else 3)
        for mc in [13, 14, 15, 16]:
            rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if rows:
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            cursor.executemany("""INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?,?,?)""", batch)
            cursor.connection.commit()
        print(f"  ✅ FACT_KET_QUA: {len(rows):,}")
    
    return len(df_essay), len(rows)

def load_to_database(df):
    print("\n💾 LOAD TO DATABASE")
    start = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        load_all_dimensions(cursor, df)
        load_facts(cursor, df)
        print(f"  ✅ Done: {time.time()-start:.1f}s")
    except Exception as e:
        print(f"  ❌ {e}")
        import traceback; traceback.print_exc()
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    
    print("\n📥 1. CONNECT & LOAD MASTER")
    start = time.time()
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_data(blob_service)
    
    survey_content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    print(f"  ✅ Extract: {time.time()-start:.1f}s")
    
    if not survey_content:
        print("❌ No data!"); sys.exit(1)
    
    print("\n📝 2. PARSE + NLP")
    start = time.time()
    df = parse_survey_parallel(survey_content)
    print(f"  ✅ Parse: {time.time()-start:.1f}s")
    
    if df.empty:
        print("❌ No data!"); sys.exit(1)
    
    # Backup
    df.to_parquet(f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", index=False)
    
    # Load DB
    load_to_database(df)
    
    print(f"\n🎉 DONE! Total: {time.time()-total_start:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
