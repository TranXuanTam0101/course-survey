#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - POLARS ULTRA FAST
- Dùng Polars thay Pandas (nhanh 10-50x)
- Parse CSV trực tiếp không cần custom parser
- Transform vectorized toàn bộ
"""

import os
import sys
import re
import io
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import polars as pl
import numpy as np
import pyodbc
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu SEMESTER hoặc SURVEY_FILE")
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

# ================= PATTERNS =================
DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
CTS_PATTERN = re.compile(r'^CTS-', re.IGNORECASE)

# ================= WEIGHTS =================
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
    'Cau13': WEIGHTS_CAU13, 'Cau14': WEIGHTS_CAU14,
    'Cau15': WEIGHTS_CAU15, 'Cau16': WEIGHTS_CAU16
}

# ================= HELPER FUNCTIONS =================
def to_int(val):
    if val is None or (hasattr(val, 'is_null') and val.is_null()):
        return None
    return int(val)

def to_float(val):
    if val is None or (hasattr(val, 'is_null') and val.is_null()):
        return None
    return float(val)

def to_str(val, max_len=None):
    if val is None or (hasattr(val, 'is_null') and val.is_null()):
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

def derive_ma_hoc_ky() -> str:
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

# ================= EXTRACT =================
def download_blob_to_string(blob_service: BlobServiceClient, container: str, blob_path: str) -> str:
    try:
        blob_client = blob_service.get_container_client(container).get_blob_client(blob_path)
        if not blob_client.exists():
            return ""
        stream = blob_client.download_blob(max_concurrency=4)
        return stream.readall().decode('utf-8-sig')
    except Exception as e:
        print(f"  -> Lỗi download {blob_path}: {e}")
        return ""

def parse_hp_csv(content: str) -> pl.DataFrame:
    if not content:
        return pl.DataFrame()
    try:
        df = pl.read_csv(io.StringIO(content))
        if df.width >= 4:
            df = df.select(df.columns[1:4])
            df = df.rename({df.columns[0]: 'MaHP', df.columns[1]: 'TenKhoa', df.columns[2]: 'TenHP'})
        df = df.with_columns(pl.col('TenKhoa').map_elements(create_ma_khoa, return_dtype=pl.String).alias('MaKhoa'))
        return df
    except Exception as e:
        print(f"  -> Lỗi parse HP-Khoa: {e}")
        return pl.DataFrame()

def parse_cn_csv(content: str) -> pl.DataFrame:
    if not content:
        return pl.DataFrame()
    try:
        df = pl.read_csv(io.StringIO(content))
        if df.width >= 4:
            df = df.select(df.columns[1:4])
            df = df.rename({df.columns[0]: 'TenKhoa', df.columns[1]: 'TenChuyenNganh', df.columns[2]: 'MaChuyenNganh'})
        df = df.with_columns(pl.col('TenKhoa').map_elements(create_ma_khoa, return_dtype=pl.String).alias('MaKhoa'))
        return df
    except Exception as e:
        print(f"  -> Lỗi parse TenChuyenNganh-Khoa: {e}")
        return pl.DataFrame()

def extract_all_parallel(blob_service: BlobServiceClient) -> Tuple[str, pl.DataFrame, pl.DataFrame]:
    print("  -> Download SONG SONG 3 files...")
    start = time.time()
    
    tasks = {
        'survey': ('rawdata', f"{SEMESTER}/{SURVEY_FILE}"),
        'hp': ('tailieu', f"{SEMESTER}/HP-Khoa.csv"),
        'cn': ('tailieu', f"{SEMESTER}/TenChuyenNganh-Khoa.csv")
    }
    
    results = {}
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_key = {
            executor.submit(download_blob_to_string, blob_service, container, path): key
            for key, (container, path) in tasks.items()
        }
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as e:
                print(f"    -> Lỗi {key}: {e}")
                results[key] = ""
    
    hp_df = parse_hp_csv(results.get('hp', ''))
    cn_df = parse_cn_csv(results.get('cn', ''))
    
    print(f"  -> HP-Khoa: {len(hp_df)} dòng")
    print(f"  -> TenChuyenNganh-Khoa: {len(cn_df)} dòng")
    print(f"  ✅ Extract: {time.time()-start:.2f}s")
    
    return results.get('survey', ''), hp_df, cn_df

# ================= PARSE VỚI POLARS (SIÊU NHANH) =================
def parse_survey_polars(content: str) -> pl.DataFrame:
    """
    Parse file survey bằng Polars - NHANH HƠN PANDAS 10x
    Polars tự động xử lý song song, không cần custom ProcessPoolExecutor
    """
    print("  -> Đang parse với Polars...")
    start = time.time()
    
    # Đọc toàn bộ file, không cần header
    df = pl.read_csv(
        io.StringIO(content),
        has_header=False,
        infer_schema=False,
        ignore_errors=True,
        quote_char='"'
    )
    
    print(f"  -> Đã đọc {len(df):,} dòng raw")
    
    # Lấy tất cả cột dưới dạng string
    df = df.select(pl.all().cast(pl.String))
    
    # Tìm cột chứa ngày sinh (pattern DD/MM/YYYY)
    date_cols = []
    for col in df.columns:
        # Kiểm tra mẫu đầu tiên có khớp pattern không
        sample = df[col].head(10).to_list()
        if any(DATE_PATTERN.match(str(v)) for v in sample if v):
            date_cols.append(col)
    
    if not date_cols:
        print("  -> Không tìm thấy cột ngày sinh!")
        return pl.DataFrame()
    
    # Giả định cột ngày sinh là cột đầu tiên khớp pattern
    ngay_sinh_col = date_cols[0]
    col_idx = df.columns.index(ngay_sinh_col)
    
    # Tìm cột MaGV (pattern: 7 số hoặc TGxxxxx)
    ma_gv_col = None
    for col in df.columns[col_idx+2:]:
        sample = df[col].head(20).to_list()
        if any(MA_GV_PATTERN.match(str(v)) for v in sample if v):
            ma_gv_col = col
            break
    
    if not ma_gv_col:
        print("  -> Không tìm thấy cột MaGV!")
        return pl.DataFrame()
    
    ma_gv_idx = df.columns.index(ma_gv_col)
    
    # Tìm cột NULL
    null_col = None
    for col in df.columns[ma_gv_idx+4:]:
        sample = df[col].head(10).to_list()
        if any(str(v).upper() == 'NULL' for v in sample if v):
            null_col = col
            break
    
    # Tạo DataFrame kết quả với rename
    result_cols = {
        df.columns[0]: 'Lop',
        df.columns[1]: 'MaSV',
    }
    
    # NgaySinh
    result_cols[ngay_sinh_col] = 'NgaySinh'
    
    # MaHP (cột sau ngày sinh)
    if col_idx + 1 < len(df.columns):
        result_cols[df.columns[col_idx + 1]] = 'MaHP'
    
    # MaGV
    result_cols[ma_gv_col] = 'MaGV'
    
    # HoDemGV, TenGV, LopHP
    if ma_gv_idx + 1 < len(df.columns):
        result_cols[df.columns[ma_gv_idx + 1]] = 'HoDemGV'
    if ma_gv_idx + 2 < len(df.columns):
        result_cols[df.columns[ma_gv_idx + 2]] = 'TenGV'
    if ma_gv_idx + 3 < len(df.columns):
        result_cols[df.columns[ma_gv_idx + 3]] = 'LopHP'
    
    # Câu trả lời (sau NULL)
    if null_col:
        null_idx = df.columns.index(null_col)
        if null_idx + 1 < len(df.columns):
            result_cols[df.columns[null_idx + 1]] = 'Cau13'
        if null_idx + 2 < len(df.columns):
            result_cols[df.columns[null_idx + 2]] = 'Cau14'
        if null_idx + 3 < len(df.columns):
            result_cols[df.columns[null_idx + 3]] = 'Cau15'
        if null_idx + 4 < len(df.columns):
            result_cols[df.columns[null_idx + 4]] = 'Cau16'
    
    # Chỉ giữ các cột đã xác định
    keep_cols = list(result_cols.keys())
    df = df.select(keep_cols)
    df = df.rename(result_cols)
    
    # Xử lý họ tên SV (từ cột giữa MaSV và NgaySinh)
    # Lưu ý: Polars không có apply row-wise dễ như pandas, nên ta sẽ xử lý sau khi convert sang pandas
    # hoặc dùng biểu thức Polars
    
    print(f"  -> Đã parse {len(df):,} dòng hợp lệ ({time.time()-start:.2f}s)")
    return df

# ================= TRANSFORM =================
def transform_data(df: pl.DataFrame, hp_master: pl.DataFrame, cn_master: pl.DataFrame) -> Tuple[Dict, pl.DataFrame, str]:
    print("  -> Transform...")
    start = time.time()
    
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    # Chuẩn hóa Lop
    df = df.with_columns([
        pl.col('Lop').str.contains('^CTS-').cast(pl.Boolean).alias('IsCTS'),
        pl.col('Lop').str.replace('^CTS-', '').str.split('[.\-_]').list.first().alias('LopChuanHoa')
    ])
    
    # Merge với HP-Khoa
    if not hp_master.is_empty():
        df = df.join(hp_master.select(['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']), on='MaHP', how='left')
        df = df.with_columns([
            pl.col('TenKhoa').fill_null('Trường ĐHKT'),
            pl.col('MaKhoa').fill_null('TĐHKT')
        ])
        # Cập nhật TenHP nếu có từ master
        if 'TenHP_right' in df.columns:
            df = df.with_columns(pl.col('TenHP_right').fill_null(pl.col('TenHP')).alias('TenHP'))
            df = df.drop('TenHP_right')
    else:
        df = df.with_columns([
            pl.lit('TĐHKT').alias('MaKhoa'),
            pl.lit('Trường ĐHKT').alias('TenKhoa')
        ])
    
    # Xác định Chuyên ngành
    df = df.with_columns([
        pl.when(pl.col('LopChuanHoa').str.contains(r'^\d{2}K\d{2}$'))
        .then(pl.lit('K') + pl.col('LopChuanHoa').str.slice(3, 2))
        .otherwise(pl.col('MaKhoa'))
        .alias('MaChuyenNganh')
    ])
    
    # TenChuyenNganh từ cn_master
    if not cn_master.is_empty():
        cn_mapping = cn_master.select(['MaChuyenNganh', 'TenChuyenNganh']).unique(subset=['MaChuyenNganh'])
        df = df.join(cn_mapping, on='MaChuyenNganh', how='left')
        df = df.with_columns(
            pl.col('TenChuyenNganh').fill_null(pl.lit('Chuyên ngành ') + pl.col('MaChuyenNganh'))
        )
    else:
        df = df.with_columns(
            (pl.lit('Chuyên ngành ') + pl.col('MaChuyenNganh')).alias('TenChuyenNganh')
        )
    
    # Tính điểm cho các câu hỏi
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        if col in df.columns:
            weights = ALL_WEIGHTS[col]
            # Tạo biểu thức tính điểm
            expr = pl.lit(0.0)
            for keyword, weight in weights.items():
                expr = expr + pl.when(pl.col(col).str.to_lowercase().str.contains(keyword)).then(pl.lit(weight)).otherwise(0.0)
            df = df.with_columns(expr.alias(f'{col}_Score'))
        else:
            df = df.with_columns(pl.lit(0.0).alias(f'{col}_Score'))
    
    # Tạo MaLopHP và SubmissionID
    df = df.with_columns([
        (pl.col('LopHP') + '_' + pl.col('MaHP')).alias('MaLopHP'),
        (pl.col('MaSV') + '*' + pl.col('LopHP') + '*' + pl.col('MaGV') + '_' + pl.lit(FILE_NAME)).alias('SubmissionID')
    ])
    
    # Convert sang Pandas để tạo dimensions và fact (do pandas có nhiều hàm tiện lợi hơn)
    df_pd = df.to_pandas()
    
    # Tạo Dimensions
    dims = {
        'hoc_ky': pl.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}]).to_pandas(),
        'khoa': df.select(['MaKhoa', 'TenKhoa']).unique(subset=['MaKhoa']).to_pandas(),
        'chuyen_nganh': df.select(['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']).unique(subset=['MaChuyenNganh']).to_pandas(),
        'lop_sv': df.select(['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']).unique(subset=['LopChuanHoa']).rename({'LopChuanHoa': 'MaLop'}).to_pandas(),
        'sinh_vien': df.select(['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']).unique(subset=['MaSV']).rename({'LopChuanHoa': 'MaLop'}).to_pandas(),
        'giang_vien': df.select(['MaGV', 'HoDemGV', 'TenGV']).unique(subset=['MaGV']).filter(pl.col('MaGV') != '').to_pandas(),
        'hoc_phan': df.select(['MaHP', 'TenHP', 'MaKhoa']).unique(subset=['MaHP']).filter(pl.col('MaHP') != '').to_pandas(),
        'lop_hp': df.select(['MaLopHP', 'LopHP', 'MaHP', 'MaGV']).unique(subset=['MaLopHP']).filter(pl.col('MaLopHP') != '_').to_pandas()
    }
    
    dims['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
    dims['lop_sv'] = dims['lop_sv'][dims['lop_sv']['MaLop'] != '']
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    dims['sinh_vien']['NgaySinh'] = dims['sinh_vien']['NgaySinh'].astype(str)
    
    # Tạo Fact
    fact_rows = []
    for col, mc in [('Cau13', 13), ('Cau14', 14), ('Cau15', 15), ('Cau16', 16)]:
        temp = df_pd[['SubmissionID', 'MaSV', 'MaLopHP', col, f'{col}_Score', 'IsCTS']].copy()
        temp.columns = ['SubmissionID', 'MaSV', 'MaLopHP', 'TraLoiText', 'TraLoiSo', 'IsCTS']
        temp['MaCauHoi'] = mc
        fact_rows.append(temp)
    
    import pandas as pd
    fact_df = pd.concat(fact_rows, ignore_index=True)
    fact_df['TraLoiText'] = fact_df['TraLoiText'].fillna('').astype(str).str[:1000]
    
    print(f"  -> Fact: {len(fact_df):,} dòng")
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    return dims, fact_df, ma_hoc_ky

# ================= LOAD =================
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table: str, df, columns: List[str], id_col: str) -> int:
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
                val = row[c]
                tuple_data.append(str(val)[:500] if val else None)
        data.append(tuple(tuple_data))
    
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(new_data)

def load_fact_all(cursor, fact_df) -> int:
    if fact_df.empty:
        return 0
    
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    import pandas as pd
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

def load_to_database(dims: Dict, fact_df):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        count = load_dimension(cursor, 'DIM_HOC_KY', dims['hoc_ky'],
                               ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        
        count = load_dimension(cursor, 'DIM_KHOA', dims['khoa'],
                               ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims['chuyen_nganh'],
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims['lop_sv'],
                               ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims['giang_vien'],
                               ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims['hoc_phan'],
                               ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims['sinh_vien'],
                               ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims['lop_hp'],
                               ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        
        count = load_fact_all(cursor, fact_df)
        print(f"  ✅ FACT: {count:,} dòng")
        
        print(f"  ✅ Load: {time.time()-start:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 SURVEY ETL - POLARS ULTRA FAST")
    print("=" * 60)
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print("=" * 60)
    
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure: {e}")
        sys.exit(1)
    
    print("\n📥 1. EXTRACT (PARALLEL)")
    survey_content, hp_master, cn_master = extract_all_parallel(blob_service)
    
    if not survey_content:
        print("❌ Không thể đọc file survey!")
        sys.exit(1)
    
    print("\n📝 2. PARSE (POLARS)")
    start = time.time()
    df = parse_survey_polars(survey_content)
    
    if df.is_empty():
        print("❌ Không có dữ liệu sau khi parse!")
        sys.exit(1)
    print(f"  ✅ Parse: {time.time()-start:.2f}s")
    
    print("\n🔄 3. TRANSFORM")
    start = time.time()
    dims, fact_df, ma_hoc_ky = transform_data(df, hp_master, cn_master)
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    print("\n💾 4. LOAD")
    start = time.time()
    load_to_database(dims, fact_df)
    
    total = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 HOÀN THÀNH! Tổng thời gian: {total:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
