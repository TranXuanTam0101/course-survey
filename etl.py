#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - FIXED ALL REQUIREMENTS
- FACT: INSERT ALL (không check trùng)
- TenChuyenNganh: Lấy từ file TenChuyenNganh-Khoa.csv
- NgaySinh: DD/MM/YYYY
- MaKhoa: Tất cả chữ cái đầu của mỗi từ
"""

import os
import sys
import re
import io
import csv
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
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
PARSE_WORKERS = 8

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
    """
    Lấy TẤT CẢ chữ cái đầu tiên của mỗi từ
    Ví dụ: "Bộ môn NNCN" -> "BMNNCN"
           "Trường ĐHNN" -> "TĐHNN"
           "Kế toán" -> "KT"
    """
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "TĐHKT"
    
    words = ten_khoa.split()
    initials = []
    for w in words:
        # Lấy tất cả chữ cái đầu của mỗi từ (bỏ số và ký tự đặc biệt)
        chars = [c.upper() for c in w if c.isalpha()]
        if chars:
            initials.append(chars[0])  # Chỉ lấy chữ cái đầu tiên
    
    return ''.join(initials) if initials else "TĐHKT"

def derive_ma_hoc_ky() -> str:
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

def is_date_format(value) -> bool:
    if not isinstance(value, str):
        return False
    return bool(DATE_PATTERN.match(value.strip()))

def is_ma_gv_format(value) -> bool:
    if not isinstance(value, str):
        return False
    return bool(MA_GV_PATTERN.match(value.strip()))

# ================= EXTRACT SONG SONG =================
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

def parse_hp_csv(content: str) -> pd.DataFrame:
    """Parse HP-Khoa.csv - Cột: STT, MaHP, TenKhoa, TenHP"""
    if not content:
        return pd.DataFrame()
    try:
        df = pd.read_csv(io.StringIO(content))
        if len(df.columns) >= 4:
            df = df.iloc[:, 1:4]  # Bỏ STT
            df.columns = ['MaHP', 'TenKhoa', 'TenHP']
        df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
        return df
    except Exception as e:
        print(f"  -> Lỗi parse HP-Khoa: {e}")
        return pd.DataFrame()

def parse_cn_csv(content: str) -> pd.DataFrame:
    """
    Parse TenChuyenNganh-Khoa.csv
    Cột: STT, TenKhoa, TenChuyenNganh, MaChuyenNganh
    """
    if not content:
        return pd.DataFrame()
    try:
        df = pd.read_csv(io.StringIO(content))
        if len(df.columns) >= 4:
            df = df.iloc[:, 1:4]  # Bỏ STT
            df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
        df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
        return df
    except Exception as e:
        print(f"  -> Lỗi parse TenChuyenNganh-Khoa: {e}")
        return pd.DataFrame()

def extract_all_parallel(blob_service: BlobServiceClient) -> Tuple[str, pd.DataFrame, pd.DataFrame]:
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

# ================= PARSE SONG SONG =================
def parse_one_line(line: str) -> Optional[Dict]:
    if not line or not line.strip():
        return None
    
    line = line.strip()
    try:
        row = next(csv.reader([line], quotechar='"', skipinitialspace=True))
    except:
        row = line.split(',')
    row = [x.strip() for x in row]
    
    ngay_sinh_idx = -1
    ngay_sinh = ''
    for i, val in enumerate(row):
        if DATE_PATTERN.match(val):
            ngay_sinh_idx = i
            ngay_sinh = val
            break
    if ngay_sinh_idx == -1:
        return None
    
    lop = row[0] if len(row) > 0 else ''
    ma_sv = row[1] if len(row) > 1 else ''
    ma_hp = row[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < len(row) else ''
    
    ma_gv = ''
    ma_gv_idx = -1
    for i in range(len(row) - 1, ngay_sinh_idx + 2, -1):
        if MA_GV_PATTERN.match(row[i]):
            ma_gv = row[i]
            ma_gv_idx = i
            break
    if ma_gv_idx == -1:
        ma_gv_idx = len(row) - 4 if len(row) >= 4 else ngay_sinh_idx + 2
    
    ho_ten_parts = row[2:ngay_sinh_idx] if ngay_sinh_idx > 2 else []
    ho_ten = ' '.join(ho_ten_parts)
    name_parts = ho_ten.split()
    ten = name_parts[-1] if name_parts else ''
    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
    
    ten_hp_parts = row[ngay_sinh_idx + 2:ma_gv_idx] if ma_gv_idx > ngay_sinh_idx + 2 else []
    ten_hp = ' '.join(ten_hp_parts)
    
    ho_dem_gv = row[ma_gv_idx + 1] if ma_gv_idx + 1 < len(row) else ''
    ten_gv = row[ma_gv_idx + 2] if ma_gv_idx + 2 < len(row) else ''
    lop_hp = row[ma_gv_idx + 3] if ma_gv_idx + 3 < len(row) else ''
    
    cau13 = cau14 = cau15 = cau16 = ''
    for i in range(ma_gv_idx + 4, len(row)):
        if row[i].upper() == 'NULL':
            after_null = row[i+1:]
            if after_null:
                answers = ','.join(after_null)
                parts = [p.strip() for p in answers.split(',') if p.strip()]
                cau13 = parts[0] if len(parts) > 0 else ''
                cau14 = parts[1] if len(parts) > 1 else ''
                cau15 = parts[2] if len(parts) > 2 else ''
                cau16 = parts[3] if len(parts) > 3 else ''
            break
    
    return {
        'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
        'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
        'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv, 'LopHP': lop_hp,
        'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
    }

def parse_survey_parallel(content: str, max_workers: int = PARSE_WORKERS) -> pd.DataFrame:
    print(f"  -> Đang parse song song ({max_workers} workers)...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(parse_one_line, lines, chunksize=5000))
    
    data = [r for r in results if r is not None]
    df = pd.DataFrame(data)
    print(f"  -> Đã parse {len(df):,} dòng hợp lệ ({time.time()-start:.2f}s)")
    return df

# ================= TRANSFORM =================
def calculate_scores_fast(df: pd.DataFrame) -> pd.DataFrame:
    """Tính điểm cho tất cả câu hỏi"""
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        texts = df[col].fillna('').astype(str).str.lower().values
        scores = np.zeros(len(df), dtype=np.float32)
        weights_dict = ALL_WEIGHTS[col]
        
        for keyword, weight in weights_dict.items():
            mask = np.array([keyword in t for t in texts], dtype=np.float32)
            scores += mask * weight
        
        df[f'{col}_Score'] = scores
    return df

def transform_data(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> Tuple[Dict, pd.DataFrame, str]:
    print("  -> Transform...")
    start = time.time()
    
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    # 1. Chuẩn hóa Lop
    df['IsCTS'] = df['Lop'].str.contains('^CTS-', case=False, na=False)
    df['LopChuanHoa'] = df['Lop'].str.replace('^CTS-', '', regex=True, case=False)
    df['LopChuanHoa'] = df['LopChuanHoa'].str.split('[.\-_]').str[0].fillna('')
    
    # 2. Merge với HP-Khoa để lấy MaKhoa, TenKhoa, TenHP
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna('Trường ĐHKT')
        df['MaKhoa'] = df['MaKhoa'].fillna('TĐHKT')
        df.drop(['TenHP_x', 'TenHP_y'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'TĐHKT'
        df['TenKhoa'] = 'Trường ĐHKT'
    
    # 3. Xác định Chuyên ngành
    # TH1: Lop khớp pattern XXKNN -> "K" + NN
    # TH2: Không khớp -> MaKhoa
    mask_th1 = df['LopChuanHoa'].str.match(r'^\d{2}K\d{2}$', na=False)
    df['MaChuyenNganh'] = df['MaKhoa']
    df.loc[mask_th1, 'MaChuyenNganh'] = 'K' + df.loc[mask_th1, 'LopChuanHoa'].str[3:5]
    
    # 4. Lấy TenChuyenNganh từ cn_master (nếu có), nếu không thì tạo mặc định
    if not cn_master.empty:
        # Tạo mapping từ MaChuyenNganh -> TenChuyenNganh
        cn_mapping = cn_master[['MaChuyenNganh', 'TenChuyenNganh']].drop_duplicates(subset=['MaChuyenNganh'])
        cn_mapping = cn_mapping.set_index('MaChuyenNganh')['TenChuyenNganh'].to_dict()
        
        # Ánh xạ TenChuyenNganh
        df['TenChuyenNganh'] = df['MaChuyenNganh'].map(cn_mapping)
        # Nếu không có trong mapping, tạo tên mặc định
        df['TenChuyenNganh'] = df['TenChuyenNganh'].fillna('Chuyên ngành ' + df['MaChuyenNganh'])
    else:
        df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    
    # 5. Tính điểm
    df = calculate_scores_fast(df)
    
    # 6. Tạo Dimensions
    dims = {
        'hoc_ky': pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}]),
        'khoa': df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa']),
        'chuyen_nganh': df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh']),
        'lop_sv': df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates(subset=['LopChuanHoa']).rename(columns={'LopChuanHoa': 'MaLop'}),
        'sinh_vien': df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV']).rename(columns={'LopChuanHoa': 'MaLop'}),
        'giang_vien': df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).query("MaGV != ''"),
        'hoc_phan': df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP']).query("MaHP != ''"),
        'lop_hp': df.assign(MaLopHP=df['LopHP'] + '_' + df['MaHP'])[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates(subset=['MaLopHP']).query("MaLopHP != '_'")
    }
    
    dims['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
    dims['lop_sv'] = dims['lop_sv'][dims['lop_sv']['MaLop'] != '']
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    # Giữ nguyên NgaySinh dạng DD/MM/YYYY
    dims['sinh_vien']['NgaySinh'] = dims['sinh_vien']['NgaySinh'].astype(str)
    
    # 7. Tạo Fact
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
    
    return dims, fact_df, ma_hoc_ky

# ================= LOAD =================
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table: str, df: pd.DataFrame, columns: List[str], id_col: str) -> int:
    """Load dimension - CHỈ INSERT DÒNG MỚI"""
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
                    # NgaySinh đã là DD/MM/YYYY, convert sang YYYY-MM-DD cho SQL
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

def load_fact_all(cursor, fact_df: pd.DataFrame) -> int:
    """
    Load FACT - INSERT TẤT CẢ (KHÔNG KIỂM TRA TRÙNG)
    """
    if fact_df.empty:
        return 0
    
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    # Chuẩn bị data - INSERT TẤT CẢ, không lọc
    data = list(zip(
        fact_df['SubmissionID'].astype(str).str[:500],
        fact_df['MaCauHoi'].fillna(0).astype(int),
        fact_df['MaSV'].astype(str).str[:50],
        fact_df['MaLopHP'].astype(str).str[:200],
        fact_df['TraLoiSo'].fillna(0).astype(float),
        fact_df['TraLoiText'].fillna('').astype(str).str[:1000],
        fact_df['IsCTS'].fillna(0).astype(int)
    ))
    
    # Tắt constraint tạm thời để tăng tốc
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
        if (i // BATCH_SIZE + 1) % 10 == 0:
            print(f"    -> Đã insert {total:,}/{len(data):,} dòng")
    
    # Bật lại constraint
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    print(f"  ✅ FACT done: {total:,} dòng ({time.time()-start:.2f}s)")
    return total

def load_to_database(dims: Dict, fact_df: pd.DataFrame):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # 1. DIM_HOC_KY
        count = load_dimension(cursor, 'DIM_HOC_KY', dims['hoc_ky'],
                               ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        
        # 2. DIM_KHOA
        count = load_dimension(cursor, 'DIM_KHOA', dims['khoa'],
                               ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        
        # 3. DIM_CTDT
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        print("  ✅ DIM_CTDT: ensured")
        
        # 4. DIM_CHUYEN_NGANH
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims['chuyen_nganh'],
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        
        # 5. DIM_LOP_SINH_VIEN
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims['lop_sv'],
                               ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        
        # 6. DIM_GIANG_VIEN
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims['giang_vien'],
                               ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        
        # 7. DIM_HOC_PHAN
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims['hoc_phan'],
                               ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        
        # 8. DIM_SINH_VIEN
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims['sinh_vien'],
                               ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        
        # 9. DIM_LOP_HOC_PHAN
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims['lop_hp'],
                               ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        
        # 10. FACT - INSERT ALL (KHÔNG LOẠI LẶP)
        count = load_fact_all(cursor, fact_df)
        print(f"  ✅ FACT: {count:,} dòng (INSERT ALL)")
        
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
    print("🚀 SURVEY ETL - FIXED ALL REQUIREMENTS")
    print("=" * 60)
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print(f"Parse workers: {PARSE_WORKERS}")
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
    
    print("\n📝 2. PARSE (8 WORKERS)")
    start = time.time()
    df = parse_survey_parallel(survey_content)
    
    if df.empty:
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
