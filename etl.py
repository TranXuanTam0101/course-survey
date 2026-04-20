#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - COMPLETE VERSION
- Không tạo bảng, chỉ INSERT
- FACT: INSERT ALL (không check trùng)
- DIM: Check trùng trước khi INSERT
- Xử lý đúng logic chuyên ngành
"""

import os
import sys
import re
import io
import csv
import time
from datetime import datetime
from typing import Dict, List, Tuple, Optional
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
)

BATCH_SIZE = 50000

# ================= PATTERNS =================
DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
LOP_PATTERN = re.compile(r'^(\d{2})K(\d{2})$')  # XXKNN pattern
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
ALL_WEIGHTS = {'Cau13': WEIGHTS_CAU13, 'Cau14': WEIGHTS_CAU14, 'Cau15': WEIGHTS_CAU15, 'Cau16': WEIGHTS_CAU16}

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
    """Lấy chữ cái đầu của TẤT CẢ các từ"""
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "TĐHKT"
    words = ten_khoa.split()
    initials = []
    for w in words:
        chars = [c.upper() for c in w if c.isalpha()]
        if chars:
            initials.append(chars[0])
    return ''.join(initials) if initials else "TĐHKT"

def normalize_lop(lop: str) -> Tuple[str, bool]:
    """Chuẩn hóa mã lớp"""
    if not isinstance(lop, str):
        return "", False
    is_cts = bool(CTS_PATTERN.match(lop))
    if is_cts:
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip(), is_cts

def derive_ma_hoc_ky() -> str:
    """Tạo mã học kỳ: HK2_2425"""
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

def calculate_score(text: str, weights_dict: Dict) -> Optional[float]:
    """Tính điểm dựa trên keyword"""
    if not text or not isinstance(text, str):
        return None
    text_lower = text.lower()
    score = sum(weight for kw, weight in weights_dict.items() if kw in text_lower)
    return score if score > 0 else None

def is_date_format(value) -> bool:
    return isinstance(value, str) and bool(DATE_PATTERN.match(value.strip()))

def is_ma_gv_format(value) -> bool:
    if not isinstance(value, str):
        return False
    return bool(MA_GV_PATTERN.match(value.strip()))

# ================= MASTER DATA =================
def load_master_data(blob_service: BlobServiceClient) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Đọc HP-Khoa.csv và TenChuyenNganh-Khoa.csv
    Returns: (hp_df, cn_df)
    """
    container = "tailieu"
    prefix = f"{SEMESTER}/"
    hp_df = pd.DataFrame()
    cn_df = pd.DataFrame()
    
    # 1. HP-Khoa.csv
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}HP-Khoa.csv")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8')
            hp_df = pd.read_csv(io.StringIO(content))
            # File có cột: STT, MaHP, TenKhoa, TenHP
            if len(hp_df.columns) >= 4:
                hp_df = hp_df.iloc[:, 1:4]  # Bỏ STT
                hp_df.columns = ['MaHP', 'TenKhoa', 'TenHP']
            hp_df['MaKhoa'] = hp_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> HP-Khoa: {len(hp_df)} dòng")
    except Exception as e:
        print(f"  -> Lỗi đọc HP-Khoa.csv: {e}")
    
    # 2. TenChuyenNganh-Khoa.csv
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}TenChuyenNganh-Khoa.csv")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8')
            cn_df = pd.read_csv(io.StringIO(content))
            # File có cột: STT, TenKhoa, TenChuyenNganh, MaChuyenNganh
            if len(cn_df.columns) >= 4:
                cn_df = cn_df.iloc[:, 1:4]  # Bỏ STT
                cn_df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
            cn_df['MaKhoa'] = cn_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> TenChuyenNganh-Khoa: {len(cn_df)} dòng")
    except Exception as e:
        print(f"  -> Lỗi đọc TenChuyenNganh-Khoa.csv: {e}")
    
    return hp_df, cn_df

# ================= PARSE FILE RAW =================
def parse_survey_file(content: str) -> pd.DataFrame:
    """
    Parse file CSV survey theo từng dòng
    """
    print("  -> Đang parse từng dòng...")
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    
    data = []
    for line_num, line in enumerate(lines, 1):
        # Parse dòng CSV
        try:
            row = next(csv.reader([line], quotechar='"', skipinitialspace=True))
        except:
            row = line.split(',')
        row = [x.strip() for x in row]
        
        # Tìm ngày sinh
        ngay_sinh_idx = -1
        ngay_sinh = ''
        for i, val in enumerate(row):
            if is_date_format(val):
                ngay_sinh_idx = i
                ngay_sinh = val
                break
        if ngay_sinh_idx == -1:
            continue
        
        # Thông tin cơ bản
        lop = row[0] if len(row) > 0 else ''
        ma_sv = row[1] if len(row) > 1 else ''
        ma_hp = row[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < len(row) else ''
        
        # Tìm MaGV (từ phải sang trái)
        ma_gv = ''
        ma_gv_idx = -1
        for i in range(len(row) - 1, ngay_sinh_idx + 2, -1):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i]
                ma_gv_idx = i
                break
        if ma_gv_idx == -1:
            ma_gv_idx = len(row) - 4 if len(row) >= 4 else 0
        
        # Họ tên SV
        ho_ten_parts = row[2:ngay_sinh_idx] if ngay_sinh_idx > 2 else []
        ho_ten = ' '.join(ho_ten_parts)
        name_parts = ho_ten.split()
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        # TenHP
        ten_hp = ' '.join(row[ngay_sinh_idx + 2:ma_gv_idx]) if ma_gv_idx > ngay_sinh_idx + 2 else ''
        
        # Thông tin GV
        ho_dem_gv = row[ma_gv_idx + 1] if ma_gv_idx + 1 < len(row) else ''
        ten_gv = row[ma_gv_idx + 2] if ma_gv_idx + 2 < len(row) else ''
        lop_hp = row[ma_gv_idx + 3] if ma_gv_idx + 3 < len(row) else ''
        
        # Tìm NULL và câu trả lời
        cau13 = cau14 = cau15 = cau16 = ''
        for i in range(ma_gv_idx + 4, len(row)):
            if row[i].upper() == 'NULL':
                after_null = row[i+1:]
                # Parse câu trả lời (phần sau NULL)
                answers = ','.join(after_null)
                parts = [p.strip() for p in answers.split(',') if p.strip()]
                cau13 = parts[0] if len(parts) > 0 else ''
                cau14 = parts[1] if len(parts) > 1 else ''
                cau15 = parts[2] if len(parts) > 2 else ''
                cau16 = parts[3] if len(parts) > 3 else ''
                break
        
        data.append({
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv, 'LopHP': lop_hp,
            'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
        })
    
    print(f"  -> Đã parse {len(data):,} dòng hợp lệ")
    return pd.DataFrame(data)

# ================= TRANSFORM =================
def transform_data(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> Tuple[Dict, pd.DataFrame, str]:
    """
    Transform dữ liệu thành Dimensions và Fact
    """
    print("  -> Transform...")
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    # 1. Chuẩn hóa Lop
    norm = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm.apply(lambda x: x[0])
    df['IsCTS'] = norm.apply(lambda x: x[1])
    
    # 2. Merge với HP-Khoa để lấy MaKhoa, TenKhoa
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna('Trường ĐHKT')
        df['MaKhoa'] = df['MaKhoa'].fillna('TĐHKT')
        df.drop(['TenHP_x', 'TenHP_y'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'TĐHKT'
        df['TenKhoa'] = 'Trường ĐHKT'
    
    # 3. Xác định Chuyên ngành theo logic TH1/TH2
    def get_ma_chuyen_nganh(row):
        lop_chuan = row['LopChuanHoa']
        ma_khoa = row['MaKhoa']
        
        # TH1: Lop khớp pattern XXKNN -> "K" + NN
        m = LOP_PATTERN.match(lop_chuan)
        if m:
            return f"K{m.group(2)}"
        
        # TH2: Không khớp (bao gồm CTS) -> MaKhoa
        return ma_khoa
    
    df['MaChuyenNganh'] = df.apply(get_ma_chuyen_nganh, axis=1)
    df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    
    # 4. Tính điểm
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_score(x, ALL_WEIGHTS[col]))
    
    # 5. Tạo Dimensions
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
    dims['sinh_vien']['NgaySinh'] = pd.to_datetime(dims['sinh_vien']['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    
    # 6. Tạo Fact
    df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + FILE_NAME
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    
    fact_rows = []
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        mc = 13 + ['Cau13', 'Cau14', 'Cau15', 'Cau16'].index(col)
        temp = df[['SubmissionID', 'MaSV', 'MaLopHP', col, f'{col}_Score', 'IsCTS']].copy()
        temp['MaCauHoi'] = mc
        temp.rename(columns={col: 'TraLoiText', f'{col}_Score': 'TraLoiSo'}, inplace=True)
        fact_rows.append(temp)
    
    fact_df = pd.concat(fact_rows, ignore_index=True)
    fact_df['TraLoiText'] = fact_df['TraLoiText'].fillna('').astype(str).str[:1000]
    
    print(f"  -> Fact: {len(fact_df):,} dòng")
    
    return dims, fact_df, ma_hoc_ky

# ================= LOAD =================
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    """Lấy danh sách ID đã tồn tại"""
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table: str, df: pd.DataFrame, columns: List[str], id_col: str) -> int:
    """
    Load dimension - CHỈ INSERT DÒNG MỚI (kiểm tra trùng)
    """
    if df.empty:
        return 0
    
    existing = get_existing_ids(cursor, table, id_col)
    new_data = df[~df[id_col].isin(existing)]
    
    if new_data.empty:
        return 0
    
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    data = [tuple(to_str(row[c]) if c != 'IsCTS' else (1 if row[c] else 0) for c in columns) 
            for _, row in new_data.iterrows()]
    
    cursor.executemany(query, data)
    return len(new_data)

def load_fact_all(cursor, fact_df: pd.DataFrame) -> int:
    """
    Load FACT - INSERT TẤT CẢ (không kiểm tra trùng)
    """
    if fact_df.empty:
        return 0
    
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    data = []
    for _, row in fact_df.iterrows():
        data.append((
            to_str(row['SubmissionID'], 500),
            to_int(row['MaCauHoi']),
            to_str(row['MaSV'], 50),
            to_str(row['MaLopHP'], 200),
            to_float(row['TraLoiSo']),
            to_str(row['TraLoiText'], 1000),
            1 if row['IsCTS'] else 0
        ))
    
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
        print(f"    -> Batch {i//BATCH_SIZE + 1}: {len(batch):,} dòng")
    
    print(f"  ✅ FACT done: {time.time()-start:.2f}s")
    return total

def load_to_database(dims: Dict, fact_df: pd.DataFrame, ma_hoc_ky: str):
    """Load tất cả dữ liệu vào database"""
    print("  -> Load...")
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
        print(f"  ✅ DIM_KHOA: {count} new / {len(dims['khoa'])} total")
        
        # 3. DIM_CTDT
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        cursor.connection.commit()
        print("  ✅ DIM_CTDT: ensured")
        
        # 4. DIM_CHUYEN_NGANH
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims['chuyen_nganh'],
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new / {len(dims['chuyen_nganh'])} total")
        
        # 5. DIM_LOP_SINH_VIEN
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims['lop_sv'],
                               ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new / {len(dims['lop_sv'])} total")
        
        # 6. DIM_GIANG_VIEN
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims['giang_vien'],
                               ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new / {len(dims['giang_vien'])} total")
        
        # 7. DIM_HOC_PHAN
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims['hoc_phan'],
                               ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new / {len(dims['hoc_phan'])} total")
        
        # 8. DIM_SINH_VIEN
        dims['sinh_vien']['NgaySinh'] = dims['sinh_vien']['NgaySinh'].dt.strftime('%Y-%m-%d')
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims['sinh_vien'],
                               ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new / {len(dims['sinh_vien'])} total")
        
        # 9. DIM_LOP_HOC_PHAN
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims['lop_hp'],
                               ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new / {len(dims['lop_hp'])} total")
        
        # 10. FACT - INSERT ALL
        count = load_fact_all(cursor, fact_df)
        print(f"  ✅ FACT: {count:,} dòng đã insert")
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 SURVEY ETL - COMPLETE")
    print("=" * 60)
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # ========== EXTRACT ==========
    print("\n📥 EXTRACT")
    start = time.time()
    hp_master, cn_master = load_master_data(blob_service)
    
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    content = blob_client.download_blob().readall().decode('utf-8-sig')
    print(f"  ✅ Extract: {time.time()-start:.2f}s")
    
    # ========== TRANSFORM ==========
    print("\n🔄 TRANSFORM")
    start = time.time()
    df = parse_survey_file(content)
    dims, fact_df, ma_hoc_ky = transform_data(df, hp_master, cn_master)
    print(f"  ✅ Transform: {time.time()-start:.2f}s")
    
    # ========== LOAD ==========
    print("\n💾 LOAD")
    start = time.time()
    load_to_database(dims, fact_df, ma_hoc_ky)
    print(f"  ✅ Load: {time.time()-start:.2f}s")
    
    # ========== TOTAL ==========
    total = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 TOTAL: {total:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
