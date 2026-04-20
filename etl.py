#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - FIXED ALL FK + MA KHOA + DEFAULT KHOA
"""
import os
import sys
import re
import io
import time
from datetime import datetime
from typing import Dict, Tuple
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

DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
CTS_PATTERN = re.compile(r'^CTS-', re.IGNORECASE)
LOP_PATTERN = re.compile(r'^(\d{2})K(\d{2})$')

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
    """Lấy chữ cái đầu của TẤT CẢ các từ trong tên khoa"""
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "TĐHKT"  # Mặc định: Trường ĐHKT
    
    # Tách từ và lấy chữ cái đầu
    words = ten_khoa.split()
    initials = []
    for w in words:
        # Lấy các chữ cái (bỏ qua số và ký tự đặc biệt)
        chars = [c.upper() for c in w if c.isalpha()]
        if chars:
            initials.append(chars[0])  # Chỉ lấy chữ cái đầu của mỗi từ
    
    if not initials:
        return "TĐHKT"
    
    return ''.join(initials)

def normalize_lop(lop: str):
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
    """Lấy học kỳ từ ký tự cuối cùng trước .csv"""
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]  # "2024-2025" -> "2425"
    base_name = SURVEY_FILE.replace('.csv', '')
    hoc_ky = base_name[-1] if base_name[-1] in ['1', '2'] else '2'
    return f"HK{hoc_ky}_{year_part}"

def calculate_score(text, weights_dict):
    if not text or not isinstance(text, str):
        return None
    text_lower = text.lower()
    score = sum(weight for kw, weight in weights_dict.items() if kw in text_lower)
    return score if score > 0 else None

# ================= PARSE =================
def parse_survey_ultra_fast(content: str) -> pd.DataFrame:
    print("  -> Đang parse...")
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    
    data = []
    for line in lines:
        if 'NULL' not in line.upper():
            continue
        
        parts = line.split('NULL', 1)
        if len(parts) != 2:
            continue
        
        left_str, right_str = parts[0], parts[1]
        left = [x.strip() for x in left_str.split(',')]
        
        ngay_sinh_idx = -1
        ngay_sinh = ''
        for i, v in enumerate(left):
            if DATE_PATTERN.match(v):
                ngay_sinh_idx = i
                ngay_sinh = v
                break
        if ngay_sinh_idx == -1:
            continue
        
        lop = left[0] if len(left) > 0 else ''
        ma_sv = left[1] if len(left) > 1 else ''
        ma_hp = left[ngay_sinh_idx+1] if ngay_sinh_idx+1 < len(left) else ''
        
        ma_gv = ''
        ma_gv_idx = len(left) - 4
        for i in range(len(left)-1, ngay_sinh_idx+2, -1):
            if MA_GV_PATTERN.match(left[i]):
                ma_gv = left[i]
                ma_gv_idx = i
                break
        
        ho_ten_parts = left[2:ngay_sinh_idx]
        ho_ten = ' '.join(ho_ten_parts)
        name_parts = ho_ten.split()
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        ten_hp = ' '.join(left[ngay_sinh_idx+2:ma_gv_idx])
        ho_dem_gv = left[ma_gv_idx+1] if ma_gv_idx+1 < len(left) else ''
        ten_gv = left[ma_gv_idx+2] if ma_gv_idx+2 < len(left) else ''
        lop_hp = left[ma_gv_idx+3] if ma_gv_idx+3 < len(left) else ''
        
        right_parts = [x.strip() for x in right_str.split(',') if x.strip()]
        cau13 = right_parts[0] if len(right_parts) > 0 else ''
        cau14 = right_parts[1] if len(right_parts) > 1 else ''
        cau15 = right_parts[2] if len(right_parts) > 2 else ''
        cau16 = right_parts[3] if len(right_parts) > 3 else ''
        
        data.append({
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv, 'LopHP': lop_hp,
            'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
        })
    
    return pd.DataFrame(data)

# ================= MASTER DATA =================
def load_master_data(blob_service: BlobServiceClient):
    container = "tailieu"
    prefix = f"{SEMESTER}/"
    hp_df = pd.DataFrame()
    cn_df = pd.DataFrame()
    
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}HP-Khoa.csv")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8')
            hp_df = pd.read_csv(io.StringIO(content))
            if len(hp_df.columns) >= 4:
                hp_df = hp_df.iloc[:, 1:4]
                hp_df.columns = ['MaHP', 'TenKhoa', 'TenHP']
            hp_df['MaKhoa'] = hp_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> HP: {len(hp_df)}")
    except Exception as e:
        print(f"  -> Lỗi HP: {e}")
    
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}TenChuyenNganh-Khoa.csv")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8')
            cn_df = pd.read_csv(io.StringIO(content))
            if len(cn_df.columns) >= 4:
                cn_df = cn_df.iloc[:, 1:4]
                cn_df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
            cn_df['MaKhoa'] = cn_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> CN: {len(cn_df)}")
    except Exception as e:
        print(f"  -> Lỗi CN: {e}")
    
    return hp_df, cn_df

# ================= TRANSFORM =================
def transform_data_fast(df: pd.DataFrame, hp_master: pd.DataFrame):
    print("  -> Transform...")
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2]) if ma_hoc_ky[2].isdigit() else 2
    
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    norm = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm.apply(lambda x: x[0])
    df['IsCTS'] = norm.apply(lambda x: x[1])
    
    # Merge master data
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna('Trường ĐHKT')  # FIX: Mặc định là Trường ĐHKT
        df['MaKhoa'] = df['MaKhoa'].fillna('TĐHKT')  # FIX: Mã mặc định TĐHKT
        df.drop(['TenHP_x', 'TenHP_y'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'TĐHKT'
        df['TenKhoa'] = 'Trường ĐHKT'
    
    # Đảm bảo khoa mặc định tồn tại
    default_khoa = pd.DataFrame([{'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'}])
    
    # Chuyên ngành
    def get_th1(lop):
        if not isinstance(lop, str):
            return None
        m = LOP_PATTERN.match(lop)
        return f"K{m.group(2)}" if m else None
    
    df['MaCN_TH1'] = df['LopChuanHoa'].apply(get_th1)
    df['MaChuyenNganh'] = df['MaCN_TH1'].fillna(df['MaKhoa'])
    df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    df.drop(columns=['MaCN_TH1'], inplace=True)
    
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_score(x, ALL_WEIGHTS[col]))
    
    # Tạo dims
    dim_khoa = df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa'])
    dim_khoa = pd.concat([dim_khoa, default_khoa]).drop_duplicates(subset=['MaKhoa'])
    
    dim_cn = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh'])
    dim_cn['MaCTDT'] = 'CTDT_CHINHQUY'
    
    dim_lop = df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates()
    dim_lop.rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dim_lop = dim_lop[dim_lop['MaLop'] != '']
    
    dim_sv = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV'])
    dim_sv.rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dim_sv['NgaySinh'] = pd.to_datetime(dim_sv['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    
    dim_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).query("MaGV != ''")
    
    dim_hp = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP']).query("MaHP != ''")
    
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    dim_lhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates(subset=['MaLopHP']).query("MaLopHP != '_'")
    dim_lhp['MaHocKy'] = ma_hoc_ky
    
    # Fact
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
    
    dims = {
        'hoc_ky': pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}]),
        'khoa': dim_khoa,
        'chuyen_nganh': dim_cn,
        'lop_sv': dim_lop,
        'sinh_vien': dim_sv,
        'giang_vien': dim_gv,
        'hoc_phan': dim_hp,
        'lop_hp': dim_lhp
    }
    
    return dims, fact_df, ma_hoc_ky

# ================= LOAD =================
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_fast(dims: Dict, fact_df: pd.DataFrame, ma_hoc_ky: str):
    print("  -> Load...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # 1. DIM_HOC_KY
        hk = dims['hoc_ky'].iloc[0]
        existing_hk = get_existing_ids(cursor, 'DIM_HOC_KY', 'MaHocKy')
        if hk['MaHocKy'] not in existing_hk:
            cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)",
                          (to_str(hk['MaHocKy']), to_str(hk['NamHoc']), to_int(hk['HocKy'])))
            conn.commit()
            print("  ✅ DIM_HOC_KY: inserted")
        else:
            print("  ⏭️ DIM_HOC_KY: already exists")
        
        # 2. DIM_KHOA
        existing_khoa = get_existing_ids(cursor, 'DIM_KHOA', 'MaKhoa')
        new_khoa = dims['khoa'][~dims['khoa']['MaKhoa'].isin(existing_khoa)]
        if not new_khoa.empty:
            data = [(to_str(r['MaKhoa']), to_str(r['TenKhoa'])) for _, r in new_khoa.iterrows()]
            cursor.executemany("INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)", data)
            conn.commit()
            existing_khoa.update(new_khoa['MaKhoa'].tolist())
        print(f"  ✅ DIM_KHOA: {len(new_khoa)} new / {len(dims['khoa'])} total")
        
        # 3. DIM_CTDT
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        
        # 4. DIM_CHUYEN_NGANH
        existing_cn = get_existing_ids(cursor, 'DIM_CHUYEN_NGANH', 'MaChuyenNganh')
        new_cn = dims['chuyen_nganh'][~dims['chuyen_nganh']['MaChuyenNganh'].isin(existing_cn)]
        if not new_cn.empty:
            data = [(to_str(r['MaChuyenNganh']), to_str(r['TenChuyenNganh']), 
                     to_str(r['MaKhoa']), 'CTDT_CHINHQUY') for _, r in new_cn.iterrows()]
            cursor.executemany("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaKhoa, MaCTDT) VALUES (?, ?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_CHUYEN_NGANH: {len(new_cn)} new / {len(dims['chuyen_nganh'])} total")
        
        # 5. DIM_LOP_SINH_VIEN (PHẢI INSERT TRƯỚC DIM_SINH_VIEN)
        existing_lop = get_existing_ids(cursor, 'DIM_LOP_SINH_VIEN', 'MaLop')
        new_lop = dims['lop_sv'][~dims['lop_sv']['MaLop'].isin(existing_lop)]
        if not new_lop.empty:
            data = [(to_str(r['MaLop']), to_str(r['Lop']), 
                     to_str(r['MaChuyenNganh']), 1 if r['IsCTS'] else 0) for _, r in new_lop.iterrows()]
            cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh, IsCTS) VALUES (?, ?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_LOP_SINH_VIEN: {len(new_lop)} new / {len(dims['lop_sv'])} total")
        
        # 6. DIM_GIANG_VIEN
        existing_gv = get_existing_ids(cursor, 'DIM_GIANG_VIEN', 'MaGV')
        new_gv = dims['giang_vien'][~dims['giang_vien']['MaGV'].isin(existing_gv)]
        if not new_gv.empty:
            data = [(to_str(r['MaGV']), to_str(r['HoDemGV']), to_str(r['TenGV'])) for _, r in new_gv.iterrows()]
            cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_GIANG_VIEN: {len(new_gv)} new / {len(dims['giang_vien'])} total")
        
        # 7. DIM_HOC_PHAN
        valid_hp = dims['hoc_phan'][dims['hoc_phan']['MaKhoa'].isin(existing_khoa)]
        existing_hp = get_existing_ids(cursor, 'DIM_HOC_PHAN', 'MaHP')
        new_hp = valid_hp[~valid_hp['MaHP'].isin(existing_hp)]
        if not new_hp.empty:
            data = [(to_str(r['MaHP']), to_str(r['TenHP']), to_str(r['MaKhoa'])) for _, r in new_hp.iterrows()]
            cursor.executemany("INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES (?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_HOC_PHAN: {len(new_hp)} new / {len(valid_hp)} valid")
        
        # 8. DIM_SINH_VIEN (SAU KHI CÓ DIM_LOP_SINH_VIEN)
        existing_sv = get_existing_ids(cursor, 'DIM_SINH_VIEN', 'MaSV')
        new_sv = dims['sinh_vien'][~dims['sinh_vien']['MaSV'].isin(existing_sv)].copy()
        if not new_sv.empty:
            new_sv['NgaySinh'] = pd.to_datetime(new_sv['NgaySinh'], format='%d/%m/%Y', errors='coerce').dt.strftime('%Y-%m-%d')
            data = []
            for _, r in new_sv.iterrows():
                data.append((
                    to_str(r['MaSV']), to_str(r['HoDem']), to_str(r['Ten']),
                    to_str(r['NgaySinh']) if pd.notna(r['NgaySinh']) else None,
                    to_str(r['MaLop']), to_int(r['IsCTS']) or 0
                ))
            cursor.executemany("INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop, IsCTS) VALUES (?, ?, ?, ?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_SINH_VIEN: {len(new_sv)} new / {len(dims['sinh_vien'])} total")
        
        # 9. DIM_LOP_HOC_PHAN
        existing_lhp = get_existing_ids(cursor, 'DIM_LOP_HOC_PHAN', 'MaLopHP')
        new_lhp = dims['lop_hp'][~dims['lop_hp']['MaLopHP'].isin(existing_lhp)]
        if not new_lhp.empty:
            data = [(to_str(r['MaLopHP']), to_str(r['LopHP']), to_str(r['MaHP']),
                     to_str(r['MaGV']), to_str(r['MaHocKy'])) for _, r in new_lhp.iterrows()]
            cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", data)
            conn.commit()
        print(f"  ✅ DIM_LOP_HOC_PHAN: {len(new_lhp)} new / {len(dims['lop_hp'])} total")
        
        # 10. FACT
        print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
        start = time.time()
        
        valid_sv = get_existing_ids(cursor, 'DIM_SINH_VIEN', 'MaSV')
        valid_lhp = get_existing_ids(cursor, 'DIM_LOP_HOC_PHAN', 'MaLopHP')
        
        data = []
        for _, row in fact_df.iterrows():
            ma_sv = to_str(row['MaSV'])
            ma_lop_hp = to_str(row['MaLopHP'])
            if ma_sv in valid_sv and ma_lop_hp in valid_lhp:
                data.append((
                    to_str(row['SubmissionID'], 500), to_int(row['MaCauHoi']),
                    ma_sv[:50], ma_lop_hp[:200], to_float(row['TraLoiSo']),
                    to_str(row['TraLoiText'], 1000), 1 if row['IsCTS'] else 0
                ))
        
        print(f"  -> Valid: {len(data):,} dòng")
        
        for i in range(0, len(data), 50000):
            batch = data[i:i+50000]
            cursor.executemany("""
                INSERT INTO FACT_TRA_LOI_KHAO_SAT 
                (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText, IsCTS)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
            print(f"    -> Batch {i//50000 + 1}: {len(batch)} dòng")
        
        print(f"  ✅ FACT done: {time.time()-start:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 50)
    print("🚀 ULTRA FAST ETL (COMPLETE)")
    print("=" * 50)
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    print("\n📥 EXTRACT")
    start = time.time()
    hp_master, cn_master = load_master_data(blob_service)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    content = blob_client.download_blob().readall().decode('utf-8-sig')
    print(f"  ✅ {time.time()-start:.2f}s")
    
    print("\n🔄 TRANSFORM")
    start = time.time()
    df = parse_survey_ultra_fast(content)
    print(f"  -> Parse: {len(df):,} dòng")
    dims, fact_df, ma_hoc_ky = transform_data_fast(df, hp_master)
    print(f"  ✅ {time.time()-start:.2f}s")
    
    print("\n💾 LOAD")
    start = time.time()
    load_fast(dims, fact_df, ma_hoc_ky)
    print(f"  ✅ {time.time()-start:.2f}s")
    
    total = time.time() - total_start
    print("\n" + "=" * 50)
    print(f"🎉 TOTAL: {total:.1f}s")
    print("=" * 50)

if __name__ == "__main__":
    main()
