import os
import sys
import re
import io
import csv
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
import numpy as np
import pymssql
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

DB_CONFIG = {
    'server': 'course-survey.database.windows.net',
    'user': 'sqladmin',
    'password': 'Due@2026',
    'database': 'course-survey-db',
    'timeout': 120,
    'autocommit': False
}

# ================= TRỌNG SỐ =================
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
COLUMN_ORDER = ['Cau13', 'Cau14', 'Cau15', 'Cau16']

# Patterns
DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
LOP_PATTERN = re.compile(r'^(\d{2})K(\d{2})$')
CTS_PATTERN = re.compile(r'^CTS-', re.IGNORECASE)


# ================= HELPER =================
def create_ma_khoa(ten_khoa: str) -> str:
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "UNKNOWN"
    words = ten_khoa.split()
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def normalize_lop(lop: str) -> Tuple[str, bool]:
    if not isinstance(lop, str):
        return "", False
    is_cts = bool(CTS_PATTERN.match(lop))
    if is_cts:
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip(), is_cts


def get_db_connection():
    return pymssql.connect(**DB_CONFIG)


def derive_ma_hoc_ky() -> str:
    # Sửa logic: 2024-2025 -> 2425 (chỉ lấy 2 số cuối của mỗi năm)
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]  # "2024-2025" -> "24" + "25" = "2425"
    if '252' in SURVEY_FILE:
        hoc_ky = '2'
    elif '251' in SURVEY_FILE:
        hoc_ky = '1'
    else:
        hoc_ky = '2'
    return f"HK{hoc_ky}_{year_part}"


def safe_str(value) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


# ================= PARSE FUNCTIONS =================
def is_date_format(value):
    return isinstance(value, str) and bool(DATE_PATTERN.match(value.strip()))


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    return bool(MA_GV_PATTERN.match(value.strip()))


def calculate_weighted_score(text, column_name):
    if not text or not isinstance(text, str):
        return None
    text_lower = text.lower()
    total_score = 0.0
    weights = ALL_WEIGHTS.get(column_name, {})
    for keyword, weight in weights.items():
        if keyword in text_lower:
            total_score += weight
    return total_score if total_score > 0 else None


def parse_survey_fast(content: str) -> pd.DataFrame:
    """Parse nhanh file CSV"""
    lines = content.strip().split('\n')
    rows = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = next(csv.reader([line], quotechar='"', skipinitialspace=True))
            rows.append([col.strip() for col in row])
        except:
            rows.append([col.strip() for col in line.split(',')])
    
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame()
    
    # Tìm vị trí NULL
    results = []
    for idx, row in df.iterrows():
        # Tìm NULL
        null_idx = -1
        for i, val in enumerate(row):
            if isinstance(val, str) and val.upper().strip() == 'NULL':
                null_idx = i
                break
        if null_idx == -1:
            continue
        
        # Sau NULL
        after = row.iloc[null_idx+1:].dropna().astype(str).tolist()
        answers = ','.join(after)
        parts = [p.strip() for p in answers.split(',') if p.strip()]
        cau13 = parts[0] if len(parts) > 0 else ''
        cau14 = parts[1] if len(parts) > 1 else ''
        cau15 = parts[2] if len(parts) > 2 else ''
        cau16 = parts[3] if len(parts) > 3 else ''
        
        # Trước NULL
        left = row.iloc[:null_idx].tolist()
        
        # Ngày sinh
        ngay_sinh_idx = -1
        for i, v in enumerate(left):
            if isinstance(v, str) and DATE_PATTERN.match(v.strip()):
                ngay_sinh_idx = i
                break
        if ngay_sinh_idx == -1:
            continue
        
        ngay_sinh = left[ngay_sinh_idx].strip()
        ma_hp = left[ngay_sinh_idx + 1].strip() if ngay_sinh_idx + 1 < len(left) else ''
        
        # MaGV
        ma_gv = ''
        ma_gv_idx = -1
        for i in range(len(left) - 1, ngay_sinh_idx + 2, -1):
            if isinstance(left[i], str) and MA_GV_PATTERN.match(left[i].strip()):
                ma_gv = left[i].strip()
                ma_gv_idx = i
                break
        if ma_gv_idx == -1:
            ma_gv_idx = len(left) - 4
        
        # TenHP
        ten_hp = ' '.join(str(x).strip() for x in left[ngay_sinh_idx+2:ma_gv_idx] if x and str(x).strip())
        
        # GV info
        ho_dem_gv = left[ma_gv_idx+1].strip() if ma_gv_idx+1 < len(left) else ''
        ten_gv = left[ma_gv_idx+2].strip() if ma_gv_idx+2 < len(left) else ''
        lop_hp = left[ma_gv_idx+3].strip() if ma_gv_idx+3 < len(left) else ''
        
        # SV info
        ho_ten_parts = left[2:ngay_sinh_idx]
        ho_ten = ' '.join(str(x).strip() for x in ho_ten_parts if x and str(x).strip())
        name_parts = ho_ten.split()
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        results.append({
            'Lop': str(left[0]).strip() if len(left) > 0 else '',
            'MaSV': str(left[1]).strip() if len(left) > 1 else '',
            'HoDem': ho_dem,
            'Ten': ten,
            'NgaySinh': ngay_sinh,
            'MaHP': ma_hp,
            'TenHP': ten_hp,
            'MaGV': ma_gv,
            'HoDemGV': ho_dem_gv,
            'TenGV': ten_gv,
            'LopHP': lop_hp,
            'Cau13': cau13,
            'Cau14': cau14,
            'Cau15': cau15,
            'Cau16': cau16
        })
    
    return pd.DataFrame(results)


# ================= MASTER DATA =================
def load_master_data(blob_service: BlobServiceClient) -> Tuple[pd.DataFrame, pd.DataFrame]:
    container = "tailieu"
    prefix = f"{SEMESTER}/"
    hp_df = pd.DataFrame()
    cn_df = pd.DataFrame()
    
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}HP-Khoa.csv")
        if client.exists():
            data = client.download_blob().readall()
            content = data.decode('utf-8')
            # Đọc CSV đúng cách
            hp_df = pd.read_csv(io.StringIO(content))
            # Đổi tên cột dựa trên vị trí
            cols = hp_df.columns.tolist()
            if len(cols) >= 4:
                hp_df = hp_df.iloc[:, 1:4]  # Bỏ cột STT
                hp_df.columns = ['MaHP', 'TenKhoa', 'TenHP']
            hp_df['MaKhoa'] = hp_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> Đã tải {len(hp_df)} học phần")
    except Exception as e:
        print(f"  -> Lỗi HP-Khoa.csv: {e}")
    
    try:
        client = blob_service.get_container_client(container).get_blob_client(f"{prefix}TenChuyenNganh-Khoa.csv")
        if client.exists():
            data = client.download_blob().readall()
            content = data.decode('utf-8')
            cn_df = pd.read_csv(io.StringIO(content))
            cols = cn_df.columns.tolist()
            if len(cols) >= 4:
                cn_df = cn_df.iloc[:, 1:4]  # Bỏ cột STT
                cn_df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
            cn_df['MaKhoa'] = cn_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> Đã tải {len(cn_df)} chuyên ngành")
    except Exception as e:
        print(f"  -> Lỗi TenChuyenNganh-Khoa.csv: {e}")
    
    return hp_df, cn_df


# ================= TRANSFORM =================
def transform_data(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> Tuple[Dict, pd.DataFrame, str]:
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2])
    
    # Chuẩn hóa Lop
    norm = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm.apply(lambda x: x[0])
    df['IsCTS'] = norm.apply(lambda x: x[1])
    
    # Merge master - lấy TenKhoa và MaKhoa
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left', suffixes=('', '_m'))
        df['TenHP'] = df['TenHP_m'].fillna(df['TenHP'])
        df['TenKhoa'] = df['TenKhoa'].fillna('UNKNOWN')
        df['MaKhoa'] = df['MaKhoa'].fillna('UNKNOWN')
        df.drop(columns=['TenHP_m'], inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'UNKNOWN'
        df['TenKhoa'] = 'UNKNOWN'
    
    # Chuyên ngành: TH1 = "K"+NN, TH2 = MaKhoa
    def get_th1(lop):
        if not isinstance(lop, str):
            return None
        m = LOP_PATTERN.match(lop)
        return f"K{m.group(2)}" if m else None
    
    df['MaCN_TH1'] = df['LopChuanHoa'].apply(get_th1)
    df['MaChuyenNganh'] = df['MaCN_TH1'].fillna(df['MaKhoa'])
    df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    df.drop(columns=['MaCN_TH1'], inplace=True)
    
    # Tính điểm
    for col in COLUMN_ORDER:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_weighted_score(x, col))
    
    # === DIM_KHOA ===
    dim_khoa = df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa'])
    dim_khoa = dim_khoa[dim_khoa['MaKhoa'] != 'UNKNOWN']
    
    # === DIM_HOC_KY ===
    dim_hocky = pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}])
    
    # === DIM_CHUYEN_NGANH ===
    dim_cn = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh'])
    dim_cn['MaCTDT'] = 'CTDT_CHINHQUY'
    
    # === DIM_LOP_SINH_VIEN ===
    dim_lop = df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates()
    dim_lop.rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dim_lop = dim_lop[dim_lop['MaLop'] != '']
    
    # === DIM_SINH_VIEN ===
    dim_sv = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV'])
    dim_sv.rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dim_sv['NgaySinh'] = pd.to_datetime(dim_sv['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    
    # === DIM_GIANG_VIEN ===
    dim_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV'])
    dim_gv = dim_gv[dim_gv['MaGV'] != '']
    
    # === DIM_HOC_PHAN ===
    dim_hp = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP'])
    dim_hp = dim_hp[dim_hp['MaHP'] != '']
    
    # === DIM_LOP_HOC_PHAN ===
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    dim_lhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates()
    dim_lhp['MaHocKy'] = ma_hoc_ky
    dim_lhp = dim_lhp[dim_lhp['MaLopHP'] != '_']
    
    # === FACT ===
    df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + FILE_NAME
    fact_rows = []
    for _, row in df.iterrows():
        for mc, col in zip([13, 14, 15, 16], COLUMN_ORDER):
            fact_rows.append({
                'SubmissionID': row['SubmissionID'],
                'MaCauHoi': mc,
                'MaSV': row['MaSV'],
                'MaLopHP': row['MaLopHP'],
                'TraLoiSo': row[f'{col}_Score'],
                'TraLoiText': str(row[col])[:1000] if row[col] else '',
                'IsCTS': row['IsCTS']
            })
    fact_df = pd.DataFrame(fact_rows)
    
    dims = {
        'hoc_ky': dim_hocky,
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
def bulk_insert(conn, df: pd.DataFrame, table_name: str, columns: List[str]):
    if df.empty:
        return 0
    cursor = conn.cursor()
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    data = [tuple(None if pd.isna(row[c]) else row[c] for c in columns) for _, row in df.iterrows()]
    try:
        cursor.executemany(query, data)
        conn.commit()
        return len(data)
    except Exception as e:
        print(f"  -> Lỗi {table_name}: {e}")
        conn.rollback()
        return 0


def load_to_database(dims: Dict, fact_df: pd.DataFrame, ma_hoc_ky: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. DIM_HOC_KY
        hk = dims['hoc_ky'].iloc[0]
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = %s)
            INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (%s, %s, %s)
        """, (hk['MaHocKy'], hk['MaHocKy'], hk['NamHoc'], hk['HocKy']))
        conn.commit()
        print(f"  -> DIM_HOC_KY: {hk['MaHocKy']}")
        
        # 2. DIM_KHOA
        if not dims['khoa'].empty:
            existing = pd.read_sql("SELECT MaKhoa FROM DIM_KHOA", conn)
            new_khoa = dims['khoa'][~dims['khoa']['MaKhoa'].isin(existing['MaKhoa'])]
            if not new_khoa.empty:
                bulk_insert(conn, new_khoa, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'])
                print(f"  -> DIM_KHOA: {len(new_khoa)} dòng")
        
        # 3. DIM_CHUONG_TRINH_DAO_TAO
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        
        # 4. DIM_CHUYEN_NGANH
        if not dims['chuyen_nganh'].empty:
            existing = pd.read_sql("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH", conn)
            new_cn = dims['chuyen_nganh'][~dims['chuyen_nganh']['MaChuyenNganh'].isin(existing['MaChuyenNganh'])]
            if not new_cn.empty:
                bulk_insert(conn, new_cn, 'DIM_CHUYEN_NGANH', 
                           ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'])
                print(f"  -> DIM_CHUYEN_NGANH: {len(new_cn)} dòng")
        
        # 5. DIM_LOP_SINH_VIEN
        if not dims['lop_sv'].empty:
            existing = pd.read_sql("SELECT MaLop FROM DIM_LOP_SINH_VIEN", conn)
            new_lop = dims['lop_sv'][~dims['lop_sv']['MaLop'].isin(existing['MaLop'])]
            if not new_lop.empty:
                bulk_insert(conn, new_lop, 'DIM_LOP_SINH_VIEN', 
                           ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'])
                print(f"  -> DIM_LOP_SINH_VIEN: {len(new_lop)} dòng")
        
        # 6. DIM_GIANG_VIEN
        if not dims['giang_vien'].empty:
            existing = pd.read_sql("SELECT MaGV FROM DIM_GIANG_VIEN", conn)
            new_gv = dims['giang_vien'][~dims['giang_vien']['MaGV'].isin(existing['MaGV'])]
            if not new_gv.empty:
                bulk_insert(conn, new_gv, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'])
                print(f"  -> DIM_GIANG_VIEN: {len(new_gv)} dòng")
        
        # 7. DIM_HOC_PHAN
        if not dims['hoc_phan'].empty:
            existing = pd.read_sql("SELECT MaHP FROM DIM_HOC_PHAN", conn)
            new_hp = dims['hoc_phan'][~dims['hoc_phan']['MaHP'].isin(existing['MaHP'])]
            if not new_hp.empty:
                bulk_insert(conn, new_hp, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'])
                print(f"  -> DIM_HOC_PHAN: {len(new_hp)} dòng")
        
        # 8. DIM_SINH_VIEN (PHẢI INSERT TRƯỚC FACT)
        if not dims['sinh_vien'].empty:
            existing = pd.read_sql("SELECT MaSV FROM DIM_SINH_VIEN", conn)
            new_sv = dims['sinh_vien'][~dims['sinh_vien']['MaSV'].isin(existing['MaSV'])]
            if not new_sv.empty:
                new_sv_copy = new_sv.copy()
                new_sv_copy['NgaySinh'] = new_sv_copy['NgaySinh'].dt.strftime('%Y-%m-%d')
                bulk_insert(conn, new_sv_copy, 'DIM_SINH_VIEN', 
                           ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'])
                print(f"  -> DIM_SINH_VIEN: {len(new_sv)} dòng")
        
        # 9. DIM_LOP_HOC_PHAN
        if not dims['lop_hp'].empty:
            existing = pd.read_sql("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN", conn)
            new_lhp = dims['lop_hp'][~dims['lop_hp']['MaLopHP'].isin(existing['MaLopHP'])]
            if not new_lhp.empty:
                bulk_insert(conn, new_lhp, 'DIM_LOP_HOC_PHAN', 
                           ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'])
                print(f"  -> DIM_LOP_HOC_PHAN: {len(new_lhp)} dòng")
        
        # 10. FACT (INSERT SAU CÙNG)
        if not fact_df.empty:
            # Lọc chỉ giữ các dòng có MaSV và MaLopHP tồn tại
            existing_sv = pd.read_sql("SELECT MaSV FROM DIM_SINH_VIEN", conn)
            existing_lhp = pd.read_sql("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN", conn)
            fact_valid = fact_df[
                fact_df['MaSV'].isin(existing_sv['MaSV']) & 
                fact_df['MaLopHP'].isin(existing_lhp['MaLopHP'])
            ]
            if not fact_valid.empty:
                bulk_insert(conn, fact_valid, 'FACT_TRA_LOI_KHAO_SAT',
                           ['SubmissionID', 'MaCauHoi', 'MaSV', 'MaLopHP', 'TraLoiSo', 'TraLoiText', 'IsCTS'])
                print(f"  -> FACT: {len(fact_valid)} dòng (bỏ {len(fact_df)-len(fact_valid)} dòng lỗi FK)")
        
        print("\n✅ Hoàn tất!")
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


# ================= MAIN =================
def main():
    print(f"=== SURVEY ETL PIPELINE ===")
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}\n")
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # 1. EXTRACT
    print("1. EXTRACT - Đang tải dữ liệu...")
    hp_master, cn_master = load_master_data(blob_service)
    
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    data = blob_client.download_blob().readall()
    content = data.decode('utf-8-sig')
    
    # 2. TRANSFORM
    print("\n2. TRANSFORM - Đang xử lý...")
    df = parse_survey_fast(content)
    print(f"  -> Đã parse {len(df)} dòng hợp lệ")
    
    if df.empty:
        print("Không có dữ liệu!")
        return
    
    dims, fact_df, ma_hoc_ky = transform_data(df, hp_master, cn_master)
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    print(f"  -> Số sinh viên CTS: {df['IsCTS'].sum()}/{len(df)}")
    
    # 3. LOAD
    print("\n3. LOAD - Đang tải lên Database...")
    load_to_database(dims, fact_df, ma_hoc_ky)
    
    # 4. SAVE PROCESSED
    output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    output_path = f"{SEMESTER}/{output_filename}"
    output = df.to_csv(index=False, encoding='utf-8-sig')
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    print(f"\n✅ File kết quả: {output_path}")
    print("\n=== HOÀN THÀNH ===")


if __name__ == "__main__":
    main()
