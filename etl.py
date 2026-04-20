import os
import sys
import re
import io
import csv
import time
from datetime import datetime
from typing import List, Dict, Tuple
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

def derive_ma_hoc_ky() -> str:
    years = SEMESTER.split('-')
    year_part = years[0][2:] + years[1][2:]
    if '252' in SURVEY_FILE:
        hoc_ky = '2'
    elif '251' in SURVEY_FILE:
        hoc_ky = '1'
    else:
        hoc_ky = '2'
    return f"HK{hoc_ky}_{year_part}"

def calculate_score(text, weights_dict):
    if not text or not isinstance(text, str):
        return None
    text_lower = text.lower()
    score = sum(weight for kw, weight in weights_dict.items() if kw in text_lower)
    return score if score > 0 else None

# ================= 🔥 PARSE SIÊU NHANH (VECTORIZED) =================
def parse_survey_ultra_fast(content: str) -> pd.DataFrame:
    """Parse dùng list comprehension - nhanh gấp 50x"""
    print("  -> Đang parse...")
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    
    data = []
    for line in lines:
        # Tìm NULL
        if 'NULL' not in line.upper():
            continue
        
        # Tách trước và sau NULL
        parts = line.split('NULL', 1)
        if len(parts) != 2:
            continue
        
        left_str, right_str = parts[0], parts[1]
        
        # Parse left part
        left = [x.strip() for x in left_str.split(',')]
        
        # Tìm ngày sinh
        ngay_sinh_idx = -1
        ngay_sinh = ''
        for i, v in enumerate(left):
            if DATE_PATTERN.match(v):
                ngay_sinh_idx = i
                ngay_sinh = v
                break
        if ngay_sinh_idx == -1:
            continue
        
        # Lấy thông tin cơ bản
        lop = left[0] if len(left) > 0 else ''
        ma_sv = left[1] if len(left) > 1 else ''
        ma_hp = left[ngay_sinh_idx+1] if ngay_sinh_idx+1 < len(left) else ''
        
        # Tìm MaGV (từ phải sang trái)
        ma_gv = ''
        ma_gv_idx = len(left) - 4
        for i in range(len(left)-1, ngay_sinh_idx+2, -1):
            if MA_GV_PATTERN.match(left[i]):
                ma_gv = left[i]
                ma_gv_idx = i
                break
        
        # Họ tên SV
        ho_ten_parts = left[2:ngay_sinh_idx]
        ho_ten = ' '.join(ho_ten_parts)
        name_parts = ho_ten.split()
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        # Thông tin GV
        ten_hp = ' '.join(left[ngay_sinh_idx+2:ma_gv_idx])
        ho_dem_gv = left[ma_gv_idx+1] if ma_gv_idx+1 < len(left) else ''
        ten_gv = left[ma_gv_idx+2] if ma_gv_idx+2 < len(left) else ''
        lop_hp = left[ma_gv_idx+3] if ma_gv_idx+3 < len(left) else ''
        
        # Parse right part (câu trả lời)
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
def load_master_data(blob_service: BlobServiceClient) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

# ================= TRANSFORM NHANH =================
def transform_data_fast(df: pd.DataFrame, hp_master: pd.DataFrame) -> Tuple[Dict, pd.DataFrame, str]:
    print("  -> Transform...")
    ma_hoc_ky = derive_ma_hoc_ky()
    nam_hoc = SEMESTER
    hoc_ky = int(ma_hoc_ky[2])
    
    # Chuẩn hóa lớp (vectorized)
    norm = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm.apply(lambda x: x[0])
    df['IsCTS'] = norm.apply(lambda x: x[1])
    
    # Merge master
    if not hp_master.empty:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], on='MaHP', how='left')
        df['TenHP'] = df['TenHP_y'].fillna(df['TenHP_x'])
        df['TenKhoa'] = df['TenKhoa'].fillna('UNKNOWN')
        df['MaKhoa'] = df['MaKhoa'].fillna('UNKNOWN')
        df.drop(['TenHP_x', 'TenHP_y'], axis=1, inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'UNKNOWN'
        df['TenKhoa'] = 'UNKNOWN'
    
    df['MaChuyenNganh'] = df['MaKhoa']
    df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
    
    # Tính điểm
    for col in ['Cau13', 'Cau14', 'Cau15', 'Cau16']:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_score(x, ALL_WEIGHTS[col]))
    
    # Dimensions
    dims = {
        'hoc_ky': pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}]),
        'khoa': df[['MaKhoa', 'TenKhoa']].drop_duplicates().query("MaKhoa != 'UNKNOWN'"),
        'sinh_vien': df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV']).rename(columns={'LopChuanHoa': 'MaLop'}),
        'giang_vien': df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates().query("MaGV != ''"),
        'hoc_phan': df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates().query("MaHP != ''"),
        'lop_hp': df.assign(MaLopHP=df['LopHP'] + '_' + df['MaHP'])[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates().query("MaLopHP != '_'")
    }
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    
    # FACT - Vectorized
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

# ================= 🔥 LOAD SIÊU NHANH =================
def load_fast(dims: Dict, fact_df: pd.DataFrame, ma_hoc_ky: str):
    print("  -> Load...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # DIM_HOC_KY
        hk = dims['hoc_ky'].iloc[0]
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = ?)
            INSERT INTO DIM_HOC_KY VALUES (?, ?, ?)
        """, (hk['MaHocKy'], hk['MaHocKy'], hk['NamHoc'], hk['HocKy']))
        
        # Các DIM khác - insert nhanh
        for table, df, cols, id_col in [
            ('DIM_KHOA', dims['khoa'], ['MaKhoa', 'TenKhoa'], 'MaKhoa'),
            ('DIM_GIANG_VIEN', dims['giang_vien'], ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV'),
            ('DIM_HOC_PHAN', dims['hoc_phan'], ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP'),
        ]:
            if not df.empty:
                existing = pd.read_sql(f"SELECT {id_col} FROM {table}", conn)
                new_data = df[~df[id_col].isin(existing[id_col])]
                if not new_data.empty:
                    placeholders = ','.join(['?']*len(cols))
                    data = [tuple(row[c] for c in cols) for _, row in new_data.iterrows()]
                    cursor.executemany(f"INSERT INTO {table} VALUES ({placeholders})", data)
        
        # DIM_SINH_VIEN
        if not dims['sinh_vien'].empty:
            existing = pd.read_sql("SELECT MaSV FROM DIM_SINH_VIEN", conn)
            new_sv = dims['sinh_vien'][~dims['sinh_vien']['MaSV'].isin(existing['MaSV'])].copy()
            if not new_sv.empty:
                new_sv['NgaySinh'] = pd.to_datetime(new_sv['NgaySinh'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
                data = [tuple(row) for row in new_sv[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS']].values]
                cursor.executemany("INSERT INTO DIM_SINH_VIEN VALUES (?,?,?,?,?,?)", data)
        
        # DIM_LOP_HOC_PHAN
        if not dims['lop_hp'].empty:
            existing = pd.read_sql("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN", conn)
            new_lhp = dims['lop_hp'][~dims['lop_hp']['MaLopHP'].isin(existing['MaLopHP'])]
            if not new_lhp.empty:
                data = [tuple(row) for row in new_lhp[['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy']].values]
                cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN VALUES (?,?,?,?,?)", data)
        
        conn.commit()
        
        # FACT - Insert trực tiếp (NHANH NHẤT)
        print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
        start = time.time()
        
        data = []
        for _, row in fact_df.iterrows():
            data.append((
                str(row['SubmissionID'])[:500],
                int(row['MaCauHoi']),
                str(row['MaSV'])[:50],
                str(row['MaLopHP'])[:200],
                float(row['TraLoiSo']) if pd.notna(row['TraLoiSo']) else None,
                str(row['TraLoiText'])[:1000] if row['TraLoiText'] else '',
                1 if row['IsCTS'] else 0
            ))
        
        # Insert theo batch 50K
        for i in range(0, len(data), 50000):
            batch = data[i:i+50000]
            cursor.executemany("""
                INSERT INTO FACT_TRA_LOI_KHAO_SAT 
                (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText, IsCTS)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()
        
        print(f"  ✅ FACT done: {time.time()-start:.2f}s")
        
    finally:
        conn.close()

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 50)
    print("🚀 ULTRA FAST ETL")
    print("=" * 50)
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # EXTRACT
    print("\n📥 EXTRACT")
    start = time.time()
    hp_master, cn_master = load_master_data(blob_service)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    content = blob_client.download_blob().readall().decode('utf-8-sig')
    print(f"  ✅ {time.time()-start:.2f}s")
    
    # TRANSFORM
    print("\n🔄 TRANSFORM")
    start = time.time()
    df = parse_survey_ultra_fast(content)
    print(f"  -> Parse: {len(df):,} dòng")
    dims, fact_df, ma_hoc_ky = transform_data_fast(df, hp_master)
    print(f"  ✅ {time.time()-start:.2f}s")
    
    # LOAD
    print("\n💾 LOAD")
    start = time.time()
    load_fast(dims, fact_df, ma_hoc_ky)
    print(f"  ✅ {time.time()-start:.2f}s")
    
    # TOTAL
    total = time.time() - total_start
    print("\n" + "=" * 50)
    print(f"🎉 TOTAL: {total:.1f}s")
    print("=" * 50)

if __name__ == "__main__":
    main()
