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
    'timeout': 30,
    'autocommit': False
}

# ================= PATTERNS =================
DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
LOP_PATTERN = re.compile(r'^(\d{2})K(\d{2})$')
CTS_PATTERN = re.compile(r'^CTS-', re.IGNORECASE)

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


# ================= HELPER FUNCTIONS =================
def create_ma_khoa(ten_khoa: str) -> str:
    """Tạo MaKhoa từ chữ cái đầu viết hoa của từng từ"""
    if not isinstance(ten_khoa, str):
        return "UNKNOWN"
    words = ten_khoa.split()
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def normalize_lop(lop: str) -> Tuple[str, bool]:
    """
    Chuẩn hóa Lop: bỏ hậu tố ./-/_
    Trả về (lop_chuan_hoa, is_cts)
    """
    if not isinstance(lop, str):
        return "", False
    
    is_cts = bool(CTS_PATTERN.match(lop))
    
    # Bỏ tiền tố CTS- nếu có
    if is_cts:
        lop = lop[4:]
    
    # Bỏ hậu tố sau . - _
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    
    return lop.strip(), is_cts


def parse_csv_line(line: str) -> List[str]:
    """Parse một dòng CSV với quotechar"""
    return next(csv.reader([line], quotechar='"', skipinitialspace=True))


def download_master_data(blob_service: BlobServiceClient) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Tải và xử lý master data từ Blob"""
    container_name = "tailieu"
    prefix = f"{SEMESTER}/"
    
    hp_df = pd.DataFrame()
    cn_df = pd.DataFrame()
    
    try:
        # HP-Khoa.csv
        hp_blob = blob_service.get_container_client(container_name).get_blob_client(f"{prefix}HP-Khoa.csv")
        if hp_blob.exists():
            data = hp_blob.download_blob().readall()
            hp_df = pd.read_csv(io.StringIO(data.decode('utf-8')))
            hp_df.columns = ['STT', 'MaHP', 'Khoa', 'TenHP']
            hp_df = hp_df[['MaHP', 'Khoa', 'TenHP']]
            hp_df['MaKhoa'] = hp_df['Khoa'].apply(create_ma_khoa)
            hp_df.rename(columns={'Khoa': 'TenKhoa'}, inplace=True)
            print(f"  -> Đã tải {len(hp_df)} học phần")
    except Exception as e:
        print(f"  -> Lỗi tải HP-Khoa.csv: {e}")
    
    try:
        # TenChuyenNganh-Khoa.csv
        cn_blob = blob_service.get_container_client(container_name).get_blob_client(f"{prefix}TenChuyenNganh-Khoa.csv")
        if cn_blob.exists():
            data = cn_blob.download_blob().readall()
            cn_df = pd.read_csv(io.StringIO(data.decode('utf-8')))
            cn_df.columns = ['STT', 'Khoa', 'TenChuyenNganh', 'MaChuyenNganh']
            cn_df = cn_df[['Khoa', 'TenChuyenNganh', 'MaChuyenNganh']]
            cn_df['MaKhoa'] = cn_df['Khoa'].apply(create_ma_khoa)
            cn_df.rename(columns={'Khoa': 'TenKhoa'}, inplace=True)
            print(f"  -> Đã tải {len(cn_df)} chuyên ngành")
    except Exception as e:
        print(f"  -> Lỗi tải TenChuyenNganh-Khoa.csv: {e}")
    
    return hp_df, cn_df


def derive_ma_hoc_ky() -> str:
    """Xác định MaHocKy từ semester và tên file"""
    year_part = SEMESTER.replace('-', '')[2:]  # 2024-2025 -> 2425
    if '252' in SURVEY_FILE:
        hoc_ky = '2'
    elif '251' in SURVEY_FILE:
        hoc_ky = '1'
    else:
        hoc_ky = '2'
    return f"HK{hoc_ky}_{year_part}"


def get_db_connection():
    """Tạo kết nối database"""
    return pymssql.connect(**DB_CONFIG)


# ================= PARSE FUNCTIONS =================
def process_row_vectorized(rows: List[List[str]]) -> pd.DataFrame:
    """Xử lý tất cả các dòng - tối ưu vectorized"""
    results = []
    
    for idx, row in enumerate(rows, 1):
        if not row or len(row) < 2:
            continue
            
        try:
            # Tìm NULL marker
            null_idx = -1
            for i, val in enumerate(row):
                if val and val.upper().strip() == 'NULL':
                    null_idx = i
                    break
            
            if null_idx == -1:
                continue
            
            # Phần sau NULL
            after_null = row[null_idx + 1:]
            raw_answers = ','.join(after_null) if after_null else ''
            
            # Parse 4 câu trả lời
            cau13 = cau14 = cau15 = cau16 = ''
            if raw_answers:
                # Tách đơn giản bằng dấu phẩy và gán theo thứ tự
                parts = [p.strip() for p in raw_answers.split(',') if p.strip()]
                if len(parts) >= 4:
                    cau13, cau14, cau15, cau16 = parts[:4]
                elif len(parts) == 3:
                    cau13, cau14, cau15 = parts
                elif len(parts) == 2:
                    cau13, cau14 = parts
                elif len(parts) == 1:
                    cau13 = parts[0]
            
            # Phần trước NULL
            left_parts = row[:null_idx]
            
            # Xác định từ phải sang trái
            gia_tri = left_parts[-1] if len(left_parts) > 0 else ''
            cau_hoi = left_parts[-2] if len(left_parts) > 1 else ''
            lop_hp = left_parts[-3] if len(left_parts) > 2 else ''
            ten_gv = left_parts[-4] if len(left_parts) > 3 else ''
            ho_dem_gv = left_parts[-5] if len(left_parts) > 4 else ''
            
            # Tìm MaGV
            ma_gv = ''
            ma_gv_idx = -1
            for i in range(len(left_parts) - 6, -1, -1):
                if MA_GV_PATTERN.match(left_parts[i]):
                    ma_gv = left_parts[i]
                    ma_gv_idx = i
                    break
            
            if ma_gv_idx == -1:
                ma_gv_idx = len(left_parts) - 6
            
            # Tìm NgaySinh
            ngay_sinh = ''
            ngay_sinh_idx = -1
            for i, val in enumerate(left_parts):
                if DATE_PATTERN.match(val):
                    ngay_sinh = val
                    ngay_sinh_idx = i
                    break
            
            if ngay_sinh_idx == -1:
                continue
            
            # MaHP (ngay sau NgaySinh)
            ma_hp = left_parts[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < len(left_parts) else ''
            
            # TenHP (giữa NgaySinh và MaGV)
            ten_hp_start = ngay_sinh_idx + 2
            ten_hp_end = ma_gv_idx if ma_gv_idx > ten_hp_start else len(left_parts)
            ten_hp = ' '.join(left_parts[ten_hp_start:ten_hp_end])
            
            # HoTen (từ index 2 đến trước NgaySinh)
            ho_ten_parts = left_parts[2:ngay_sinh_idx]
            ho_ten = ' '.join(ho_ten_parts)
            ho_dem = ''
            ten = ''
            if ho_ten:
                name_parts = ho_ten.split()
                ten = name_parts[-1] if name_parts else ''
                ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            lop = left_parts[0]
            ma_sv = left_parts[1]
            
            results.append({
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
                'Cau13': cau13,
                'Cau14': cau14,
                'Cau15': cau15,
                'Cau16': cau16
            })
            
        except Exception as e:
            print(f"  -> Lỗi dòng {idx}: {e}")
            continue
    
    return pd.DataFrame(results)


# ================= TRANSFORM FUNCTIONS =================
def determine_chuyen_nganh(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> pd.DataFrame:
    """Xác định Chuyên ngành theo TH1 và TH2"""
    
    # Chuẩn hóa Lop và xác định IsCTS
    norm_data = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm_data.apply(lambda x: x[0])
    df['IsCTS'] = norm_data.apply(lambda x: x[1])
    
    # Merge với HP master để lấy MaKhoa
    df = df.merge(hp_master[['MaHP', 'MaKhoa', 'TenHP']], on='MaHP', how='left', suffixes=('', '_master'))
    df['TenHP'] = df['TenHP'].fillna(df['TenHP_master'])
    df.drop(columns=['TenHP_master'], inplace=True, errors='ignore')
    df['MaKhoa'] = df['MaKhoa'].fillna('UNKNOWN')
    
    # Xác định TH1: LopChuanHoa khớp pattern
    def get_th1_cn(lop_chuan):
        match = LOP_PATTERN.match(lop_chuan)
        if match:
            return f"K{match.group(2)}"
        return None
    
    df['MaChuyenNganh_TH1'] = df['LopChuanHoa'].apply(get_th1_cn)
    
    # Tạo mapping MaKhoa -> MaChuyenNganh mặc định (TH2)
    khoa_to_cn = cn_master.groupby('MaKhoa').first()['MaChuyenNganh'].to_dict()
    
    # Xác định MaChuyenNganh cuối cùng
    def get_final_cn(row):
        if pd.notna(row['MaChuyenNganh_TH1']):
            return row['MaChuyenNganh_TH1']
        else:
            # TH2: Lấy từ MaKhoa (CN = MaKhoa)
            return row['MaKhoa']
    
    df['MaChuyenNganh'] = df.apply(get_final_cn, axis=1)
    
    # Tạo TenChuyenNganh
    cn_names = cn_master.set_index('MaChuyenNganh')['TenChuyenNganh'].to_dict()
    df['TenChuyenNganh'] = df['MaChuyenNganh'].map(cn_names).fillna('Chuyên ngành ' + df['MaChuyenNganh'])
    
    # Cleanup
    df.drop(columns=['MaChuyenNganh_TH1'], inplace=True)
    
    return df


def calculate_tra_loi_so_vectorized(df: pd.DataFrame) -> pd.DataFrame:
    """Tính điểm cho các câu trả lời - vectorized"""
    
    def calc_score(text, col_name):
        if pd.isna(text) or text == '':
            return None
        text_lower = str(text).lower()
        score = 0.0
        weights = ALL_WEIGHTS.get(col_name, {})
        for kw, w in weights.items():
            if kw in text_lower:
                score += w
        return score if score > 0 else None
    
    for col in COLUMN_ORDER:
        df[f'{col}_Score'] = df[col].apply(lambda x: calc_score(x, col))
    
    return df


def prepare_dimension_tables(df: pd.DataFrame, ma_hoc_ky: str) -> Dict[str, pd.DataFrame]:
    """Chuẩn bị các DataFrame cho Dimension Tables"""
    
    dims = {}
    
    # DIM_KHOA
    khoa_list = []
    if 'TenKhoa' in df.columns:
        khoa_list.extend(df[['MaKhoa', 'TenKhoa']].drop_duplicates().to_dict('records'))
    dims['khoa'] = pd.DataFrame(khoa_list).drop_duplicates(subset=['MaKhoa']) if khoa_list else pd.DataFrame()
    
    # DIM_CHUYEN_NGANH
    dims['chuyen_nganh'] = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates()
    dims['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
    
    # DIM_LOP_SINH_VIEN
    dims['lop_sv'] = df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates()
    dims['lop_sv'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    
    # DIM_SINH_VIEN
    dims['sinh_vien'] = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV'])
    dims['sinh_vien'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dims['sinh_vien']['NgaySinh'] = pd.to_datetime(dims['sinh_vien']['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    
    # DIM_GIANG_VIEN
    dims['giang_vien'] = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV'])
    
    # DIM_HOC_PHAN
    dims['hoc_phan'] = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP'])
    
    # DIM_LOP_HOC_PHAN
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    dims['lop_hp'] = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates()
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    
    return dims


def prepare_fact_table(df: pd.DataFrame, ma_hoc_ky: str) -> pd.DataFrame:
    """Chuẩn bị Fact Table - Unpivot"""
    
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + FILE_NAME
    
    fact_records = []
    for _, row in df.iterrows():
        for ma_cau_hoi, col in zip([13, 14, 15, 16], COLUMN_ORDER):
            fact_records.append({
                'SubmissionID': row['SubmissionID'],
                'MaCauHoi': ma_cau_hoi,
                'MaSV': row['MaSV'],
                'MaLopHP': row['MaLopHP'],
                'TraLoiSo': row.get(f'{col}_Score'),
                'TraLoiText': row[col] if pd.notna(row[col]) else '',
                'IsCTS': row['IsCTS']
            })
    
    return pd.DataFrame(fact_records)


# ================= LOAD FUNCTIONS =================
def bulk_insert(conn, df: pd.DataFrame, table_name: str, columns: List[str]):
    """Bulk insert với executemany"""
    if df.empty:
        print(f"  -> {table_name}: 0 dòng")
        return 0
    
    cursor = conn.cursor()
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    
    # Chuyển thành list of tuples, xử lý NaN
    data = []
    for _, row in df[columns].iterrows():
        tuple_row = tuple(None if pd.isna(v) else v for v in row)
        data.append(tuple_row)
    
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"  -> {table_name}: {len(data)} dòng")
        return len(data)
    except Exception as e:
        print(f"  -> Lỗi {table_name}: {e}")
        conn.rollback()
        return 0


def load_to_database(dims: Dict[str, pd.DataFrame], fact_df: pd.DataFrame, ma_hoc_ky: str):
    """Load tất cả dữ liệu vào database"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. DIM_HOC_KY
        nam_hoc = SEMESTER
        hoc_ky_so = int(ma_hoc_ky[2])
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = %s)
            INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (%s, %s, %s)
        """, (ma_hoc_ky, ma_hoc_ky, nam_hoc, hoc_ky_so))
        conn.commit()
        print(f"  -> DIM_HOC_KY: Đã đảm bảo tồn tại {ma_hoc_ky}")
        
        # 2. Xóa Fact cũ
        print(f"  -> Đang xóa Fact cũ của kỳ {ma_hoc_ky}...")
        cursor.execute("""
            DELETE F FROM FACT_TRA_LOI_KHAO_SAT F
            INNER JOIN DIM_LOP_HOC_PHAN L ON F.MaLopHP = L.MaLopHP
            WHERE L.MaHocKy = %s
        """, (ma_hoc_ky,))
        deleted = cursor.rowcount
        conn.commit()
        print(f"  -> Đã xóa {deleted} dòng Fact cũ")
        
        # 3. Bulk Insert Dimensions
        bulk_insert(conn, dims.get('khoa', pd.DataFrame()), 'DIM_KHOA', ['MaKhoa', 'TenKhoa'])
        bulk_insert(conn, pd.DataFrame([{'MaCTDT': 'CTDT_CHINHQUY', 'TenCTDT': 'Chính quy'}]), 
                   'DIM_CHUONG_TRINH_DAO_TAO', ['MaCTDT', 'TenCTDT'])
        bulk_insert(conn, dims.get('chuyen_nganh', pd.DataFrame()), 'DIM_CHUYEN_NGANH', 
                   ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'])
        bulk_insert(conn, dims.get('lop_sv', pd.DataFrame()), 'DIM_LOP_SINH_VIEN', 
                   ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'])
        bulk_insert(conn, dims.get('sinh_vien', pd.DataFrame()), 'DIM_SINH_VIEN', 
                   ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'])
        bulk_insert(conn, dims.get('giang_vien', pd.DataFrame()), 'DIM_GIANG_VIEN', 
                   ['MaGV', 'HoDemGV', 'TenGV'])
        bulk_insert(conn, dims.get('hoc_phan', pd.DataFrame()), 'DIM_HOC_PHAN', 
                   ['MaHP', 'TenHP', 'MaKhoa'])
        bulk_insert(conn, dims.get('lop_hp', pd.DataFrame()), 'DIM_LOP_HOC_PHAN', 
                   ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'])
        
        # 4. Bulk Insert Fact
        bulk_insert(conn, fact_df, 'FACT_TRA_LOI_KHAO_SAT', 
                   ['SubmissionID', 'MaCauHoi', 'MaSV', 'MaLopHP', 'TraLoiSo', 'TraLoiText', 'IsCTS'])
        
        print("\n✅ Hoàn tất ETL Database!")
        
    except Exception as e:
        print(f"\n❌ Lỗi DB: {e}")
        conn.rollback()
    finally:
        conn.close()


# ================= MAIN =================
def main():
    print(f"=== SURVEY ETL PIPELINE ===")
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print()
    
    # Kết nối Blob
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"Lỗi kết nối Blob: {e}")
        sys.exit(1)
    
    # 1. EXTRACT
    print("1. EXTRACT - Đang tải dữ liệu...")
    
    # Tải master data
    hp_master, cn_master = download_master_data(blob_service)
    
    # Tải file khảo sát
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    data = blob_client.download_blob().readall()
    
    # Parse CSV
    rows = []
    for line in data.decode('utf-8-sig').strip().split('\n'):
        if line.strip():
            try:
                rows.append(parse_csv_line(line))
            except:
                rows.append([col.strip() for col in line.split(',')])
    
    print(f"  -> Đã đọc {len(rows)} dòng dữ liệu")
    
    # 2. TRANSFORM
    print("\n2. TRANSFORM - Đang xử lý...")
    
    # Parse dữ liệu
    df = process_row_vectorized(rows)
    print(f"  -> Đã parse {len(df)} dòng hợp lệ")
    
    if df.empty:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    # Xác định Chuyên ngành
    df = determine_chuyen_nganh(df, hp_master, cn_master)
    
    # Tính điểm
    df = calculate_tra_loi_so_vectorized(df)
    
    # Xác định MaHocKy
    ma_hoc_ky = derive_ma_hoc_ky()
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    # Thống kê CTS
    cts_count = df['IsCTS'].sum()
    print(f"  -> Số sinh viên CTS: {cts_count}/{len(df)}")
    
    # 3. LOAD
    print("\n3. LOAD - Đang tải lên Database...")
    
    # Chuẩn bị Dimension tables
    dims = prepare_dimension_tables(df, ma_hoc_ky)
    
    # Chuẩn bị Fact table
    fact_df = prepare_fact_table(df, ma_hoc_ky)
    
    # Load vào Database
    load_to_database(dims, fact_df, ma_hoc_ky)
    
    print("\n=== HOÀN THÀNH ===")


if __name__ == "__main__":
    main()
