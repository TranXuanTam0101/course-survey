import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp
import pickle

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_CONTAINER = "tailieu"
PROCESSED_PATH = "processed-data"
PREPROCESSED_PATH = "preprocessed-data"  # Thư mục mới cho dữ liệu đã tiền xử lý

# Số lượng worker
NUM_WORKERS = max(2, mp.cpu_count())
CHUNK_SIZE = 50000

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')


# ================= HÀM TIỆN ÍCH =================
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


def create_ma_khoa(ten_khoa: str) -> str:
    SPECIAL_MA_KHOA = {
        'Bộ môn NNCN': 'BNNNCN', 'Trường ĐHNN': 'TĐHNN', 'Luật': 'LUAT',
        'Marketing': 'MKT', 'Trường ĐHKT': 'TĐHKT', 'Phòng Đào Tạo': 'PĐT'
    }
    for special_name, special_code in SPECIAL_MA_KHOA.items():
        if special_name.lower() in ten_khoa.lower():
            return special_code
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def extract_ma_nganh_from_ten_nganh(ten_nganh: str) -> str:
    if not isinstance(ten_nganh, str) or not ten_nganh:
        return "UNKNOWN"
    words = re.split(r'[\s\-]+', ten_nganh.strip())
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def determine_ma_chuyen_nganh(lop: str) -> tuple:
    """Xác định MaChuyenNganh từ Lop"""
    if not lop or not isinstance(lop, str):
        return None, None, None, None
    
    lop_upper = lop.upper().strip()
    
    # CÓ CTS
    if 'CTS' in lop_upper:
        return "NULL_CTS", "Chuyên ngành NULL_CTS", "Trường ĐH Kinh tế", "TĐHKT"
    
    # CÓ QT
    if 'QT' in lop_upper:
        return "NULL_QT", "Chuyên ngành NULL_QT", "Phòng Đào Tạo", "PĐT"
    
    # CÓ ACCA
    if 'ACCA' in lop_upper:
        match = re.search(r'K(\d{2})', lop_upper)
        if match:
            ma_cn = f"K{match.group(1)}-ACCA"
            return ma_cn, None, None, None
    
    # CÒN LẠI - LẤY Kxx
    match = re.search(r'K(\d{2})', lop_upper)
    if match:
        ma_cn = f"K{match.group(1)}"
        return ma_cn, None, None, None
    
    return None, None, None, None


# ================= BLOB FUNCTIONS =================
def download_blob(blob_service, container, path):
    try:
        container_client = blob_service.get_container_client(container)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            return blob.download_blob().readall().decode('utf-8-sig')
        return ""
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return ""


def save_preprocessed_data(blob_service, data_dict, filename):
    """Lưu dữ liệu đã tiền xử lý dưới dạng pickle"""
    path = f"{PREPROCESSED_PATH}/{filename}.pkl"
    try:
        # Chuyển DataFrame thành binary
        pickled_data = pickle.dumps(data_dict)
        
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(path)
        blob.upload_blob(pickled_data, overwrite=True)
        print(f"  ✅ Đã lưu preprocessed data: {path}")
        return True
    except Exception as e:
        print(f"  ❌ Lỗi lưu: {e}")
        return False


# ================= LOAD MASTER DATA =================
def load_hp_master(blob_service):
    content = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    if not content:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 4:
        df = df.iloc[:, 1:4]
        df.columns = ['MaHP', 'TenKhoa', 'TenHP']
    df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
    return df


def load_chuyennganh_master(blob_service):
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if not content:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean = df_clean[df_clean['MaChuyenNganh'].astype(str).str.strip() != '']
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(extract_ma_nganh_from_ten_nganh)
    df_clean = df_clean.drop_duplicates(subset=['MaChuyenNganh'])
    
    dim_nganh = df_clean[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
    dim_chuyennganh = df_clean[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    
    mapping = {}
    for _, row in df_clean.iterrows():
        ma_chuyen = row['MaChuyenNganh']
        if ma_chuyen and ma_chuyen not in mapping:
            mapping[ma_chuyen] = {
                'TenChuyenNganh': row['TenChuyenNganh'],
                'MaNganh': row['MaNganh'],
                'TenNganh': row['TenNganh'],
                'MaKhoa': row['MaKhoa'],
                'TenKhoa': row['TenKhoa']
            }
    return dim_nganh, dim_chuyennganh, mapping


# ================= PARSE SURVEY DATA =================
def is_date_format(value):
    return bool(_date_pattern.match(value.strip())) if isinstance(value, str) else False


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH"


def parse_lines_batch(lines_batch):
    results = []
    for line in lines_batch:
        if not line or not line.strip():
            continue
        row = [x.strip() for x in line.split(',')]
        row_len = len(row)
        if row_len < 15:
            continue
        try:
            lop = row[0]
            ma_sv = row[1]
            ngay_sinh = ''
            ngay_sinh_index = -1
            for i in range(2, min(row_len, 12)):
                if is_date_format(row[i]):
                    ngay_sinh = row[i]
                    ngay_sinh_index = i
                    break
            if ngay_sinh_index == -1:
                continue
            
            ho_dem = ''
            ten = ''
            if ngay_sinh_index > 1:
                name_parts = [p for p in row[2:ngay_sinh_index] if p]
                if name_parts:
                    ten = name_parts[-1]
                    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else ''
            ma_gv = ''
            ma_gv_index = -1
            
            for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
                if is_ma_gv_format(row[i]):
                    ma_gv = row[i]
                    ma_gv_index = i
                    break
            
            if ma_gv_index == -1:
                ma_gv_index = row_len - 4 if row_len >= 4 else ngay_sinh_index + 2
            
            ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]) if ma_gv_index > ngay_sinh_index + 2 else ''
            ho_dem_gv = row[ma_gv_index + 1] if ma_gv_index + 1 < row_len else ''
            ten_gv = row[ma_gv_index + 2] if ma_gv_index + 2 < row_len else ''
            lop_hp = row[ma_gv_index + 3] if ma_gv_index + 3 < row_len else ''
            cau_hoi = row[ma_gv_index + 4] if ma_gv_index + 4 < row_len else ''
            gia_tri = row[ma_gv_index + 5] if ma_gv_index + 5 < row_len else ''
            
            null_index = -1
            for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
                if row[i].upper() == 'NULL' or row[i] == '':
                    null_index = i
                    break
            
            essay_text = ''
            if null_index != -1 and null_index + 1 < row_len:
                after_null = row[null_index + 1:]
                essay_text = ','.join(after_null).strip()
            
            submission_id = f"{ma_sv}_{lop_hp}_{ma_gv}_{FILE_NAME}"
            
            results.append({
                'SubmissionID': submission_id,
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
                'CauHoi': cau_hoi,
                'GiaTri': gia_tri,
                'EssayText': essay_text
            })
        except Exception:
            continue
    return results


def parse_survey_to_long_format(content: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_rows = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    df = pd.DataFrame(all_rows)
    print(f"  -> Đã parse {len(df):,} dòng câu trả lời ({time.time()-start:.2f}s)")
    return df


# ================= TIỀN XỬ LÝ DỮ LIỆU =================
def preprocess_data(df_raw: pd.DataFrame, hp_master, dim_nganh, dim_chuyennganh, mapping):
    """Tiền xử lý tất cả dữ liệu và trả về các DataFrame đã sẵn sàng để insert"""
    print("  -> Tiền xử lý dữ liệu...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # ===== 1. XỬ LÝ DIMENSION DATA =====
    print("\n  📊 Xử lý DIMENSION tables...")
    
    # DIM_KHOA
    all_khoa = set()
    if not hp_master.empty:
        all_khoa.update(hp_master['MaKhoa'].unique())
    if not dim_nganh.empty:
        all_khoa.update(dim_nganh['MaKhoa'].unique())
    default_khoa = {'TĐHKT': 'Trường ĐH Kinh tế', 'PĐT': 'Phòng Đào Tạo'}
    all_khoa.update(default_khoa.keys())
    
    dim_khoa = pd.DataFrame([(ma, default_khoa.get(ma, ma)) for ma in all_khoa], 
                            columns=['MaKhoa', 'TenKhoa'])
    
    # DIM_NGANH
    default_nganh = [
        ('NULL_CTS', 'Ngành NULL_CTS', 'TĐHKT'),
        ('NULL_QT', 'Ngành NULL_QT', 'PĐT')
    ]
    dim_nganh_list = [(ma, ten, makhoa) for ma, ten, makhoa in default_nganh]
    
    if not dim_chuyennganh.empty:
        for _, row in dim_chuyennganh.iterrows():
            ma_nganh = row['MaNganh']
            ten_nganh = row.get('TenNganh', f'Ngành {ma_nganh}')
            dim_nganh_list.append((ma_nganh, ten_nganh, 'TĐHKT'))
    
    dim_nganh_df = pd.DataFrame(dim_nganh_list, columns=['MaNganh', 'TenNganh', 'MaKhoa']).drop_duplicates('MaNganh')
    
    # DIM_CHUYEN_NGANH
    default_chuyennganh = [
        ('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
        ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')
    ]
    dim_chuyennganh_list = [(ma, ten, manganh) for ma, ten, manganh in default_chuyennganh]
    
    if not dim_chuyennganh.empty:
        for _, row in dim_chuyennganh.iterrows():
            ma_chuyen = row['MaChuyenNganh']
            ma_nganh = row['MaNganh']
            dim_chuyennganh_list.append((ma_chuyen, row['TenChuyenNganh'], ma_nganh))
    
    dim_chuyennganh_df = pd.DataFrame(dim_chuyennganh_list, 
                                      columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']).drop_duplicates('MaChuyenNganh')
    
    # DIM_HOC_PHAN
    hp_dict = {}
    if not hp_master.empty:
        hp_master_unique = hp_master.drop_duplicates(subset=['MaHP'], keep='first')
        hp_dict = hp_master_unique.set_index('MaHP')[['TenHP', 'MaKhoa']].to_dict('index')
    
    df_hp_raw = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP'])
    dim_hocphan_list = []
    for _, row in df_hp_raw.iterrows():
        ma_hp = row['MaHP']
        if ma_hp in hp_dict:
            dim_hocphan_list.append((ma_hp, hp_dict[ma_hp]['TenHP'], hp_dict[ma_hp]['MaKhoa']))
        else:
            ten_hp = row['TenHP'] if pd.notna(row['TenHP']) else f"Học phần {ma_hp}"
            dim_hocphan_list.append((ma_hp, ten_hp, 'TĐHKT'))
    
    dim_hocphan_df = pd.DataFrame(dim_hocphan_list, columns=['MaHP', 'TenHP', 'MaKhoa']).drop_duplicates('MaHP')
    
    # DIM_GIANG_VIEN
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    dim_giangvien_df = pd.DataFrame(df_gv, columns=['MaGV', 'HoDemGV', 'TenGV'])
    
    # DIM_HOC_KY
    dim_hocky_df = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], 
                                columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # DIM_LOP_SINH_VIEN
    valid_chuyennganh = set(dim_chuyennganh_df['MaChuyenNganh'].values)
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    
    dim_lopsv_list = []
    for _, row in df_lop_unique.iterrows():
        lop = row['Lop']
        ma_cn, ten_cn, ten_khoa, ma_khoa = determine_ma_chuyen_nganh(lop)
        if ma_cn and ma_cn in valid_chuyennganh:
            dim_lopsv_list.append((lop, lop, ma_cn))
    
    dim_lopsv_df = pd.DataFrame(dim_lopsv_list, columns=['MaLop', 'Lop', 'MaChuyenNganh']).drop_duplicates('MaLop')
    
    # DIM_SINH_VIEN
    valid_lop = set(dim_lopsv_df['MaLop'].values)
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    
    dim_sinhvien_list = []
    for _, row in df_sv.iterrows():
        ma_sv = row['MaSV']
        lop = row['Lop']
        if lop in valid_lop:
            ngay_sinh = None
            if row['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            dim_sinhvien_list.append((ma_sv, row['HoDem'] or '', row['Ten'] or '', ngay_sinh, lop))
    
    dim_sinhvien_df = pd.DataFrame(dim_sinhvien_list, 
                                   columns=['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']).drop_duplicates('MaSV')
    
    # DIM_LOP_HOC_PHAN
    valid_hp = set(dim_hocphan_df['MaHP'].values)
    valid_gv = set(dim_giangvien_df['MaGV'].values)
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    
    dim_lophp_list = []
    for _, row in df_lhp.iterrows():
        lop_hp = row['LopHP']
        if row['MaHP'] in valid_hp and row['MaGV'] in valid_gv:
            dim_lophp_list.append((lop_hp, lop_hp, row['MaHP'], row['MaGV'], ma_hoc_ky))
    
    dim_lophp_df = pd.DataFrame(dim_lophp_list, 
                                columns=['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy']).drop_duplicates('MaLopHP')
    
    # ===== 2. XỬ LÝ FACT DATA =====
    print("\n  📊 Xử lý FACT tables...")
    
    # FACT_GOP_Y_TU_LUAN
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if text_df.empty:
        fact_main_df = pd.DataFrame()
    else:
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        text_df_unique['NoiDungGopY'] = text_df_unique['NoiDungGopY'].str[:4000]  # Giới hạn độ dài
        text_df_unique['Is_Valid'] = 1
        text_df_unique['Sentiment'] = 'neutral'  # Placeholder, sẽ được xử lý bởi job 2
        text_df_unique['Tag_HocPhan'] = 0
        text_df_unique['Tag_DayHoc'] = 0
        text_df_unique['Tag_KiemTra'] = 0
        text_df_unique['Tag_Khac'] = 1
        
        fact_main_df = text_df_unique[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid',
            'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].copy()
    
    # FACT_KET_QUA_DANH_GIA - Tạo đủ 12 câu
    mcq_df = df_raw[
        df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
        df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != '')
    ].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
        all_submissions = mcq_df['SubmissionID'].unique()
        fact_ketqua_list = []
        
        for sub_id in all_submissions:
            sub_data = mcq_df[mcq_df['SubmissionID'] == sub_id]
            existing_questions = set(sub_data['MaCauHoi'].values)
            
            if len(existing_questions) >= 12:
                if len(existing_questions) > 12:
                    top_questions = sub_data.nlargest(12, 'Diem')[['SubmissionID', 'MaCauHoi', 'Diem']]
                    for _, row in top_questions.iterrows():
                        fact_ketqua_list.append((row['SubmissionID'], row['MaCauHoi'], row['Diem']))
                else:
                    for _, row in sub_data.iterrows():
                        fact_ketqua_list.append((row['SubmissionID'], row['MaCauHoi'], row['Diem']))
            else:
                for _, row in sub_data.iterrows():
                    fact_ketqua_list.append((row['SubmissionID'], row['MaCauHoi'], row['Diem']))
                missing_questions = set(range(1, 13)) - existing_questions
                for q in missing_questions:
                    fact_ketqua_list.append((sub_id, q, 5))
        
        fact_ketqua_df = pd.DataFrame(fact_ketqua_list, columns=['SubmissionID', 'MaCauHoi', 'Diem'])
    else:
        fact_ketqua_df = pd.DataFrame()
    
    print(f"  ✅ Tiền xử lý xong ({time.time()-start:.2f}s)")
    
    # Đóng gói tất cả dữ liệu
    preprocessed_data = {
        'metadata': {
            'semester': SEMESTER,
            'survey_file': SURVEY_FILE,
            'file_name': FILE_NAME,
            'ma_hoc_ky': ma_hoc_ky,
            'nam_hoc': nam_hoc,
            'hoc_ky': hoc_ky,
            'timestamp': datetime.now().isoformat()
        },
        'dim_khoa': dim_khoa,
        'dim_nganh': dim_nganh_df,
        'dim_chuyen_nganh': dim_chuyennganh_df,
        'dim_hoc_phan': dim_hocphan_df,
        'dim_giang_vien': dim_giangvien_df,
        'dim_hoc_ky': dim_hocky_df,
        'dim_lop_sinh_vien': dim_lopsv_df,
        'dim_sinh_vien': dim_sinhvien_df,
        'dim_lop_hoc_phan': dim_lophp_df,
        'fact_gop_y_tu_luan': fact_main_df,
        'fact_ket_qua_danh_gia': fact_ketqua_df,
        'df_raw': df_raw  # Giữ raw data để tham khảo
    }
    
    return preprocessed_data


# ================= MAIN PREPROCESSING JOB =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 JOB 1: TIỀN XỬ LÝ DỮ LIỆU")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
    print(f"WORKERS: {NUM_WORKERS}")
    print("=" * 60)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    # 2. Đọc dữ liệu master
    print("\n📥 2. Đọc dữ liệu master...")
    hp_master = load_hp_master(blob_service)
    dim_nganh, dim_chuyennganh, mapping = load_chuyennganh_master(blob_service)
    print(f"  ✅ HP-Khoa: {len(hp_master)} dòng")
    print(f"  ✅ DIM_NGANH: {len(dim_nganh)} dòng")
    print(f"  ✅ DIM_CHUYEN_NGANH: {len(dim_chuyennganh)} dòng")
    
    # 3. Đọc dữ liệu survey
    print(f"\n📥 3. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    # 4. Parse dữ liệu
    print("\n📝 4. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    print(f"  ✅ Parse: {len(df_raw):,} dòng câu trả lời trong {parse_time:.1f}s")
    
    # 5. Tiền xử lý dữ liệu
    print("\n🔧 5. Tiền xử lý dữ liệu...")
    preprocess_start = time.time()
    preprocessed_data = preprocess_data(df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping)
    preprocess_time = time.time() - preprocess_start
    
    # 6. Lưu dữ liệu đã tiền xử lý
    print("\n💾 6. Lưu dữ liệu đã tiền xử lý...")
    save_preprocessed_data(blob_service, preprocessed_data, f"{FILE_NAME}_preprocessed")
    
    # 7. Thống kê
    total_time = time.time() - total_start
    print("\n📊 7. KẾT QUẢ TIỀN XỬ LÝ:")
    print(f"   - DIM_KHOA: {len(preprocessed_data['dim_khoa']):,} dòng")
    print(f"   - DIM_NGANH: {len(preprocessed_data['dim_nganh']):,} dòng")
    print(f"   - DIM_CHUYEN_NGANH: {len(preprocessed_data['dim_chuyen_nganh']):,} dòng")
    print(f"   - DIM_HOC_PHAN: {len(preprocessed_data['dim_hoc_phan']):,} dòng")
    print(f"   - DIM_GIANG_VIEN: {len(preprocessed_data['dim_giang_vien']):,} dòng")
    print(f"   - DIM_HOC_KY: {len(preprocessed_data['dim_hoc_ky']):,} dòng")
    print(f"   - DIM_LOP_SINH_VIEN: {len(preprocessed_data['dim_lop_sinh_vien']):,} dòng")
    print(f"   - DIM_SINH_VIEN: {len(preprocessed_data['dim_sinh_vien']):,} dòng")
    print(f"   - DIM_LOP_HOC_PHAN: {len(preprocessed_data['dim_lop_hoc_phan']):,} dòng")
    print(f"   - FACT_GOP_Y_TU_LUAN: {len(preprocessed_data['fact_gop_y_tu_luan']):,} dòng")
    print(f"   - FACT_KET_QUA_DANH_GIA: {len(preprocessed_data['fact_ket_qua_danh_gia']):,} dòng")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH TIỀN XỬ LÝ! Thời gian: {total_time:.1f}s")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Preprocess: {preprocess_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
