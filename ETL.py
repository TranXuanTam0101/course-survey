import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import warnings
warnings.filterwarnings('ignore')

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

# Số lượng worker - GIẢM XUỐNG để tránh overhead
NUM_WORKERS = max(2, min(mp.cpu_count(), 4))  # Giới hạn tối đa 4 workers
CHUNK_SIZE = 100000  # Tăng chunk size

# ================= COMPILE REGEX PATTERNS (CHỈ 1 LẦN) =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')


# ================= HÀM TIỆN ÍCH TỐI ƯU =================
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
        'bộ môn nncn': 'BNNNCN', 'trường đhnn': 'TĐHNN', 'luật': 'LUAT',
        'marketing': 'MKT', 'trường đhkt': 'TĐHKT', 'phòng đào tạo': 'PĐT'
    }
    ten_lower = ten_khoa.lower()
    for special_name, special_code in SPECIAL_MA_KHOA.items():
        if special_name in ten_lower:
            return special_code
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def determine_ma_chuyen_nganh(lop: str) -> str:
    """Tối ưu: chỉ trả về ma_chuyen_nganh"""
    if not isinstance(lop, str):
        return None
    
    lop_upper = lop.upper().strip()
    
    if 'CTS' in lop_upper:
        return "NULL_CTS"
    if 'QT' in lop_upper:
        return "NULL_QT"
    if 'ACCA' in lop_upper:
        match = re.search(r'K(\d{2})', lop_upper)
        if match:
            return f"K{match.group(1)}-ACCA"
    match = re.search(r'K(\d{2})', lop_upper)
    if match:
        return f"K{match.group(1)}"
    return None


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


def save_processed(blob_service, df, filename):
    path = f"{PROCESSED_PATH}/{filename}"
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    try:
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(path)
        blob.upload_blob(csv_data, overwrite=True)
        print(f"  ✅ Đã lưu: {path}")
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
        return pd.DataFrame(), {}
    
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame(), {}
    
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean = df_clean[df_clean['MaChuyenNganh'].astype(str).str.strip() != '']
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(
        lambda x: ''.join([w[0].upper() for w in re.split(r'[\s\-]+', str(x).strip()) if w and w[0].isalpha()]) or "UNKNOWN"
    )
    df_clean = df_clean.drop_duplicates(subset=['MaChuyenNganh'])
    
    return df_clean, None


# ================= PARSE SURVEY DATA - TỐI ƯU HÓA =================
def parse_line_fast(line):
    """Parse một dòng CSV - KHÔNG dùng try-except trong vòng lặp"""
    if not line or not line.strip():
        return None
    
    row = line.strip().split(',')
    row_len = len(row)
    if row_len < 15:
        return None
    
    # Lấy các giá trị cơ bản
    lop = row[0]
    ma_sv = row[1]
    
    # Tìm ngày sinh (tối ưu: dùng vòng lặp đơn giản)
    ngay_sinh = ''
    ngay_sinh_index = -1
    for i in range(2, min(row_len, 12)):
        val = row[i].strip()
        if _date_pattern.match(val):
            ngay_sinh = val
            ngay_sinh_index = i
            break
    
    if ngay_sinh_index == -1:
        return None
    
    # Xử lý tên
    ho_dem = ''
    ten = ''
    if ngay_sinh_index > 1:
        name_parts = [row[j].strip() for j in range(2, ngay_sinh_index) if row[j].strip()]
        if name_parts:
            ten = name_parts[-1]
            ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
    
    # Lấy MaHP
    ma_hp = row[ngay_sinh_index + 1].strip() if ngay_sinh_index + 1 < row_len else ''
    
    # Tìm mã giảng viên
    ma_gv = ''
    ma_gv_index = -1
    for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
        val = row[i].strip()
        if _ma_gv_pattern.match(val):
            ma_gv = val
            ma_gv_index = i
            break
    
    if ma_gv_index == -1:
        ma_gv_index = row_len - 4 if row_len >= 4 else ngay_sinh_index + 2
    
    # Tên học phần
    ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]).strip()
    
    # Tên giảng viên
    ho_dem_gv = row[ma_gv_index + 1].strip() if ma_gv_index + 1 < row_len else ''
    ten_gv = row[ma_gv_index + 2].strip() if ma_gv_index + 2 < row_len else ''
    lop_hp = row[ma_gv_index + 3].strip() if ma_gv_index + 3 < row_len else ''
    cau_hoi = row[ma_gv_index + 4].strip() if ma_gv_index + 4 < row_len else ''
    gia_tri = row[ma_gv_index + 5].strip() if ma_gv_index + 5 < row_len else ''
    
    # Tìm NULL
    null_index = -1
    for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
        if row[i].strip().upper() == 'NULL' or row[i].strip() == '':
            null_index = i
            break
    
    essay_text = ''
    if null_index != -1 and null_index + 1 < row_len:
        essay_text = ','.join(row[null_index + 1:]).strip()
    
    # Tạo SubmissionID
    submission_id = f"{ma_sv}_{lop_hp}_{ma_gv}_{FILE_NAME}"
    
    return {
        'SubmissionID': submission_id,
        'Lop': lop.strip(),
        'MaSV': ma_sv.strip(),
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
    }


def parse_lines_batch_optimized(lines_batch):
    """Xử lý batch với list comprehension"""
    results = []
    for line in lines_batch:
        result = parse_line_fast(line)
        if result:
            results.append(result)
    return results


def parse_survey_to_long_format(content: str) -> pd.DataFrame:
    """Parse CSV tối ưu với chunk lớn hơn"""
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    # Chia batch nhỏ hơn số workers
    batch_size = max(10000, len(lines) // NUM_WORKERS)
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    
    all_rows = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch_optimized, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    df = pd.DataFrame(all_rows)
    print(f"  -> Đã parse {len(df):,} dòng câu trả lời ({time.time()-start:.2f}s)")
    return df


# ================= TRANSFORM TỐI ƯU =================
def transform_data_optimized(df_raw: pd.DataFrame) -> tuple:
    """Transform dữ liệu với pandas vectorized operations"""
    print("  -> Transform dữ liệu...")
    start = time.time()
    
    # ===== 1. XỬ LÝ CÂU TỰ LUẬN =====
    text_mask = df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')
    text_df = df_raw[text_mask].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        # Loại bỏ trùng lặp
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        
        # Làm sạch text
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip().str[:4000]
        text_df_unique['Is_Valid'] = 1
        
        # Tags mặc định
        text_df_unique['Sentiment'] = 'neutral'
        text_df_unique['Tag_HocPhan'] = 0
        text_df_unique['Tag_DayHoc'] = 0
        text_df_unique['Tag_KiemTra'] = 0
        text_df_unique['Tag_Khac'] = 1
        
        fact_main = text_df_unique[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid',
            'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].copy()
    
    # ===== 2. XỬ LÝ CÂU TRẮC NGHIỆM =====
    mcq_mask = (df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
                df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != ''))
    mcq_df = df_raw[mcq_mask].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
        # Tạo đủ 12 câu cho mỗi submission - TỐI ƯU với groupby
        def process_submission(group):
            existing = set(group['MaCauHoi'].values)
            if len(existing) >= 12:
                if len(existing) > 12:
                    return group.nlargest(12, 'Diem')[['SubmissionID', 'MaCauHoi', 'Diem']]
                return group[['SubmissionID', 'MaCauHoi', 'Diem']]
            else:
                missing = set(range(1, 13)) - existing
                missing_data = pd.DataFrame({
                    'SubmissionID': [group.name] * len(missing),
                    'MaCauHoi': list(missing),
                    'Diem': [5] * len(missing)
                })
                return pd.concat([group[['SubmissionID', 'MaCauHoi', 'Diem']], missing_data])
        
        # Group by SubmissionID và apply
        fact_ketqua = mcq_df.groupby('SubmissionID', group_keys=False).apply(process_submission).reset_index(drop=True)
        
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= TẠO DIMENSIONS =================
def create_dimensions_optimized(df_raw: pd.DataFrame, hp_master, chuyennganh_master) -> dict:
    """Tạo các dimension tables từ dữ liệu"""
    print("  -> Tạo dimension tables...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # 1. DIM_KHOA
    khoa_list = []
    if not hp_master.empty:
        khoa_list.extend(hp_master[['MaKhoa', 'TenKhoa']].drop_duplicates().values.tolist())
    
    default_khoa = [('TĐHKT', 'Trường ĐH Kinh tế'), ('PĐT', 'Phòng Đào Tạo')]
    khoa_dict = dict(default_khoa)
    for ma, ten in khoa_list:
        khoa_dict[ma] = ten
    
    dim_khoa = pd.DataFrame([(ma, ten) for ma, ten in khoa_dict.items()], columns=['MaKhoa', 'TenKhoa'])
    
    # 2. DIM_NGANH và DIM_CHUYEN_NGANH
    default_nganh = [
        ('NULL_CTS', 'Ngành NULL_CTS', 'TĐHKT'),
        ('NULL_QT', 'Ngành NULL_QT', 'PĐT')
    ]
    
    dim_nganh_list = [(ma, ten, makhoa) for ma, ten, makhoa in default_nganh]
    dim_chuyennganh_list = [
        ('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
        ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')
    ]
    
    if not chuyennganh_master.empty:
        for _, row in chuyennganh_master.iterrows():
            dim_nganh_list.append((row['MaNganh'], row['TenNganh'], row['MaKhoa']))
            dim_chuyennganh_list.append((row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']))
    
    dim_nganh = pd.DataFrame(dim_nganh_list, columns=['MaNganh', 'TenNganh', 'MaKhoa']).drop_duplicates('MaNganh')
    dim_chuyen_nganh = pd.DataFrame(dim_chuyennganh_list, columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']).drop_duplicates('MaChuyenNganh')
    
    # 3. DIM_HOC_PHAN
    hp_list = []
    hp_dict = {}
    if not hp_master.empty:
        hp_master_unique = hp_master.drop_duplicates(subset=['MaHP'], keep='first')
        hp_dict = hp_master_unique.set_index('MaHP')[['TenHP', 'MaKhoa']].to_dict('index')
    
    df_hp_raw = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP'])
    for _, row in df_hp_raw.iterrows():
        ma_hp = row['MaHP']
        if ma_hp in hp_dict:
            hp_list.append((ma_hp, hp_dict[ma_hp]['TenHP'], hp_dict[ma_hp]['MaKhoa']))
        else:
            ten_hp = row['TenHP'] if pd.notna(row['TenHP']) else f"Học phần {ma_hp}"
            hp_list.append((ma_hp, ten_hp, 'TĐHKT'))
    
    dim_hoc_phan = pd.DataFrame(hp_list, columns=['MaHP', 'TenHP', 'MaKhoa']).drop_duplicates('MaHP')
    
    # 4. DIM_GIANG_VIEN
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    dim_giang_vien = df_gv[['MaGV', 'HoDemGV', 'TenGV']].copy()
    
    # 5. DIM_HOC_KY
    dim_hoc_ky = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # 6. DIM_LOP_SINH_VIEN
    valid_chuyen_nganh = set(dim_chuyen_nganh['MaChuyenNganh'].values)
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    
    lop_list = []
    for lop in df_lop_unique['Lop'].values:
        ma_cn = determine_ma_chuyen_nganh(lop)
        if ma_cn and ma_cn in valid_chuyen_nganh:
            lop_list.append((lop, lop, ma_cn))
    
    dim_lop_sinh_vien = pd.DataFrame(lop_list, columns=['MaLop', 'Lop', 'MaChuyenNganh']).drop_duplicates('MaLop')
    
    # 7. DIM_SINH_VIEN
    valid_lop = set(dim_lop_sinh_vien['MaLop'].values)
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    
    sv_list = []
    for _, row in df_sv.iterrows():
        if row['Lop'] in valid_lop:
            ngay_sinh = None
            if row['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            sv_list.append((row['MaSV'], row['HoDem'] or '', row['Ten'] or '', ngay_sinh, row['Lop']))
    
    dim_sinh_vien = pd.DataFrame(sv_list, columns=['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']).drop_duplicates('MaSV')
    
    # 8. DIM_LOP_HOC_PHAN
    valid_hp = set(dim_hoc_phan['MaHP'].values)
    valid_gv = set(dim_giang_vien['MaGV'].values)
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    
    lhp_list = []
    for _, row in df_lhp.iterrows():
        if row['MaHP'] in valid_hp and row['MaGV'] in valid_gv:
            lhp_list.append((row['LopHP'], row['LopHP'], row['MaHP'], row['MaGV'], ma_hoc_ky))
    
    dim_lop_hoc_phan = pd.DataFrame(lhp_list, columns=['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy']).drop_duplicates('MaLopHP')
    
    print(f"  ✅ Tạo dimensions xong ({time.time()-start:.2f}s)")
    
    return {
        'dim_khoa': dim_khoa,
        'dim_nganh': dim_nganh,
        'dim_chuyen_nganh': dim_chuyen_nganh,
        'dim_hoc_phan': dim_hoc_phan,
        'dim_giang_vien': dim_giang_vien,
        'dim_hoc_ky': dim_hoc_ky,
        'dim_lop_sinh_vien': dim_lop_sinh_vien,
        'dim_sinh_vien': dim_sinh_vien,
        'dim_lop_hoc_phan': dim_lop_hoc_phan,
        'ma_hoc_ky': ma_hoc_ky
    }


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - TỐI ƯU HÓA")
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
    chuyennganh_master, _ = load_chuyennganh_master(blob_service)
    print(f"  ✅ HP-Khoa: {len(hp_master)} dòng")
    print(f"  ✅ Chuyên ngành master: {len(chuyennganh_master)} dòng")
    
    # 3. Đọc dữ liệu survey
    print(f"\n📥 3. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    print(f"  ✅ Dung lượng file: {len(survey_content):,} bytes")
    
    # 4. Parse dữ liệu
    print("\n📝 4. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    print(f"  ✅ Parse: {len(df_raw):,} dòng ({parse_time:.1f}s)")
    
    # 5. Tạo dimensions
    print("\n🏗️ 5. Tạo dimension tables...")
    dims = create_dimensions_optimized(df_raw, hp_master, chuyennganh_master)
    
    # 6. Transform data
    print("\n🔄 6. Transform dữ liệu...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_data_optimized(df_raw)
    transform_time = time.time() - transform_start
    
    # 7. Lưu backup
    print("\n💾 7. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    if not fact_ketqua.empty:
        save_processed(blob_service, fact_ketqua, f"{FILE_NAME}_ketqua_{timestamp}.csv")
    
    # 8. Thống kê
    total_time = time.time() - total_start
    print("\n📊 8. KẾT QUẢ:")
    print(f"   - Dòng dữ liệu thô: {len(df_raw):,}")
    print(f"   - Số phiếu tự luận: {len(fact_main):,}")
    print(f"   - Số câu trắc nghiệm: {len(fact_ketqua):,}")
    
    print(f"\n   Dimensions:")
    for name, df in dims.items():
        if name != 'ma_hoc_ky':
            print(f"      - {name}: {len(df):,} dòng")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH! Thời gian: {total_time:.1f}s")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform: {transform_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
