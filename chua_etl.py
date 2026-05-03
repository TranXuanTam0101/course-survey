import os
import sys
import re
import io
import time
import pickle
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
import warnings
warnings.filterwarnings('ignore')

# ================= CONFIG TỐI ƯU =================
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
PREPROCESSED_PATH = "preprocessed-data"

# Tối ưu số lượng worker
NUM_WORKERS = max(2, min(mp.cpu_count(), 6))
CHUNK_SIZE = 200000

# ================= COMPILE REGEX PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# NLP Patterns
_sentiment_keywords = {
    'very_positive': ['tuyệt vời', 'xuất sắc', 'hoàn hảo', 'quá tuyệt', 'siêu', 'rất tốt', 'cực kỳ'],
    'very_negative': ['tệ hại', 'tồi tệ', 'thất vọng', 'quá khó', 'rất chán'],
    'positive': ['tuyệt', 'tốt', 'hay', 'ổn', 'hài lòng', 'cảm ơn', 'ok', 'great', 'excellent', 
                 'thoải mái', 'vui', 'sôi nổi', 'hấp dẫn', 'dễ', 'thân thiện', 'tâm lý', 
                 'tận tâm', 'nhiệt tình', 'chu đáo', 'chi tiết', 'sáng tạo', 'thực tế', 'hiệu quả'],
    'negative': ['tệ', 'kém', 'dở', 'chán', 'khó', 'mông lung', 'lan man', 'dài dòng', 'qua loa',
                 'chắp vá', 'đọc chép', 'cứng nhắc', 'đơn điệu', 'thiếu', 'cũ kỹ', 'nhanh', 'lố giờ']
}

_sentiment_very_pos = re.compile('|'.join(_sentiment_keywords['very_positive']))
_sentiment_very_neg = re.compile('|'.join(_sentiment_keywords['very_negative']))
_sentiment_pos = re.compile('|'.join(_sentiment_keywords['positive']))
_sentiment_neg = re.compile('|'.join(_sentiment_keywords['negative']))

_tag_hocphan = re.compile(r'(chuẩn đầu ra|mục tiêu|nội dung|chương trình|môn học|trang bị|cung cấp|đào tạo|bám sát|phù hợp|rõ ràng|đầy đủ)', re.I)
_tag_dayhoc = re.compile(r'(giảng viên|thầy|cô|tận tâm|nhiệt tình|truyền cảm hứng|dạy|giảng|bài giảng|sinh động|linh hoạt|tương tác|dễ hiểu)', re.I)
_tag_kiemtra = re.compile(r'(kiểm tra|đánh giá|công bằng|minh bạch|thi|đề thi|cho điểm|công khai)', re.I)


# ================= HÀM TIỆN ÍCH MỚI =================
def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    return f"HK{hoc_ky}_{nam_bat_dau % 100}{nam_ket_thuc % 100}", f"{nam_bat_dau}-{nam_ket_thuc}", hoc_ky


def create_ma_khoa_new(ten_khoa: str, khoa_counter: dict) -> str:
    """Tạo mã khoa tự sinh: KHOA_001, KHOA_002, ..."""
    # Xử lý đặc biệt cho Ngữ Văn - Truyền thông và Toán - Tin
    special_names = ['ngữ văn - truyền thông', 'toán - tin', 'ngữ văn', 'truyền thông', 'toán', 'tin']
    ten_lower = ten_khoa.lower().strip()
    
    for special in special_names:
        if special in ten_lower:
            return 'TĐHSP'  # Trường ĐHSP
    
    # Tạo mã mới nếu chưa có
    if ten_khoa not in khoa_counter:
        khoa_counter[ten_khoa] = f"KHOA_{len(khoa_counter) + 1:03d}"
    
    return khoa_counter[ten_khoa]


def create_ma_nganh_new(ten_nganh: str, nganh_counter: dict) -> str:
    """Tạo mã ngành tự sinh: NGANH_001, NGANH_002, ..."""
    if ten_nganh not in nganh_counter:
        nganh_counter[ten_nganh] = f"NGANH_{len(nganh_counter) + 1:03d}"
    
    return nganh_counter[ten_nganh]


def create_ma_chuyen_nganh_new(ten_chuyen_nganh: str, chuyen_nganh_counter: dict) -> str:
    """Tạo mã chuyên ngành tự sinh: CN_001, CN_002, ..."""
    if ten_chuyen_nganh not in chuyen_nganh_counter:
        chuyen_nganh_counter[ten_chuyen_nganh] = f"CN_{len(chuyen_nganh_counter) + 1:03d}"
    
    return chuyen_nganh_counter[ten_chuyen_nganh]


def determine_ma_chuyen_nganh_new(lop: str, chuyen_nganh_counter: dict) -> str:
    """Xác định MaChuyenNganh từ Lop - tạo mới nếu chưa có"""
    if not isinstance(lop, str):
        return None
    
    lop_upper = lop.upper().strip()
    
    # Các trường hợp đặc biệt
    if 'CTS' in lop_upper:
        return "NULL_CTS"
    if 'QT' in lop_upper:
        return "NULL_QT"
    
    # Tạo mã mới cho lớp
    if lop not in chuyen_nganh_counter:
        chuyen_nganh_counter[lop] = f"CN_{len(chuyen_nganh_counter) + 1:03d}"
    
    return chuyen_nganh_counter[lop]


# ================= NLP FUNCTIONS =================
def analyze_sentiment_ultra_fast(text: str) -> str:
    if not isinstance(text, str) or len(text) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    if _sentiment_very_pos.search(text_lower):
        return 'positive'
    if _sentiment_very_neg.search(text_lower):
        return 'negative'
    
    pos_cnt = len(_sentiment_pos.findall(text_lower))
    neg_cnt = len(_sentiment_neg.findall(text_lower))
    
    return 'positive' if pos_cnt > neg_cnt else ('negative' if neg_cnt > pos_cnt else 'neutral')


def extract_tags_ultra_fast(text: str) -> tuple:
    if not isinstance(text, str):
        return (0, 0, 0, 1)
    
    text_lower = text.lower()
    tag_hp = 1 if _tag_hocphan.search(text_lower) else 0
    tag_dh = 1 if _tag_dayhoc.search(text_lower) else 0
    tag_kt = 1 if _tag_kiemtra.search(text_lower) else 0
    
    return (tag_hp, tag_dh, tag_kt, 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0)


def process_nlp_batch_ultra_fast(df):
    if df.empty:
        return df
    
    texts = df['NoiDungGopY'].fillna('').astype(str).values
    
    sentiments = [analyze_sentiment_ultra_fast(t) for t in texts]
    tags = [extract_tags_ultra_fast(t) for t in texts]
    
    df['Sentiment'] = sentiments
    df['Tag_HocPhan'] = [t[0] for t in tags]
    df['Tag_DayHoc'] = [t[1] for t in tags]
    df['Tag_KiemTra'] = [t[2] for t in tags]
    df['Tag_Khac'] = [t[3] for t in tags]
    df['Is_Valid'] = 1
    
    return df


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
    path = f"{PREPROCESSED_PATH}/{filename}.pkl"
    try:
        pickled_data = pickle.dumps(data_dict, protocol=pickle.HIGHEST_PROTOCOL)
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(path)
        blob.upload_blob(pickled_data, overwrite=True)
        print(f"  ✅ Đã lưu: {path}")
        return True
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
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
    return df


def load_chuyennganh_master(blob_service):
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if not content:
        return pd.DataFrame()
    
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame()
    
    return df_clean.drop_duplicates(subset=['MaChuyenNganh'])


# ================= PARSE SURVEY DATA =================
def parse_line_ultra_fast(line):
    if not line or len(line) < 50:
        return None
    
    parts = line.strip().split(',')
    n = len(parts)
    if n < 15:
        return None
    
    # Tìm ngày sinh
    ngay_sinh_idx = -1
    ngay_sinh = ''
    for i in range(2, min(n, 12)):
        val = parts[i].strip()
        if val and len(val) == 10 and val[2] == '/' and val[5] == '/':
            ngay_sinh = val
            ngay_sinh_idx = i
            break
    
    if ngay_sinh_idx == -1:
        return None
    
    # Lấy tên
    ho_dem = ''
    ten = parts[ngay_sinh_idx - 1].strip() if ngay_sinh_idx > 1 else ''
    if ngay_sinh_idx > 2:
        ho_dem = ' '.join(parts[2:ngay_sinh_idx-1]).strip()
    
    # Lấy MaHP
    ma_hp = parts[ngay_sinh_idx + 1].strip() if ngay_sinh_idx + 1 < n else ''
    
    # Tìm mã GV
    ma_gv_idx = -1
    ma_gv = ''
    start_idx = ngay_sinh_idx + 2
    end_idx = min(n, start_idx + 23)
    for i in range(start_idx, end_idx):
        val = parts[i].strip()
        if val and ((len(val) == 7 and val.isdigit()) or (len(val) == 7 and val.startswith('TG')) or val == 'gvDacThu_TKTH'):
            ma_gv = val
            ma_gv_idx = i
            break
    
    if ma_gv_idx == -1:
        ma_gv_idx = n - 4 if n >= 4 else start_idx
    
    # Tên HP
    ten_hp = ' '.join(parts[ngay_sinh_idx + 2:ma_gv_idx]).strip()
    
    # Tên GV
    ho_dem_gv = parts[ma_gv_idx + 1].strip() if ma_gv_idx + 1 < n else ''
    ten_gv = parts[ma_gv_idx + 2].strip() if ma_gv_idx + 2 < n else ''
    lop_hp = parts[ma_gv_idx + 3].strip() if ma_gv_idx + 3 < n else ''
    cau_hoi = parts[ma_gv_idx + 4].strip() if ma_gv_idx + 4 < n else ''
    gia_tri = parts[ma_gv_idx + 5].strip() if ma_gv_idx + 5 < n else ''
    
    # Tìm NULL
    null_idx = -1
    for i in range(ma_gv_idx + 6, min(n, ma_gv_idx + 20)):
        if parts[i].strip().upper() == 'NULL' or parts[i].strip() == '':
            null_idx = i
            break
    
    essay_text = ''
    if null_idx != -1 and null_idx + 1 < n:
        essay_text = ','.join(parts[null_idx + 1:]).strip()
    
    return (
        f"{parts[1].strip()}_{lop_hp}_{ma_gv}_{FILE_NAME}",
        parts[0].strip(),
        parts[1].strip(),
        ho_dem,
        ten,
        ngay_sinh,
        ma_hp,
        ten_hp,
        ma_gv,
        ho_dem_gv,
        ten_gv,
        lop_hp,
        cau_hoi,
        gia_tri,
        essay_text
    )


def parse_batch_worker(lines_batch):
    results = []
    for line in lines_batch:
        result = parse_line_ultra_fast(line)
        if result:
            results.append(result)
    return results


def parse_survey_ultra_fast(content: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = content.strip().split('\n')
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batch_size = max(20000, len(lines) // NUM_WORKERS)
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    
    all_rows = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_batch_worker, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    columns = ['SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
               'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
               'CauHoi', 'GiaTri', 'EssayText']
    
    df = pd.DataFrame(all_rows, columns=columns)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.2f}s)")
    return df


# ================= TẠO DIMENSIONS MỚI (TỰ SINH MÃ) =================
def create_dimensions_new(df_raw, hp_master, chuyennganh_master):
    print("  -> Tạo dimension tables (tự sinh mã)...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # Counter cho các mã tự sinh
    khoa_counter = {}
    nganh_counter = {}
    chuyen_nganh_counter = {}
    
    # 1. DIM_KHOA - Tự sinh mã
    khoa_list = []
    khoa_list.append(('TĐHKT', 'Trường ĐH Kinh tế'))
    khoa_list.append(('PĐT', 'Phòng Đào Tạo'))
    
    # Lấy danh sách khoa từ hp_master
    if not hp_master.empty:
        for ten_khoa in hp_master['TenKhoa'].drop_duplicates().values:
            ma_khoa = create_ma_khoa_new(ten_khoa, khoa_counter)
            khoa_list.append((ma_khoa, ten_khoa))
    
    dim_khoa = pd.DataFrame(khoa_list, columns=['MaKhoa', 'TenKhoa']).drop_duplicates('MaKhoa')
    
    # 2. DIM_NGANH - Tự sinh mã
    default_nganh = [
        ('NULL_CTS', 'Ngành NULL_CTS', 'TĐHKT'),
        ('NULL_QT', 'Ngành NULL_QT', 'PĐT')
    ]
    dim_nganh_list = list(default_nganh)
    
    # Lấy danh sách ngành từ chuyennganh_master
    if not chuyennganh_master.empty:
        for ten_nganh in chuyennganh_master['TenNganh'].drop_duplicates().values:
            ma_nganh = create_ma_nganh_new(ten_nganh, nganh_counter)
            # Tìm mã khoa tương ứng
            sample_row = chuyennganh_master[chuyennganh_master['TenNganh'] == ten_nganh].iloc[0]
            ten_khoa = sample_row['TenKhoa']
            # Tìm mã khoa từ dim_khoa
            ma_khoa = dim_khoa[dim_khoa['TenKhoa'] == ten_khoa]['MaKhoa'].values
            ma_khoa = ma_khoa[0] if len(ma_khoa) > 0 else 'TĐHKT'
            dim_nganh_list.append((ma_nganh, ten_nganh, ma_khoa))
    
    dim_nganh = pd.DataFrame(dim_nganh_list, columns=['MaNganh', 'TenNganh', 'MaKhoa']).drop_duplicates('MaNganh')
    
    # 3. DIM_CHUYEN_NGANH - Tự sinh mã
    default_chuyennganh = [
        ('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
        ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')
    ]
    dim_chuyennganh_list = list(default_chuyennganh)
    
    # Lấy danh sách chuyên ngành từ chuyennganh_master
    if not chuyennganh_master.empty:
        for _, row in chuyennganh_master.iterrows():
            ten_chuyen = row['TenChuyenNganh']
            ma_chuyen = create_ma_chuyen_nganh_new(ten_chuyen, chuyen_nganh_counter)
            ten_nganh = row['TenNganh']
            # Tìm mã ngành từ dim_nganh
            ma_nganh = dim_nganh[dim_nganh['TenNganh'] == ten_nganh]['MaNganh'].values
            ma_nganh = ma_nganh[0] if len(ma_nganh) > 0 else 'NULL_CTS'
            dim_chuyennganh_list.append((ma_chuyen, ten_chuyen, ma_nganh))
    
    dim_chuyen_nganh = pd.DataFrame(dim_chuyennganh_list, columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']).drop_duplicates('MaChuyenNganh')
    
    # 4. DIM_HOC_PHAN
    hp_list = []
    hp_dict = {}
    if not hp_master.empty:
        hp_master_unique = hp_master.drop_duplicates(subset=['MaHP'], keep='first')
        # Tạo mapping từ MaHP cũ sang thông tin
        for _, row in hp_master_unique.iterrows():
            hp_dict[row['MaHP']] = (row['TenHP'], row['TenKhoa'])
    
    df_hp_raw = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP'])
    for _, row in df_hp_raw.iterrows():
        ma_hp = row['MaHP']
        if ma_hp in hp_dict:
            ten_hp, ten_khoa = hp_dict[ma_hp]
            # Tìm mã khoa
            ma_khoa = dim_khoa[dim_khoa['TenKhoa'] == ten_khoa]['MaKhoa'].values
            ma_khoa = ma_khoa[0] if len(ma_khoa) > 0 else 'TĐHKT'
            hp_list.append((ma_hp, ten_hp, ma_khoa))
        else:
            ten_hp = row['TenHP'] if pd.notna(row['TenHP']) else f"Học phần {ma_hp}"
            hp_list.append((ma_hp, ten_hp, 'TĐHKT'))
    
    dim_hoc_phan = pd.DataFrame(hp_list, columns=['MaHP', 'TenHP', 'MaKhoa']).drop_duplicates('MaHP')
    
    # 5. DIM_GIANG_VIEN
    dim_giang_vien = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV']).copy()
    
    # 6. DIM_HOC_KY
    dim_hoc_ky = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # 7. DIM_LOP_SINH_VIEN - Sử dụng counter mới
    valid_cn = set(dim_chuyen_nganh['MaChuyenNganh'].values)
    lop_counter = {}
    lop_list = []
    for lop in df_raw['Lop'].drop_duplicates().dropna().values:
        ma_cn = determine_ma_chuyen_nganh_new(lop, lop_counter)
        if ma_cn in valid_cn or ma_cn in ['NULL_CTS', 'NULL_QT']:
            lop_list.append((lop, lop, ma_cn))
    
    dim_lop_sinh_vien = pd.DataFrame(lop_list, columns=['MaLop', 'Lop', 'MaChuyenNganh'])
    
    # 8. DIM_SINH_VIEN
    valid_lop = set(dim_lop_sinh_vien['MaLop'].values)
    sv_list = []
    for ma_sv, ho_dem, ten, ngay_sinh, lop in df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV']).values:
        if lop in valid_lop:
            try:
                ngay_sinh_dt = datetime.strptime(ngay_sinh, '%d/%m/%Y').date() if ngay_sinh else None
            except:
                ngay_sinh_dt = None
            sv_list.append((ma_sv, ho_dem or '', ten or '', ngay_sinh_dt, lop))
    
    dim_sinh_vien = pd.DataFrame(sv_list, columns=['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'])
    
    # 9. DIM_LOP_HOC_PHAN
    valid_hp = set(dim_hoc_phan['MaHP'].values)
    valid_gv = set(dim_giang_vien['MaGV'].values)
    lhp_list = [(lop_hp, lop_hp, ma_hp, ma_gv, ma_hoc_ky)
                for lop_hp, ma_hp, ma_gv in df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP']).values
                if ma_hp in valid_hp and ma_gv in valid_gv]
    
    dim_lop_hoc_phan = pd.DataFrame(lhp_list, columns=['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'])
    
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


# ================= TRANSFORM & NLP =================
def transform_with_nlp_fast(df_raw: pd.DataFrame) -> tuple:
    print("  -> Transform dữ liệu & NLP...")
    start = time.time()
    
    # XỬ LÝ TỰ LUẬN
    text_mask = df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')
    text_df = df_raw[text_mask].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip().str[:4000]
        
        print(f"      -> NLP cho {len(text_df_unique):,} bài...")
        nlp_start = time.time()
        
        text_df_unique = process_nlp_batch_ultra_fast(text_df_unique)
        
        print(f"      -> NLP xong ({time.time()-nlp_start:.2f}s)")
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].copy()
    
    # XỬ LÝ TRẮC NGHIỆM
    mcq_mask = (df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
                df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != ''))
    mcq_df = df_raw[mcq_mask].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
        def complete_questions(g):
            qs = set(g['MaCauHoi'])
            if len(qs) >= 12:
                return g.nlargest(12, 'Diem')[['SubmissionID', 'MaCauHoi', 'Diem']] if len(qs) > 12 else g[['SubmissionID', 'MaCauHoi', 'Diem']]
            missing = set(range(1, 13)) - qs
            return pd.concat([g[['SubmissionID', 'MaCauHoi', 'Diem']], 
                             pd.DataFrame({'SubmissionID': [g.name] * len(missing), 'MaCauHoi': list(missing), 'Diem': [5] * len(missing)})])
        
        # Loại bỏ duplicate trong fact_ketqua trước khi lưu
        fact_ketqua = mcq_df.groupby('SubmissionID', group_keys=False).apply(complete_questions).reset_index(drop=True)
        
        # Loại bỏ duplicate key (SubmissionID, MaCauHoi) nếu có
        fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
        
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng (đã loại duplicate)")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform & NLP xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 JOB 1: TIỀN XỬ LÝ SIÊU TỐC (TỰ SINH MÃ KHOA, NGÀNH)")
    print("=" * 70)
    print(f"📂 File: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ Workers: {NUM_WORKERS}")
    print("=" * 70)
    print("\n📌 LOGIC MÃ TỰ SINH:")
    print("   - Mã Khoa: KHOA_001, KHOA_002, ...")
    print("   - Mã Ngành: NGANH_001, NGANH_002, ...")
    print("   - Mã Chuyên ngành: CN_001, CN_002, ...")
    print("   - Đặc biệt: Ngữ Văn - Truyền thông và Toán - Tin -> TĐHSP")
    print("=" * 70)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    # 2. Đọc master data
    print("\n📥 2. Đọc master data...")
    hp_master = load_hp_master(blob_service)
    chuyennganh_master = load_chuyennganh_master(blob_service)
    print(f"  ✅ HP-Khoa: {len(hp_master):,} rows")
    print(f"  ✅ Chuyên ngành master: {len(chuyennganh_master):,} rows")
    
    # 3. Đọc survey
    print(f"\n📥 3. Đọc survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file!")
        return
    print(f"  ✅ Dung lượng: {len(survey_content):,} bytes")
    
    # 4. Parse
    print("\n📝 4. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_ultra_fast(survey_content)
    parse_time = time.time() - parse_start
    print(f"  ✅ Parse: {len(df_raw):,} rows ({parse_time:.1f}s)")
    
    # 5. Tạo dimensions
    print("\n🏗️ 5. Tạo dimensions...")
    dims = create_dimensions_new(df_raw, hp_master, chuyennganh_master)
    
    # 6. Transform & NLP
    print("\n🔄 6. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_with_nlp_fast(df_raw)
    transform_time = time.time() - transform_start
    
    # 7. Lưu preprocessed data
    print("\n💾 7. Lưu preprocessed data...")
    preprocessed_data = {
        'metadata': {
            'semester': SEMESTER,
            'survey_file': SURVEY_FILE,
            'timestamp': datetime.now().isoformat(),
            'ma_hoc_ky': dims['ma_hoc_ky']
        },
        **dims,
        'fact_gop_y_tu_luan': fact_main,
        'fact_ket_qua_danh_gia': fact_ketqua
    }
    
    save_preprocessed_data(blob_service, preprocessed_data, f"{FILE_NAME}_preprocessed")
    
    # 8. Thống kê
    total_time = time.time() - total_start
    print("\n" + "=" * 70)
    print("📊 KẾT QUẢ:")
    print(f"   ✅ Parse: {len(df_raw):,} rows ({parse_time:.1f}s)")
    print(f"   ✅ Transform & NLP: {transform_time:.1f}s")
    print(f"   ✅ FACT_GOP_Y_TU_LUAN: {len(fact_main):,} rows")
    print(f"   ✅ FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} rows")
    
    if not fact_main.empty:
        print(f"\n   📌 Sentiment distribution:")
        for sent, cnt in fact_main['Sentiment'].value_counts().items():
            print(f"      - {sent}: {cnt:,} ({cnt/len(fact_main)*100:.1f}%)")
        
        print(f"\n   📌 Tag distribution:")
        print(f"      - Học phần: {fact_main['Tag_HocPhan'].sum():,}")
        print(f"      - Dạy học: {fact_main['Tag_DayHoc'].sum():,}")
        print(f"      - Kiểm tra: {fact_main['Tag_KiemTra'].sum():,}")
        print(f"      - Khác: {fact_main['Tag_Khac'].sum():,}")
    
    print(f"\n   Dimensions:")
    for name, df in dims.items():
        if name != 'ma_hoc_ky' and not df.empty:
            print(f"      - {name}: {len(df):,} rows")
    
    print(f"\n⏱️ Tổng thời gian: {total_time:.1f}s")
    print(f"🚀 Tốc độ: {len(df_raw)/total_time:,.0f} rows/s")
    print("=" * 70)
    print("✅ DỮ LIỆU ĐÃ SẴN SÀNG CHO JOB 2!")
    print("=" * 70)


if __name__ == "__main__":
    main()
