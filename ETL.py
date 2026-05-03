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
NUM_WORKERS = max(2, min(mp.cpu_count(), 6))  # Tăng lên 6 workers
CHUNK_SIZE = 200000  # Tăng chunk size

# ================= COMPILE REGEX PATTERNS (TOÀN CỤC) =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# NLP Patterns - TỐI ƯU HÓA
_sentiment_keywords = {
    'very_positive': ['tuyệt vời', 'xuất sắc', 'hoàn hảo', 'quá tuyệt', 'siêu', 'rất tốt', 'cực kỳ'],
    'very_negative': ['tệ hại', 'tồi tệ', 'thất vọng', 'quá khó', 'rất chán'],
    'positive': ['tuyệt', 'tốt', 'hay', 'ổn', 'hài lòng', 'cảm ơn', 'ok', 'great', 'excellent', 
                 'thoải mái', 'vui', 'sôi nổi', 'hấp dẫn', 'dễ', 'thân thiện', 'tâm lý', 
                 'tận tâm', 'nhiệt tình', 'chu đáo', 'chi tiết', 'sáng tạo', 'thực tế', 'hiệu quả'],
    'negative': ['tệ', 'kém', 'dở', 'chán', 'khó', 'mông lung', 'lan man', 'dài dòng', 'qua loa',
                 'chắp vá', 'đọc chép', 'cứng nhắc', 'đơn điệu', 'thiếu', 'cũ kỹ', 'nhanh', 'lố giờ']
}

# Compile patterns một lần
_sentiment_very_pos = re.compile('|'.join(_sentiment_keywords['very_positive']))
_sentiment_very_neg = re.compile('|'.join(_sentiment_keywords['very_negative']))
_sentiment_pos = re.compile('|'.join(_sentiment_keywords['positive']))
_sentiment_neg = re.compile('|'.join(_sentiment_keywords['negative']))

_tag_hocphan = re.compile(r'(chuẩn đầu ra|mục tiêu|nội dung|chương trình|môn học|trang bị|cung cấp|đào tạo|bám sát|phù hợp|rõ ràng|đầy đủ)', re.I)
_tag_dayhoc = re.compile(r'(giảng viên|thầy|cô|tận tâm|nhiệt tình|truyền cảm hứng|dạy|giảng|bài giảng|sinh động|linh hoạt|tương tác|dễ hiểu)', re.I)
_tag_kiemtra = re.compile(r'(kiểm tra|đánh giá|công bằng|minh bạch|thi|đề thi|cho điểm|công khai)', re.I)


# ================= HÀM TIỆN ÍCH TỐI ƯU =================
def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    return f"HK{hoc_ky}_{nam_bat_dau % 100}{nam_ket_thuc % 100}", f"{nam_bat_dau}-{nam_ket_thuc}", hoc_ky


def create_ma_khoa(ten_khoa: str) -> str:
    SPECIAL = {'bộ môn nncn': 'BNNNCN', 'trường đhnn': 'TĐHNN', 'luật': 'LUAT',
               'marketing': 'MKT', 'trường đhkt': 'TĐHKT', 'phòng đào tạo': 'PĐT'}
    ten_lower = ten_khoa.lower()
    for k, v in SPECIAL.items():
        if k in ten_lower:
            return v
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def determine_ma_chuyen_nganh(lop: str) -> str:
    if not isinstance(lop, str):
        return None
    lop_upper = lop.upper().strip()
    if 'CTS' in lop_upper:
        return "NULL_CTS"
    if 'QT' in lop_upper:
        return "NULL_QT"
    if 'ACCA' in lop_upper:
        match = re.search(r'K(\d{2})', lop_upper)
        return f"K{match.group(1)}-ACCA" if match else None
    match = re.search(r'K(\d{2})', lop_upper)
    return f"K{match.group(1)}" if match else None


# ================= NLP HÀM TỐI ƯU =================
def analyze_sentiment_ultra_fast(text: str) -> str:
    """Phân tích sentiment siêu nhanh"""
    if not isinstance(text, str) or len(text) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    # Kiểm tra nhanh
    if _sentiment_very_pos.search(text_lower):
        return 'positive'
    if _sentiment_very_neg.search(text_lower):
        return 'negative'
    
    # Đếm nhanh
    pos_cnt = len(_sentiment_pos.findall(text_lower))
    neg_cnt = len(_sentiment_neg.findall(text_lower))
    
    return 'positive' if pos_cnt > neg_cnt else ('negative' if neg_cnt > pos_cnt else 'neutral')


def extract_tags_ultra_fast(text: str) -> tuple:
    """Trích xuất tags siêu nhanh"""
    if not isinstance(text, str):
        return (0, 0, 0, 1)
    
    text_lower = text.lower()
    tag_hp = 1 if _tag_hocphan.search(text_lower) else 0
    tag_dh = 1 if _tag_dayhoc.search(text_lower) else 0
    tag_kt = 1 if _tag_kiemtra.search(text_lower) else 0
    
    return (tag_hp, tag_dh, tag_kt, 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0)


def process_nlp_batch_ultra_fast(df):
    """Xử lý NLP batch siêu nhanh"""
    if df.empty:
        return df
    
    texts = df['NoiDungGopY'].fillna('').astype(str).values
    
    # Dùng list comprehension thay vì apply
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
    """Lưu dữ liệu đã tiền xử lý dạng pickle"""
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
    df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
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
    
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean = df_clean[df_clean['MaChuyenNganh'].astype(str).str.strip() != '']
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(
        lambda x: ''.join([w[0].upper() for w in str(x).strip().split() if w and w[0].isalpha()]) or "UNKNOWN"
    )
    
    return df_clean.drop_duplicates(subset=['MaChuyenNganh'])


# ================= PARSE SURVEY DATA - TỐI ƯU CAO =================
def parse_line_ultra_fast(line):
    """Parse một dòng CSV - tối ưu tối đa"""
    if not line or len(line) < 50:
        return None
    
    parts = line.strip().split(',')
    n = len(parts)
    if n < 15:
        return None
    
    # Tìm ngày sinh - tối ưu bằng cách giới hạn vòng lặp
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
        f"{parts[1].strip()}_{lop_hp}_{ma_gv}_{FILE_NAME}",  # SubmissionID
        parts[0].strip(),  # Lop
        parts[1].strip(),  # MaSV
        ho_dem,  # HoDem
        ten,  # Ten
        ngay_sinh,  # NgaySinh
        ma_hp,  # MaHP
        ten_hp,  # TenHP
        ma_gv,  # MaGV
        ho_dem_gv,  # HoDemGV
        ten_gv,  # TenGV
        lop_hp,  # LopHP
        cau_hoi,  # CauHoi
        gia_tri,  # GiaTri
        essay_text  # EssayText
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
    
    # Chia batch
    batch_size = max(20000, len(lines) // NUM_WORKERS)
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    
    all_rows = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_batch_worker, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    # Tạo DataFrame nhanh
    columns = ['SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
               'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
               'CauHoi', 'GiaTri', 'EssayText']
    
    df = pd.DataFrame(all_rows, columns=columns)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.2f}s)")
    return df


# ================= TẠO DIMENSIONS NHANH =================
def create_dimensions_fast(df_raw, hp_master, chuyennganh_master):
    print("  -> Tạo dimension tables...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # DIM_KHOA - Dùng set để unique
    khoa_dict = {'TĐHKT': 'Trường ĐH Kinh tế', 'PĐT': 'Phòng Đào Tạo'}
    if not hp_master.empty:
        for _, row in hp_master[['MaKhoa', 'TenKhoa']].drop_duplicates().iterrows():
            khoa_dict[row['MaKhoa']] = row['TenKhoa']
    
    dim_khoa = pd.DataFrame(list(khoa_dict.items()), columns=['MaKhoa', 'TenKhoa'])
    
    # DIM_NGANH & DIM_CHUYEN_NGANH
    default_nganh = [('NULL_CTS', 'Ngành NULL_CTS', 'TĐHKT'), ('NULL_QT', 'Ngành NULL_QT', 'PĐT')]
    dim_nganh = pd.DataFrame(default_nganh, columns=['MaNganh', 'TenNganh', 'MaKhoa'])
    
    default_chuyennganh = [('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
                           ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')]
    dim_chuyen_nganh = pd.DataFrame(default_chuyennganh, columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'])
    
    if not chuyennganh_master.empty:
        # Lấy unique values
        unique_nganh = chuyennganh_master[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
        unique_chuyennganh = chuyennganh_master[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
        
        dim_nganh = pd.concat([dim_nganh, unique_nganh], ignore_index=True).drop_duplicates('MaNganh')
        dim_chuyen_nganh = pd.concat([dim_chuyen_nganh, unique_chuyennganh], ignore_index=True).drop_duplicates('MaChuyenNganh')
    
    # DIM_HOC_PHAN
    hp_dict = {}
    if not hp_master.empty:
        hp_dict = hp_master.drop_duplicates('MaHP').set_index('MaHP')[['TenHP', 'MaKhoa']].to_dict('index')
    
    hp_list = []
    for ma_hp, ten_hp in df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP']).values:
        if ma_hp in hp_dict:
            hp_list.append((ma_hp, hp_dict[ma_hp]['TenHP'], hp_dict[ma_hp]['MaKhoa']))
        else:
            hp_list.append((ma_hp, ten_hp if pd.notna(ten_hp) else f"Học phần {ma_hp}", 'TĐHKT'))
    
    dim_hoc_phan = pd.DataFrame(hp_list, columns=['MaHP', 'TenHP', 'MaKhoa'])
    
    # DIM_GIANG_VIEN
    dim_giang_vien = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV']).copy()
    
    # DIM_HOC_KY
    dim_hoc_ky = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # DIM_LOP_SINH_VIEN
    valid_cn = set(dim_chuyen_nganh['MaChuyenNganh'].values)
    lop_list = [(lop, lop, determine_ma_chuyen_nganh(lop)) 
                for lop in df_raw['Lop'].drop_duplicates().dropna().values
                if determine_ma_chuyen_nganh(lop) in valid_cn]
    
    dim_lop_sinh_vien = pd.DataFrame(lop_list, columns=['MaLop', 'Lop', 'MaChuyenNganh'])
    
    # DIM_SINH_VIEN
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
    
    # DIM_LOP_HOC_PHAN
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


# ================= TRANSFORM & NLP SIÊU NHANH =================
def transform_with_nlp_fast(df_raw: pd.DataFrame) -> tuple:
    """Transform và xử lý NLP siêu nhanh"""
    print("  -> Transform dữ liệu & NLP...")
    start = time.time()
    
    # XỬ LÝ TỰ LUẬN
    text_mask = df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')
    text_df = df_raw[text_mask].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        # Loại bỏ trùng và làm sạch
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip().str[:4000]
        
        print(f"      -> NLP cho {len(text_df_unique):,} bài...")
        nlp_start = time.time()
        
        # Xử lý NLP
        text_df_unique = process_nlp_batch_ultra_fast(text_df_unique)
        
        print(f"      -> NLP xong ({time.time()-nlp_start:.2f}s)")
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].copy()
    
    # XỬ LÝ TRẮC NGHIỆM - TẠO 12 CÂU
    mcq_mask = (df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
                df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != ''))
    mcq_df = df_raw[mcq_mask].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
        # Tạo đủ 12 câu - dùng groupby nhanh
        def complete_questions(g):
            qs = set(g['MaCauHoi'])
            if len(qs) >= 12:
                return g.nlargest(12, 'Diem')[['SubmissionID', 'MaCauHoi', 'Diem']] if len(qs) > 12 else g[['SubmissionID', 'MaCauHoi', 'Diem']]
            missing = set(range(1, 13)) - qs
            return pd.concat([g[['SubmissionID', 'MaCauHoi', 'Diem']], 
                             pd.DataFrame({'SubmissionID': [g.name] * len(missing), 'MaCauHoi': list(missing), 'Diem': [5] * len(missing)})])
        
        fact_ketqua = mcq_df.groupby('SubmissionID', group_keys=False).apply(complete_questions).reset_index(drop=True)
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform & NLP xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 JOB 1: TIỀN XỬ LÝ SIÊU TỐC (CÓ NLP)")
    print("=" * 70)
    print(f"📂 File: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ Workers: {NUM_WORKERS}")
    print(f"📦 Chunk size: {CHUNK_SIZE:,}")
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
    print(f"  ✅ Chuyên ngành: {len(chuyennganh_master):,} rows")
    
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
    dims = create_dimensions_fast(df_raw, hp_master, chuyennganh_master)
    
    # 6. Transform & NLP
    print("\n🔄 6. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_with_nlp_fast(df_raw)
    transform_time = time.time() - transform_start
    
    # 7. Lưu preprocessed data (pickle)
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
    print(f"   ✅ Self-Luận (NLP): {len(fact_main):,} rows")
    print(f"   ✅ Trắc nghiệm: {len(fact_ketqua):,} rows")
    
    if not fact_main.empty:
        print(f"\n   📌 Sentiment:")
        for sent, cnt in fact_main['Sentiment'].value_counts().items():
            print(f"      - {sent}: {cnt:,} ({cnt/len(fact_main)*100:.1f}%)")
        
        print(f"\n   📌 Tags:")
        print(f"      - Học phần: {fact_main['Tag_HocPhan'].sum():,}")
        print(f"      - Dạy học: {fact_main['Tag_DayHoc'].sum():,}")
        print(f"      - Kiểm tra: {fact_main['Tag_KiemTra'].sum():,}")
        print(f"      - Khác: {fact_main['Tag_Khac'].sum():,}")
    
    print(f"\n⏱️ Tổng thời gian: {total_time:.1f}s")
    print(f"🚀 Tốc độ: {len(df_raw)/total_time:,.0f} rows/s")
    print("=" * 70)
    print("✅ DỮ LIỆU ĐÃ SẴN SÀNG CHO JOB 2!")
    print("=" * 70)


if __name__ == "__main__":
    main()
