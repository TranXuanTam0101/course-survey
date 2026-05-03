
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
PREPROCESSED_PATH = "preprocessed-data"

# Tối ưu số worker = CPU cores
NUM_WORKERS = mp.cpu_count()
CHUNK_SIZE = 500000  # Tăng chunk size lên 500k

# ================= COMPILE REGEX PATTERNS TOÀN CỤC =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# Tối ưu NLP - dùng set cho lookup nhanh
POSITIVE_WORDS = {'tuyệt', 'tốt', 'hay', 'ổn', 'hài lòng', 'cảm ơn', 'ok', 'great', 'excellent', 
                  'thoải mái', 'vui', 'sôi nổi', 'hấp dẫn', 'dễ', 'thân thiện', 'tâm lý', 
                  'tận tâm', 'nhiệt tình', 'chu đáo', 'chi tiết', 'sáng tạo', 'thực tế', 'hiệu quả'}
NEGATIVE_WORDS = {'tệ', 'kém', 'dở', 'chán', 'khó', 'mông lung', 'lan man', 'dài dòng', 'qua loa',
                  'chắp vá', 'đọc chép', 'cứng nhắc', 'đơn điệu', 'thiếu', 'cũ kỹ', 'nhanh', 'lố giờ'}

# Compile regex cho tags
TAG_HP = re.compile(r'(chuẩn đầu ra|mục tiêu|nội dung|chương trình|môn học|trang bị|cung cấp|đào tạo|bám sát|phù hợp|rõ ràng|đầy đủ)', re.I)
TAG_DH = re.compile(r'(giảng viên|thầy|cô|tận tâm|nhiệt tình|truyền cảm hứng|dạy|giảng|bài giảng|sinh động|linh hoạt|tương tác|dễ hiểu)', re.I)
TAG_KT = re.compile(r'(kiểm tra|đánh giá|công bằng|minh bạch|thi|đề thi|cho điểm|công khai)', re.I)


# ================= HÀM TIỆN ÍCH TỐI ƯU =================
def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    return f"HK{hoc_ky}_{nam_bat_dau % 100}{nam_ket_thuc % 100}", f"{nam_bat_dau}-{nam_ket_thuc}", hoc_ky


def create_ma_khoa_fast(ten_khoa: str, khoa_dict: dict) -> str:
    """Tạo mã khoa nhanh - dùng dict thay vì counter"""
    ten_lower = ten_khoa.lower()
    if 'ngữ văn' in ten_lower or 'truyền thông' in ten_lower or 'toán' in ten_lower or 'tin' in ten_lower:
        return 'TĐHSP'
    if ten_khoa not in khoa_dict:
        khoa_dict[ten_khoa] = f"KHOA_{len(khoa_dict) + 1:03d}"
    return khoa_dict[ten_khoa]


def create_ma_nganh_fast(ten_nganh: str, nganh_dict: dict) -> str:
    if ten_nganh not in nganh_dict:
        nganh_dict[ten_nganh] = f"NGANH_{len(nganh_dict) + 1:03d}"
    return nganh_dict[ten_nganh]


def determine_ma_chuyen_nganh_fast(lop: str) -> str:
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


# ================= NLP TỐI ƯU =================
def analyze_sentiment_fast(text: str) -> str:
    if not isinstance(text, str) or len(text) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    # Kiểm tra nhanh với từ khóa mạnh
    if 'tuyệt vời' in text_lower or 'xuất sắc' in text_lower or 'hoàn hảo' in text_lower:
        return 'positive'
    if 'tệ hại' in text_lower or 'tồi tệ' in text_lower or 'thất vọng' in text_lower:
        return 'negative'
    
    # Đếm từ tích cực/tiêu cực
    pos_count = sum(1 for w in POSITIVE_WORDS if w in text_lower)
    neg_count = sum(1 for w in NEGATIVE_WORDS if w in text_lower)
    
    return 'positive' if pos_count > neg_count else ('negative' if neg_count > pos_count else 'neutral')


def extract_tags_fast(text: str) -> tuple:
    if not isinstance(text, str):
        return (0, 0, 0, 1)
    text_lower = text.lower()
    tag_hp = 1 if TAG_HP.search(text_lower) else 0
    tag_dh = 1 if TAG_DH.search(text_lower) else 0
    tag_kt = 1 if TAG_KT.search(text_lower) else 0
    return (tag_hp, tag_dh, tag_kt, 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0)


def process_nlp_batch(df):
    """Xử lý NLP batch - dùng list comprehension"""
    if df.empty:
        return df
    
    texts = df['NoiDungGopY'].fillna('').astype(str).values
    df['Sentiment'] = [analyze_sentiment_fast(t) for t in texts]
    tags = [extract_tags_fast(t) for t in texts]
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
        df = df.iloc[:, [1, 2, 4, 5]]
        df.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    return df.drop_duplicates(subset=['MaChuyenNganh'])


# ================= PARSE SURVEY - TỐI ƯU NHẤT =================
def parse_line(line):
    """Parse một dòng - tối ưu tối đa, không dùng try-except"""
    if not line or len(line) < 50:
        return None
    
    parts = line.strip().split(',')
    n = len(parts)
    if n < 15:
        return None
    
    # Tìm ngày sinh - chỉ loop 10 lần
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
    
    ma_hp = parts[ngay_sinh_idx + 1].strip() if ngay_sinh_idx + 1 < n else ''
    
    # Tìm mã GV
    ma_gv_idx = -1
    ma_gv = ''
    for i in range(ngay_sinh_idx + 2, min(n, ngay_sinh_idx + 25)):
        val = parts[i].strip()
        if val and ((len(val) == 7 and val.isdigit()) or (len(val) == 7 and val.startswith('TG')) or val == 'gvDacThu_TKTH'):
            ma_gv = val
            ma_gv_idx = i
            break
    
    if ma_gv_idx == -1:
        ma_gv_idx = n - 4 if n >= 4 else ngay_sinh_idx + 2
    
    ten_hp = ' '.join(parts[ngay_sinh_idx + 2:ma_gv_idx]).strip()
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


def parse_batch(batch):
    return [r for line in batch if (r := parse_line(line)) is not None]


def parse_survey(content: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = content.strip().split('\n')
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    # Chia batch
    batch_size = max(50000, len(lines) // NUM_WORKERS)
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    
    all_rows = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for batch in batches:
            all_rows.extend(parse_batch(batch))
    
    columns = ['SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
               'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
               'CauHoi', 'GiaTri', 'EssayText']
    
    df = pd.DataFrame(all_rows, columns=columns)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.2f}s)")
    return df


# ================= TẠO DIMENSIONS TỐI ƯU =================
def create_dimensions(df_raw, hp_master, chuyennganh_master):
    print("  -> Tạo dimension tables...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # Dùng dict thay vì DataFrame cho đến khi cần
    khoa_dict = {'TĐHKT': 'Trường ĐH Kinh tế', 'PĐT': 'Phòng Đào Tạo'}
    nganh_dict = {}
    chuyennganh_dict = {}
    lop_dict = {}
    
    # Xử lý khoa từ hp_master
    if not hp_master.empty:
        for ten_khoa in hp_master['TenKhoa'].drop_duplicates().values:
            ma_khoa = create_ma_khoa_fast(ten_khoa, khoa_dict)
            khoa_dict[ma_khoa] = ten_khoa
    
    if not chuyennganh_master.empty:
        for ten_khoa in chuyennganh_master['TenKhoa'].drop_duplicates().values:
            ma_khoa = create_ma_khoa_fast(ten_khoa, khoa_dict)
            khoa_dict[ma_khoa] = ten_khoa
        
        # Xử lý ngành và chuyên ngành
        for _, row in chuyennganh_master.iterrows():
            ten_nganh = row['TenNganh']
            ma_nganh = create_ma_nganh_fast(ten_nganh, nganh_dict)
            # Lưu mapping
            chuyennganh_dict[row['MaChuyenNganh']] = (row['TenChuyenNganh'], ma_nganh)
    
    # Tạo DataFrame
    dim_khoa = pd.DataFrame([(k, v) for k, v in khoa_dict.items()], columns=['MaKhoa', 'TenKhoa'])
    
    # Tìm mapping khoa cho ngành
    khoa_for_nganh = {}
    if not chuyennganh_master.empty:
        for ten_nganh, ma_nganh in nganh_dict.items():
            sample = chuyennganh_master[chuyennganh_master['TenNganh'] == ten_nganh].iloc[0]
            ten_khoa = sample['TenKhoa']
            ma_khoa = dim_khoa[dim_khoa['TenKhoa'] == ten_khoa]['MaKhoa'].values
            khoa_for_nganh[ma_nganh] = ma_khoa[0] if len(ma_khoa) > 0 else 'TĐHKT'
    
    dim_nganh = pd.DataFrame([(ma_nganh, ten_nganh, khoa_for_nganh.get(ma_nganh, 'TĐHKT')) 
                              for ten_nganh, ma_nganh in nganh_dict.items()], 
                             columns=['MaNganh', 'TenNganh', 'MaKhoa'])
    
    # Thêm default
    default_nganh = pd.DataFrame([('NULL_CTS', 'Ngành NULL_CTS', 'TĐHKT'),
                                   ('NULL_QT', 'Ngành NULL_QT', 'PĐT')], 
                                  columns=['MaNganh', 'TenNganh', 'MaKhoa'])
    dim_nganh = pd.concat([default_nganh, dim_nganh], ignore_index=True).drop_duplicates('MaNganh')
    
    # DIM_CHUYEN_NGANH
    default_cn = pd.DataFrame([('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
                                ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')],
                               columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'])
    
    cn_list = [(ma_cn, ten_cn, ma_nganh) for ma_cn, (ten_cn, ma_nganh) in chuyennganh_dict.items()]
    dim_chuyen_nganh = pd.DataFrame(cn_list, columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'])
    dim_chuyen_nganh = pd.concat([default_cn, dim_chuyen_nganh], ignore_index=True).drop_duplicates('MaChuyenNganh')
    
    # DIM_HOC_PHAN
    hp_dict = {}
    if not hp_master.empty:
        for _, row in hp_master.drop_duplicates('MaHP').iterrows():
            hp_dict[row['MaHP']] = (row['TenHP'], row['TenKhoa'])
    
    hp_list = []
    df_hp = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP'])
    for ma_hp, ten_hp in df_hp.values:
        if ma_hp in hp_dict:
            ten_hp, ten_khoa = hp_dict[ma_hp]
            ma_khoa = dim_khoa[dim_khoa['TenKhoa'] == ten_khoa]['MaKhoa'].values
            ma_khoa = ma_khoa[0] if len(ma_khoa) > 0 else 'TĐHKT'
            hp_list.append((ma_hp, ten_hp, ma_khoa))
        else:
            hp_list.append((ma_hp, ten_hp if pd.notna(ten_hp) else f"Học phần {ma_hp}", 'TĐHKT'))
    
    dim_hoc_phan = pd.DataFrame(hp_list, columns=['MaHP', 'TenHP', 'MaKhoa'])
    
    # DIM_GIANG_VIEN
    dim_giang_vien = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    
    # DIM_HOC_KY
    dim_hoc_ky = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # DIM_LOP_SINH_VIEN
    valid_cn = set(dim_chuyen_nganh['MaChuyenNganh'].values)
    lop_list = []
    for lop in df_raw['Lop'].drop_duplicates().dropna().values:
        ma_cn = determine_ma_chuyen_nganh_fast(lop)
        if ma_cn and (ma_cn in valid_cn or ma_cn in ['NULL_CTS', 'NULL_QT']):
            lop_list.append((lop, lop, ma_cn))
    
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
    lhp_list = []
    for lop_hp, ma_hp, ma_gv in df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP']).values:
        if ma_hp in valid_hp and ma_gv in valid_gv:
            lhp_list.append((lop_hp, lop_hp, ma_hp, ma_gv, ma_hoc_ky))
    
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


# ================= TRANSFORM =================
def transform_data(df_raw: pd.DataFrame) -> tuple:
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
        text_df_unique = process_nlp_batch(text_df_unique)
        print(f"      -> NLP xong ({time.time()-nlp_start:.2f}s)")
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']]
    
    # XỬ LÝ TRẮC NGHIỆM
    mcq_mask = (df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
                df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != ''))
    mcq_df = df_raw[mcq_mask][['SubmissionID', 'CauHoi', 'GiaTri']].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
        # Tạo đủ 12 câu - dùng groupby nhanh
        def complete_questions(g):
            qs = set(g['MaCauHoi'])
            if len(qs) >= 12:
                if len(qs) > 12:
                    return g.nlargest(12, 'Diem')[['SubmissionID', 'MaCauHoi', 'Diem']]
                return g[['SubmissionID', 'MaCauHoi', 'Diem']]
            missing = set(range(1, 13)) - qs
            missing_df = pd.DataFrame({
                'SubmissionID': [g.name] * len(missing),
                'MaCauHoi': list(missing),
                'Diem': [5] * len(missing)
            })
            return pd.concat([g[['SubmissionID', 'MaCauHoi', 'Diem']], missing_df])
        
        fact_ketqua = mcq_df.groupby('SubmissionID', group_keys=False).apply(complete_questions).reset_index(drop=True)
        fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 JOB 1: TIỀN XỬ LÝ SIÊU TỐC")
    print("=" * 70)
    print(f"📂 File: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ Workers: {NUM_WORKERS}")
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
    print(f"  ✅ HP-Khoa: {len(hp_master):,}")
    print(f"  ✅ Chuyên ngành: {len(chuyennganh_master):,}")
    
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
    df_raw = parse_survey(survey_content)
    parse_time = time.time() - parse_start
    print(f"  ✅ Parse: {len(df_raw):,} rows ({parse_time:.1f}s)")
    
    # 5. Tạo dimensions
    print("\n🏗️ 5. Tạo dimensions...")
    dims = create_dimensions(df_raw, hp_master, chuyennganh_master)
    
    # 6. Transform
    print("\n🔄 6. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_data(df_raw)
    transform_time = time.time() - transform_start
    
    # 7. Lưu preprocessed data
    print("\n💾 7. Lưu preprocessed data...")
    preprocessed_data = {
        'metadata': {'semester': SEMESTER, 'survey_file': SURVEY_FILE, 
                     'timestamp': datetime.now().isoformat(), 'ma_hoc_ky': dims['ma_hoc_ky']},
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
    print(f"   ✅ Transform: {transform_time:.1f}s")
    print(f"   ✅ FACT_GOP_Y_TU_LUAN: {len(fact_main):,} rows")
    print(f"   ✅ FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} rows")
    print(f"\n⏱️ Tổng thời gian: {total_time:.1f}s")
    print(f"🚀 Tốc độ: {len(df_raw)/total_time:,.0f} rows/s")
    print("=" * 70)
    print("✅ DỮ LIỆU ĐÃ SẴN SÀNG CHO JOB 2!")
    print("=" * 70)


if __name__ == "__main__":
    main()
