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
PREPROCESSED_PATH = "preprocessed-data"  # Lưu preprocessed data

# Số lượng worker
NUM_WORKERS = max(2, min(mp.cpu_count(), 4))
CHUNK_SIZE = 100000

# ================= COMPILE REGEX PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# NLP Patterns
_sentiment_patterns = {
    'positive': re.compile(r'(tuyệt|tốt|hay|ổn|hài lòng|cảm ơn|ok|great|excellent|thoải mái|vui|sôi nổi|hấp dẫn|dễ|thân thiện|tâm lý|tận tâm|nhiệt tình|chu đáo|chi tiết|sáng tạo|thực tế|hiệu quả)'),
    'negative': re.compile(r'(tệ|kém|dở|chán|khó|mông lung|lan man|dài dòng|qua loa|chắp vá|đọc chép|cứng nhắc|đơn điệu|thiếu|cũ kỹ|nhanh|lố giờ|không|chưa|chẳng)'),
    'very_positive': re.compile(r'(tuyệt vời|xuất sắc|hoàn hảo|quá tuyệt|siêu|rất tốt|cực kỳ)'),
    'very_negative': re.compile(r'(tệ hại|tồi tệ|thất vọng|quá khó|rất chán)')
}

_tag_patterns = {
    'Tag_HocPhan': re.compile(r'(chuẩn đầu ra|mục tiêu|nội dung|chương trình|môn học|trang bị|cung cấp|đào tạo|bám sát|phù hợp|rõ ràng|đầy đủ)', re.IGNORECASE),
    'Tag_DayHoc': re.compile(r'(giảng viên|thầy|cô|tận tâm|nhiệt tình|truyền cảm hứng|dạy|giảng|bài giảng|sinh động|linh hoạt|tương tác|dễ hiểu)', re.IGNORECASE),
    'Tag_KiemTra': re.compile(r'(kiểm tra|đánh giá|công bằng|minh bạch|thi|đề thi|cho điểm|công khai)', re.IGNORECASE)
}


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


# ================= NLP FUNCTIONS =================
def analyze_sentiment_fast(text: str) -> str:
    """Phân tích sentiment nhanh"""
    if not isinstance(text, str) or len(text.strip()) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    if _sentiment_patterns['very_positive'].search(text_lower):
        return 'positive'
    if _sentiment_patterns['very_negative'].search(text_lower):
        return 'negative'
    
    pos_count = len(_sentiment_patterns['positive'].findall(text_lower))
    neg_count = len(_sentiment_patterns['negative'].findall(text_lower))
    
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    return 'neutral'


def extract_tags_fast(text: str) -> tuple:
    """Trích xuất tags nhanh"""
    if not isinstance(text, str):
        return (0, 0, 0, 1)
    
    text_lower = text.lower()
    
    tag_hp = 1 if _tag_patterns['Tag_HocPhan'].search(text_lower) else 0
    tag_dh = 1 if _tag_patterns['Tag_DayHoc'].search(text_lower) else 0
    tag_kt = 1 if _tag_patterns['Tag_KiemTra'].search(text_lower) else 0
    tag_khac = 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0
    
    return (tag_hp, tag_dh, tag_kt, tag_khac)


def process_nlp_batch_vectorized(df):
    """Xử lý NLP vectorized cho DataFrame"""
    if df.empty:
        return df
    
    texts = df['NoiDungGopY'].fillna('').astype(str).values
    
    # Xử lý sentiment
    sentiments = [analyze_sentiment_fast(t) for t in texts]
    
    # Xử lý tags
    tags = [extract_tags_fast(t) for t in texts]
    
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


def save_preprocessed_data(blob_service, data_dict, filename):
    """Lưu dữ liệu đã tiền xử lý (bao gồm NLP) dưới dạng pickle"""
    path = f"{PREPROCESSED_PATH}/{filename}.pkl"
    try:
        pickled_data = pickle.dumps(data_dict)
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(path)
        blob.upload_blob(pickled_data, overwrite=True)
        print(f"  ✅ Đã lưu preprocessed data: {path}")
        return True
    except Exception as e:
        print(f"  ❌ Lỗi lưu preprocessed data: {e}")
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


# ================= PARSE SURVEY DATA =================
def parse_line_fast(line):
    """Parse một dòng CSV"""
    if not line or not line.strip():
        return None
    
    row = line.strip().split(',')
    row_len = len(row)
    if row_len < 15:
        return None
    
    lop = row[0]
    ma_sv = row[1]
    
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
    
    ho_dem = ''
    ten = ''
    if ngay_sinh_index > 1:
        name_parts = [row[j].strip() for j in range(2, ngay_sinh_index) if row[j].strip()]
        if name_parts:
            ten = name_parts[-1]
            ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
    
    ma_hp = row[ngay_sinh_index + 1].strip() if ngay_sinh_index + 1 < row_len else ''
    
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
    
    ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]).strip()
    ho_dem_gv = row[ma_gv_index + 1].strip() if ma_gv_index + 1 < row_len else ''
    ten_gv = row[ma_gv_index + 2].strip() if ma_gv_index + 2 < row_len else ''
    lop_hp = row[ma_gv_index + 3].strip() if ma_gv_index + 3 < row_len else ''
    cau_hoi = row[ma_gv_index + 4].strip() if ma_gv_index + 4 < row_len else ''
    gia_tri = row[ma_gv_index + 5].strip() if ma_gv_index + 5 < row_len else ''
    
    null_index = -1
    for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
        if row[i].strip().upper() == 'NULL' or row[i].strip() == '':
            null_index = i
            break
    
    essay_text = ''
    if null_index != -1 and null_index + 1 < row_len:
        essay_text = ','.join(row[null_index + 1:]).strip()
    
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
    results = []
    for line in lines_batch:
        result = parse_line_fast(line)
        if result:
            results.append(result)
    return results


def parse_survey_to_long_format(content: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batch_size = max(10000, len(lines) // NUM_WORKERS)
    batches = [lines[i:i+batch_size] for i in range(0, len(lines), batch_size)]
    
    all_rows = []
    with ProcessPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch_optimized, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    df = pd.DataFrame(all_rows)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.2f}s)")
    return df


# ================= TẠO DIMENSIONS =================
def create_dimensions_optimized(df_raw: pd.DataFrame, hp_master, chuyennganh_master) -> dict:
    print("  -> Tạo dimension tables...")
    start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # DIM_KHOA
    khoa_list = []
    if not hp_master.empty:
        khoa_list.extend(hp_master[['MaKhoa', 'TenKhoa']].drop_duplicates().values.tolist())
    
    default_khoa = [('TĐHKT', 'Trường ĐH Kinh tế'), ('PĐT', 'Phòng Đào Tạo')]
    khoa_dict = dict(default_khoa)
    for ma, ten in khoa_list:
        khoa_dict[ma] = ten
    
    dim_khoa = pd.DataFrame([(ma, ten) for ma, ten in khoa_dict.items()], columns=['MaKhoa', 'TenKhoa'])
    
    # DIM_NGANH và DIM_CHUYEN_NGANH
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
    
    # DIM_HOC_PHAN
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
    
    # DIM_GIANG_VIEN
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    dim_giang_vien = df_gv[['MaGV', 'HoDemGV', 'TenGV']].copy()
    
    # DIM_HOC_KY
    dim_hoc_ky = pd.DataFrame([(ma_hoc_ky, nam_hoc, hoc_ky)], columns=['MaHocKy', 'NamHoc', 'HocKy'])
    
    # DIM_LOP_SINH_VIEN
    valid_chuyen_nganh = set(dim_chuyen_nganh['MaChuyenNganh'].values)
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    
    lop_list = []
    for lop in df_lop_unique['Lop'].values:
        ma_cn = determine_ma_chuyen_nganh(lop)
        if ma_cn and ma_cn in valid_chuyen_nganh:
            lop_list.append((lop, lop, ma_cn))
    
    dim_lop_sinh_vien = pd.DataFrame(lop_list, columns=['MaLop', 'Lop', 'MaChuyenNganh']).drop_duplicates('MaLop')
    
    # DIM_SINH_VIEN
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
    
    # DIM_LOP_HOC_PHAN
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


# ================= TRANSFORM DỮ LIỆU (CÓ NLP) =================
def transform_data_with_nlp(df_raw: pd.DataFrame) -> tuple:
    """Transform dữ liệu và xử lý NLP ngay tại đây"""
    print("  -> Transform dữ liệu & xử lý NLP...")
    start = time.time()
    
    # XỬ LÝ CÂU TỰ LUẬN (có NLP)
    text_mask = df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')
    text_df = df_raw[text_mask].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        # Loại bỏ trùng lặp
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        
        # Làm sạch text
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip().str[:4000]
        
        print(f"      -> Đang xử lý NLP cho {len(text_df_unique):,} bài tự luận...")
        nlp_start = time.time()
        
        # XỬ LÝ NLP NGAY TẠI ĐÂY
        text_df_unique = process_nlp_batch_vectorized(text_df_unique)
        
        print(f"      -> NLP xong trong {time.time()-nlp_start:.2f}s")
        
        fact_main = text_df_unique[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid',
            'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].copy()
    
    # XỬ LÝ CÂU TRẮC NGHIỆM
    mcq_mask = (df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
                df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != ''))
    mcq_df = df_raw[mcq_mask].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        
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
        
        fact_ketqua = mcq_df.groupby('SubmissionID', group_keys=False).apply(process_submission).reset_index(drop=True)
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform & NLP xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 JOB 1: TIỀN XỬ LÝ DỮ LIỆU (CÓ NLP)")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
    print(f"WORKERS: {NUM_WORKERS}")
    print("=" * 60)
    print("\n📌 TIỀN XỬ LÝ BAO GỒM:")
    print("   - Parse dữ liệu từ CSV")
    print("   - Tạo dimension tables")
    print("   - Xử lý NLP (sentiment & tags)")
    print("   - Tạo đủ 12 câu trắc nghiệm")
    print("   - Lưu preprocessed data (pickle)")
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
    
    # 6. Transform dữ liệu & xử lý NLP
    print("\n🔄 6. Transform dữ liệu & xử lý NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_data_with_nlp(df_raw)
    transform_time = time.time() - transform_start
    
    # 7. Lưu preprocessed data (pickle) cho JOB 2
    print("\n💾 7. Lưu preprocessed data (pickle)...")
    preprocessed_data = {
        'metadata': {
            'semester': SEMESTER,
            'survey_file': SURVEY_FILE,
            'file_name': FILE_NAME,
            'ma_hoc_ky': dims['ma_hoc_ky'],
            'timestamp': datetime.now().isoformat(),
            'nlp_processed': True
        },
        **dims,
        'fact_gop_y_tu_luan': fact_main,
        'fact_ket_qua_danh_gia': fact_ketqua,
        'df_raw': df_raw
    }
    
    save_preprocessed_data(blob_service, preprocessed_data, f"{FILE_NAME}_preprocessed")
    
    # 8. Lưu CSV backup (phòng trường hợp pickle lỗi)
    print("\n💾 8. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    if not fact_ketqua.empty:
        save_processed(blob_service, fact_ketqua, f"{FILE_NAME}_ketqua_{timestamp}.csv")
    
    # 9. Thống kê
    total_time = time.time() - total_start
    print("\n📊 9. KẾT QUẢ TIỀN XỬ LÝ:")
    print(f"   - Dòng dữ liệu thô: {len(df_raw):,}")
    print(f"   - Số phiếu tự luận (đã NLP): {len(fact_main):,}")
    print(f"   - Số câu trắc nghiệm: {len(fact_ketqua):,}")
    
    if not fact_main.empty:
        print(f"\n   📌 Sentiment distribution (đã xử lý):")
        sentiment_counts = fact_main['Sentiment'].value_counts()
        for sent, cnt in sentiment_counts.items():
            pct = cnt/len(fact_main)*100
            print(f"      - {sent}: {cnt:,} ({pct:.1f}%)")
        
        print(f"\n   📌 Tag distribution:")
        print(f"      - Tag_HocPhan: {fact_main['Tag_HocPhan'].sum():,}")
        print(f"      - Tag_DayHoc: {fact_main['Tag_DayHoc'].sum():,}")
        print(f"      - Tag_KiemTra: {fact_main['Tag_KiemTra'].sum():,}")
        print(f"      - Tag_Khac: {fact_main['Tag_Khac'].sum():,}")
    
    print(f"\n   Dimensions:")
    for name, df in dims.items():
        if name != 'ma_hoc_ky' and not df.empty:
            print(f"      - {name}: {len(df):,} dòng")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH TIỀN XỬ LÝ! Thời gian: {total_time:.1f}s")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform & NLP: {transform_time:.1f}s")
    print(f"   - Dữ liệu đã sẵn sàng cho JOB 2")
    print("=" * 60)


if __name__ == "__main__":
    main()
