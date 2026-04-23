import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import multiprocessing as mp

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
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
    f"Connection Timeout=120;"
    f"Command Timeout=300;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_CONTAINER = "tailieu"
PROCESSED_PATH = "processed-data"

# Số lượng worker
NUM_WORKERS = max(2, mp.cpu_count())
CHUNK_SIZE = 50000

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')

# ================= CÁC HÀM TIỆN ÍCH =================
def normalize_lop(lop: str) -> str:
    if not isinstance(lop, str): 
        return ""
    if lop.upper().startswith('CTS-'): 
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: 
            lop = lop.split(sep)[0]
    return lop.strip()

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
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "UNKNOWN"
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
    lop_upper = lop.upper()
    lop_normalized = normalize_lop(lop)
    
    if _lop_pattern.match(lop_normalized):
        ma_cn = f"K{lop_normalized[3:5]}"
        return ma_cn, f"Chuyên ngành {ma_cn}", None, None
    if 'QT' in lop_upper:
        return "QT", "Chuyên ngành QT", "Phòng Đào Tạo", "PĐT"
    if 'CTS' in lop_upper or lop_upper.startswith('CTS-') or lop_upper.startswith('CTS'):
        return "CTS", "Chuyên ngành CTS", "Trường ĐHKT", "TĐHKT"
    return None, None, None, None


# ================= NLP CLASS =================
class VietnameseNLP:
    
    def __init__(self):
        self.positive_words = {
            'tuyệt vời', 'tuyệt', 'mãi yêu', 'yêu cô', 'yêu thầy', 'siêu thích',
            'hào hứng', 'thoải mái', 'vui', 'sôi nổi', 'hấp dẫn', 'dễ mến',
            'dễ gần', 'thân thiện', 'gần gũi', 'tâm lý', 'dễ thương', 'vui tính',
            'có tâm', 'tâm huyết', 'tận tâm', 'tận tụy', 'tận tình', 'nhiệt huyết',
            'chu đáo', 'kỹ', 'cẩn thận', 'chi tiết', 'sâu sắc', 'nghiêm túc',
            'linh hoạt', 'sáng tạo', 'mới mẻ', 'thực tế', 'thực tiễn', 'sát ngành',
            'bám sát', 'đúng trọng tâm', 'hiệu quả', 'tiến bộ', 'đa dạng',
            'phong phú', 'hợp lý', 'chuẩn', 'tạo điều kiện', 'hỗ trợ',
            'giải đáp thắc mắc', 'chỉnh chu', 'tốt', 'hay', 'ổn', 'hài lòng',
            'cảm ơn', 'ok', 'oke', 'oki', 'good', 'great', 'excellent'
        }
        
        self.negative_words = {
            'khó hiểu', 'khó tiếp thu', 'mông lung', 'lan man', 'dài dòng',
            'qua loa', 'chắp vá', 'đọc chép', 'phụ thuộc slide', 'thiếu linh hoạt',
            'cứng nhắc', 'nhàm chán', 'đơn điệu', 'cũ kỹ', 'dạy nhanh',
            'dạy lố giờ', 'thiếu tương tác', 'không tương tác', 'thiếu nhiệt tình',
            'không tâm huyết', 'quá rộng', 'quá khó', 'không phù hợp', 'không sát',
            'thiếu cụ thể', 'mơ hồ', 'chung chung', 'không rõ', 'thiếu tài liệu',
            'không cập nhật', 'nặng', 'quá tải', 'không công bằng', 'thiếu minh bạch',
            'bất tiện', 'chưa hoàn thiện', 'tệ', 'dở', 'kém', 'chán', 'thất vọng'
        }
        
        self.tag_keywords = {
            'Tag_HocPhan': ['chuẩn đầu ra', 'mục tiêu môn học', 'đáp ứng chương trình',
                           'nội dung', 'học phần', 'chương trình', 'môn học', 'trang bị',
                           'cung cấp', 'đào tạo', 'bám sát', 'phù hợp', 'rõ ràng', 'đầy đủ'],
            'Tag_DayHoc': ['giảng viên', 'thầy giáo', 'cô giáo', 'tận tâm', 'nhiệt tình',
                          'tận tình', 'truyền cảm hứng', 'dạy', 'giảng', 'nhiệt huyết',
                          'dễ hiểu', 'bài giảng', 'sinh động', 'linh hoạt', 'tương tác'],
            'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'công bằng', 'minh bạch', 'đánh giá đúng',
                           'thi', 'đề thi', 'cho điểm', 'công khai', 'thực lực', 'công tâm']
        }
        
        self.tag_khac_keywords = ['không có góp ý', 'không ý kiến', 'không góp ý', 
                                  'không', 'ko', 'k', 'không có', 'ok', 'ổn', 'tốt', 'được']
        
        self.neutral_phrases = ['không có ý kiến', 'không góp ý', 'không có góp ý', 
                                'không', 'ko', 'k', 'bình thường', 'tạm được']
        
        # Tạo regex cho vectorized
        self.pos_regex = '|'.join(re.escape(w) for w in self.positive_words)
        self.neg_regex = '|'.join(re.escape(w) for w in self.negative_words)
        self.tag_hp_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_HocPhan'])
        self.tag_dh_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_DayHoc'])
        self.tag_kt_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_KiemTra'])
        self.tag_khac_regex = '|'.join(re.escape(w) for w in self.tag_khac_keywords)
        self.neutral_regex = '|'.join(re.escape(w) for w in self.neutral_phrases)
    
    def analyze_sentiment_vectorized(self, texts):
        series = pd.Series(texts)
        pos_count = series.str.count(self.pos_regex).fillna(0)
        neg_count = series.str.count(self.neg_regex).fillna(0)
        is_neutral = series.str.contains(self.neutral_regex, na=False, regex=True)
        
        sentiment = pd.Series(['neutral'] * len(series))
        sentiment[(pos_count > neg_count) & ~is_neutral] = 'positive'
        sentiment[(neg_count > pos_count) & ~is_neutral] = 'negative'
        sentiment[is_neutral] = 'neutral'
        return sentiment.tolist()
    
    def extract_tags_vectorized(self, texts):
        series = pd.Series(texts)
        is_neutral = series.str.contains(self.neutral_regex, na=False, regex=True)
        
        tag_hp = series.str.contains(self.tag_hp_regex, na=False, regex=True).astype(int)
        tag_dh = series.str.contains(self.tag_dh_regex, na=False, regex=True).astype(int)
        tag_kt = series.str.contains(self.tag_kt_regex, na=False, regex=True).astype(int)
        tag_khac = ((tag_hp + tag_dh + tag_kt) == 0).astype(int)
        tag_khac[is_neutral] = 1
        
        return list(zip(tag_hp, tag_dh, tag_kt, tag_khac))

_nlp = VietnameseNLP()


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


# ================= PARSE SURVEY DATA - GIỮ NGUYÊN DẠNG DỌC =================
def parse_survey_to_long_format(content: str) -> pd.DataFrame:
    """
    Parse CSV trực tiếp thành dạng dọc (long format)
    Mỗi dòng = 1 câu trả lời (có thể không đủ 12 câu)
    """
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
            
            # Tạo SubmissionID cho cả text và MCQ
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
                'CauHoi': cau_hoi,      # Câu hỏi số mấy (1-12)
                'GiaTri': gia_tri,       # Điểm 1-5
                'EssayText': essay_text  # Nội dung tự luận (chỉ có khi CauHoi rỗng)
            })
        except Exception:
            continue
    return results

def is_date_format(value):
    return bool(_date_pattern.match(value.strip())) if isinstance(value, str) else False

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH"


# ================= TRANSFORM & NLP - GIỮ NGUYÊN DẠNG DỌC =================
def transform_with_nlp_long_format(df_raw: pd.DataFrame) -> tuple:
    """
    Transform dữ liệu dạng dọc:
    - FACT_GOP_Y_TU_LUAN: mỗi submission 1 dòng (từ EssayText)
    - FACT_KET_QUA_DANH_GIA: giữ nguyên dạng dọc của MCQ
    """
    print("  -> Transform dữ liệu...")
    start = time.time()
    
    # ===== 1. XỬ LÝ CÂU TỰ LUẬN =====
    # Lấy các dòng có EssayText (không có CauHoi)
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    # Gom các text theo SubmissionID (mỗi submission chỉ có 1 text)
    text_df = text_df.drop_duplicates('SubmissionID')
    text_df['NoiDungGopY'] = text_df['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
    
    # Vectorized NLP 
    texts = text_df['NoiDungGopY'].tolist()
    print(f"  -> Đang xử lý NLP cho {len(texts):,} bài tự luận...")
    
    text_df['Sentiment'] = _nlp.analyze_sentiment_vectorized(texts)
    tag_vectors = _nlp.extract_tags_vectorized(texts)
    
    text_df['Tag_HocPhan'] = [v[0] for v in tag_vectors]
    text_df['Tag_DayHoc'] = [v[1] for v in tag_vectors]
    text_df['Tag_KiemTra'] = [v[2] for v in tag_vectors]
    text_df['Tag_Khac'] = [v[3] for v in tag_vectors]
    text_df['Is_Valid'] = 1  # Mặc định hợp lệ
    
    # Chuẩn bị FACT_GOP_Y_TU_LUAN
    fact_main = text_df[[
        'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
        'Sentiment', 'Is_Valid',
        'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
    ]].copy()
    
    # ===== 2. XỬ LÝ CÂU TRẮC NGHIỆM =====
    # Lấy các dòng có CauHoi (1-12) và GiaTri (1-5)
    mcq_df = df_raw[
        df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
        df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != '')
    ].copy()
    
    mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
    mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
    
    # Giữ nguyên dạng dọc - KHÔNG PIVOT
    fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
    
    print(f"  ✅ Transform xong: {len(fact_main):,} bài tự luận, {len(fact_ketqua):,} câu trắc nghiệm ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= LOAD DATABASE OPTIMIZED =================
def batch_insert_optimized(cursor, table, columns, data, batch_size=100000):
    if not data:
        return 0
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(sql, batch)
        total += len(batch)
        if total % 100000 == 0:
            print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
    return total

def load_dimensions_optimized(cursor, df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping):
    print("\n📥 Loading DIMENSION tables...")
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # Lấy unique values từ df_raw
    unique_khoa = set()
    unique_nganh = set()
    unique_chuyennganh = set()
    unique_lop = df_raw['Lop'].dropna().unique()
    unique_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV')
    unique_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
    unique_hp = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP')
    unique_lophp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP')
    
    # DIM_KHOA
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing_khoa = {row[0] for row in cursor.fetchall()}
    data_khoa = [(ma, ma) for ma in unique_khoa if ma not in existing_khoa]
    if data_khoa:
        batch_insert_optimized(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], data_khoa, 10000)
    
    # DIM_NGANH
    if not dim_nganh.empty:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing_nganh = {row[0] for row in cursor.fetchall()}
        data_nganh = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) 
                      for _, row in dim_nganh.iterrows() if row['MaNganh'] not in existing_nganh]
        if data_nganh:
            batch_insert_optimized(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], data_nganh, 10000)
    
    # DIM_CHUONG_TRINH_DAO_TAO
    cursor.execute("IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY') INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')")
    
    # DIM_CHUYEN_NGANH
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn = {row[0] for row in cursor.fetchall()}
    data_cn = []
    for _, row in dim_chuyennganh.iterrows():
        if row['MaChuyenNganh'] not in existing_cn:
            data_cn.append((row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh'], 'CTDT_CHINHQUY'))
    if data_cn:
        batch_insert_optimized(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh', 'MaCTDT'], data_cn, 5000)
    
    # DIM_LOP_SINH_VIEN
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    data_lop = [(lop, lop, 'UNKNOWN') for lop in unique_lop if lop not in existing_lop]
    if data_lop:
        batch_insert_optimized(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], data_lop, 5000)
    
    # DIM_SINH_VIEN
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {row[0] for row in cursor.fetchall()}
    data_sv = []
    for _, row in unique_sv.iterrows():
        if row['MaSV'] not in existing_sv:
            ngay_sinh = None
            if row['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            data_sv.append((row['MaSV'], row['HoDem'], row['Ten'], ngay_sinh, row['Lop']))
    if data_sv:
        batch_insert_optimized(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], data_sv, 5000)
    
    # DIM_GIANG_VIEN
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_gv = {row[0] for row in cursor.fetchall()}
    data_gv = [(row['MaGV'], row['HoDemGV'], row['TenGV']) 
               for _, row in unique_gv.iterrows() if row['MaGV'] not in existing_gv]
    if data_gv:
        batch_insert_optimized(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], data_gv, 50000)
    
    # DIM_HOC_PHAN
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp = {row[0] for row in cursor.fetchall()}
    hp_dict = {}
    if not hp_master.empty:
        hp_dict = hp_master.set_index('MaHP')[['TenHP', 'MaKhoa']].to_dict('index')
    data_hp = []
    for _, row in unique_hp.iterrows():
        if row['MaHP'] not in existing_hp:
            if row['MaHP'] in hp_dict:
                data_hp.append((row['MaHP'], hp_dict[row['MaHP']]['TenHP'], hp_dict[row['MaHP']]['MaKhoa']))
            else:
                data_hp.append((row['MaHP'], row['TenHP'] or f"Học phần {row['MaHP']}", 'UNKNOWN'))
    if data_hp:
        batch_insert_optimized(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], data_hp, 5000)
    
    # DIM_HOC_KY
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", ma_hoc_ky, nam_hoc, hoc_ky)
    
    # DIM_LOP_HOC_PHAN
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {row[0] for row in cursor.fetchall()}
    data_lhp = [(row['LopHP'], row['LopHP'], row['MaHP'], row['MaGV'], ma_hoc_ky) 
                for _, row in unique_lophp.iterrows() if row['LopHP'] not in existing_lhp]
    if data_lhp:
        batch_insert_optimized(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], data_lhp, 5000)
    
    cursor.connection.commit()
    print("  ✅ All DIMENSION tables loaded!")


def load_fact_tables_optimized(cursor, fact_main, fact_ketqua):
    print("\n📥 Loading FACT tables...")
    start_time = time.time()
    
    # Tắt constraints
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    
    # FACT_GOP_Y_TU_LUAN
    if not fact_main.empty:
        data_main = [tuple(row) for row in fact_main.to_numpy()]
        count_main = batch_insert_optimized(cursor, 'FACT_GOP_Y_TU_LUAN',
            ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY', 'Sentiment', 'Is_Valid',
             'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'], data_main, 50000)
        print(f"    ✅ FACT_GOP_Y_TU_LUAN: {count_main} dòng")
    else:
        count_main = 0
    
    # FACT_KET_QUA_DANH_GIA - GIỮ NGUYÊN DẠNG DỌC
    if not fact_ketqua.empty:
        data_kq = [tuple(row) for row in fact_ketqua.to_numpy()]
        count_kq = batch_insert_optimized(cursor, 'FACT_KET_QUA_DANH_GIA',
            ['SubmissionID', 'MaCauHoi', 'Diem'], data_kq, 100000)
        print(f"    ✅ FACT_KET_QUA_DANH_GIA: {count_kq} dòng")
    else:
        count_kq = 0
    
    # Bật lại constraints
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    elapsed = time.time() - start_time
    print(f"  ✅ FACT tables loaded in {elapsed:.1f}s")
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - LONG FORMAT (GIỮ NGUYÊN DẠNG DỌC)")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
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
    
    # 3. Đọc và parse dữ liệu survey - GIỮ NGUYÊN DẠNG DỌC
    print(f"\n📥 3. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    print("\n📝 4. Parse dữ liệu (giữ nguyên dạng dọc)...")
    df_raw = parse_survey_to_long_format(survey_content)
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    
    # 5. Transform & NLP - GIỮ NGUYÊN DẠNG DỌC
    print("\n🔄 5. Transform & NLP...")
    fact_main, fact_ketqua = transform_with_nlp_long_format(df_raw)
    
    # 6. Lưu backup
    print("\n💾 6. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    save_processed(blob_service, fact_ketqua, f"{FILE_NAME}_ketqua_{timestamp}.csv")
    
    # 7. Kết nối Database
    print("\n💾 7. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        conn.fast_executemany = True
        cursor = conn.cursor()
        print("  ✅ Kết nối SQL thành công")
    except Exception as e:
        print(f"  ❌ Lỗi kết nối SQL: {e}")
        return
    
    # 8. Load to database
    try:
        load_dimensions_optimized(cursor, df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping)
        count_main, count_kq = load_fact_tables_optimized(cursor, fact_main, fact_ketqua)
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # 9. Thống kê
    print("\n📊 9. KẾT QUẢ:")
    print(f"   - Số phiếu tự luận: {len(fact_main):,}")
    print(f"   - Số câu trắc nghiệm: {count_kq:,}")
    print(f"   - Tag phân bố:")
    if len(fact_main) > 0:
        print(f"      Tag_HocPhan: {fact_main['Tag_HocPhan'].sum():,}")
        print(f"      Tag_DayHoc: {fact_main['Tag_DayHoc'].sum():,}")
        print(f"      Tag_KiemTra: {fact_main['Tag_KiemTra'].sum():,}")
        print(f"      Tag_Khac: {fact_main['Tag_Khac'].sum():,}")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH! Thời gian: {time.time()-total_start:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
