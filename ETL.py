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
BATCH_SIZE = 100000

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')


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


def determine_ma_chuyen_nganh_special(lop: str) -> tuple:
    """
    Xác định MaChuyenNganh, MaNganh, MaKhoa cho các trường hợp đặc biệt
    Returns: (ma_chuyen_nganh, ten_chuyen_nganh, ma_nganh, ten_nganh, ma_khoa, ten_khoa)
    """
    if not lop or not isinstance(lop, str):
        return None, None, None, None, None, None
    
    lop_upper = lop.upper().strip()
    
    # ===== TH1: CHỨA CTS (ưu tiên cao nhất) =====
    if 'CTS' in lop_upper:
        # Xử lý trường hợp CTS-50K hoặc CTS-50K-QT.1
        match = re.search(r'CTS[-_]?(\d{2})K', lop_upper)
        if match:
            ma_cn = f"CTS_{match.group(1)}K"
            ten_cn = f"Chuyên ngành CTS {match.group(1)}K"
        else:
            ma_cn = "CTS"
            ten_cn = "Chuyên ngành CTS"
        
        # Thuộc về Trường ĐH Kinh Tế
        return (ma_cn, ten_cn, "CN_CTS", "Chuyên ngành CTS", "TĐHKT", "Trường ĐH Kinh tế")
    
    # ===== TH2: CHỨA QT (không có CTS) =====
    if 'QT' in lop_upper:
        # Xử lý trường hợp 49KQT hoặc 50KQT
        match = re.search(r'(\d{2})KQT', lop_upper)
        if match:
            ma_cn = f"QT_{match.group(1)}K"
            ten_cn = f"Chuyên ngành QT {match.group(1)}K"
        else:
            ma_cn = "QT"
            ten_cn = "Chuyên ngành QT"
        
        # Thuộc về Phòng Đào Tạo
        return (ma_cn, ten_cn, "CN_QT", "Chuyên ngành QT", "PĐT", "Phòng Đào Tạo")
    
    # ===== TH3: LỚP THƯỜNG (Kxx) =====
    match = re.search(r'K(\d{2})', lop_upper)
    if match:
        ma_cn = f"K{match.group(1)}"
        ten_cn = f"Chuyên ngành K{match.group(1)}"
        return (ma_cn, ten_cn, f"CN_K{match.group(1)}", f"Chuyên ngành K{match.group(1)}", None, None)
    
    return None, None, None, None, None, None


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


# ================= LOAD EXISTING DATA FROM DIM TABLES =================
def load_existing_dim_data(cursor):
    """Load dữ liệu đã có từ các bảng DIM"""
    print("  -> Đọc dữ liệu existing từ DIM tables...")
    
    # DIM_KHOA
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    existing_khoa = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"     - DIM_KHOA: {len(existing_khoa)} dòng")
    
    # DIM_NGANH
    cursor.execute("SELECT MaNganh, TenNganh, MaKhoa FROM DIM_NGANH")
    existing_nganh = {row[0]: {'TenNganh': row[1], 'MaKhoa': row[2]} for row in cursor.fetchall()}
    print(f"     - DIM_NGANH: {len(existing_nganh)} dòng")
    
    # DIM_CHUYEN_NGANH
    cursor.execute("SELECT MaChuyenNganh, TenChuyenNganh, MaNganh FROM DIM_CHUYEN_NGANH")
    existing_chuyennganh = {row[0]: {'TenChuyenNganh': row[1], 'MaNganh': row[2]} for row in cursor.fetchall()}
    print(f"     - DIM_CHUYEN_NGANH: {len(existing_chuyennganh)} dòng")
    
    # DIM_HOC_PHAN
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hocphan = {row[0] for row in cursor.fetchall()}
    print(f"     - DIM_HOC_PHAN: {len(existing_hocphan)} dòng")
    
    return {
        'khoa': existing_khoa,
        'nganh': existing_nganh,
        'chuyennganh': existing_chuyennganh,
        'hocphan': existing_hocphan
    }


def insert_missing_dim_data(cursor, existing_dim):
    """Insert các giá trị còn thiếu vào DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH"""
    print("\n  -> Bổ sung dữ liệu còn thiếu vào DIM tables...")
    
    # ===== DIM_KHOA: Thêm nếu thiếu =====
    default_khoa = {
        'TĐHKT': 'Trường Đại học Kinh tế',
        'PĐT': 'Phòng Đào Tạo'
    }
    
    for ma_khoa, ten_khoa in default_khoa.items():
        if ma_khoa not in existing_dim['khoa']:
            cursor.execute("INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)", ma_khoa, ten_khoa)
            existing_dim['khoa'][ma_khoa] = ten_khoa
            print(f"        ✅ Đã thêm Khoa: {ma_khoa} - {ten_khoa}")
    
    # ===== DIM_NGANH: Thêm nếu thiếu =====
    default_nganh = {
        'CN_CTS': {'TenNganh': 'Chuyên ngành CTS', 'MaKhoa': 'TĐHKT'},
        'CN_QT': {'TenNganh': 'Chuyên ngành QT', 'MaKhoa': 'PĐT'}
    }
    
    for ma_nganh, info in default_nganh.items():
        if ma_nganh not in existing_dim['nganh']:
            cursor.execute("INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)", 
                          ma_nganh, info['TenNganh'], info['MaKhoa'])
            existing_dim['nganh'][ma_nganh] = info
            print(f"        ✅ Đã thêm Ngành: {ma_nganh} - {info['TenNganh']}")
    
    # ===== DIM_CHUYEN_NGANH: Thêm các chuyên ngành đặc biệt =====
    special_chuyennganh = [
        ('CTS', 'Chuyên ngành CTS', 'CN_CTS'),
        ('QT', 'Chuyên ngành QT', 'CN_QT')
    ]
    
    for ma_cn, ten_cn, ma_nganh in special_chuyennganh:
        if ma_cn not in existing_dim['chuyennganh'] and ma_nganh in existing_dim['nganh']:
            cursor.execute("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", 
                          ma_cn, ten_cn, ma_nganh)
            existing_dim['chuyennganh'][ma_cn] = {'TenChuyenNganh': ten_cn, 'MaNganh': ma_nganh}
            print(f"        ✅ Đã thêm Chuyên ngành: {ma_cn} - {ten_cn}")
    
    cursor.connection.commit()


# ================= NLP CLASS =================
class VietnameseNLP:
    def __init__(self):
        self.positive_words = {
            'tuyệt vời': 2.0, 'xuất sắc': 2.0, 'hoàn hảo': 2.0,
            'rất tốt': 1.5, 'rất hay': 1.5, 'cực kỳ': 1.5,
            'tốt': 1.0, 'hay': 1.0, 'ổn': 1.0, 'hài lòng': 1.0,
            'cảm ơn': 1.0, 'ok': 1.0, 'oke': 1.0,
            'tận tâm': 1.0, 'nhiệt tình': 1.0, 'dễ hiểu': 1.0
        }
        
        self.negative_words = {
            'tệ': -1.0, 'dở': -1.0, 'kém': -1.0, 'chán': -1.0,
            'khó hiểu': -1.0, 'lan man': -1.0, 'dài dòng': -1.0
        }
        
        self.no_opinion_patterns = [
            r'^không\s*(có)?\s*(gì)?\s*(ý\s*kiến)?\s*(góp\s*ý)?\s*$',
            r'^(ko|k|0|\.\.+|n/?a)$',
            r'^$'
        ]
        
        self.tag_keywords = {
            'Tag_HocPhan': ['chuẩn đầu ra', 'nội dung', 'học phần', 'môn học'],
            'Tag_DayHoc': ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng'],
            'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'thi', 'đề thi', 'điểm']
        }
        
        self.tag_hp_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_HocPhan'])
        self.tag_dh_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_DayHoc'])
        self.tag_kt_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_KiemTra'])
    
    def is_no_opinion(self, text: str) -> bool:
        if not isinstance(text, str):
            return True
        text_clean = text.lower().strip()
        for pattern in self.no_opinion_patterns:
            if re.match(pattern, text_clean):
                return True
        return False
    
    def analyze_sentiment_vectorized(self, texts):
        results = []
        for text in texts:
            if self.is_no_opinion(text):
                results.append('neutral')
            else:
                text_lower = text.lower()
                pos_score = sum(weight for word, weight in self.positive_words.items() if word in text_lower)
                neg_score = sum(weight for word, weight in self.negative_words.items() if word in text_lower)
                if pos_score + neg_score > 0.5:
                    results.append('positive')
                elif pos_score + neg_score < -0.5:
                    results.append('negative')
                else:
                    results.append('neutral')
        return results
    
    def extract_tags_vectorized(self, texts):
        series = pd.Series(texts)
        tag_hp = series.str.contains(self.tag_hp_regex, na=False, regex=True).astype(int)
        tag_dh = series.str.contains(self.tag_dh_regex, na=False, regex=True).astype(int)
        tag_kt = series.str.contains(self.tag_kt_regex, na=False, regex=True).astype(int)
        tag_khac = ((tag_hp + tag_dh + tag_kt) == 0).astype(int)
        
        for i, text in enumerate(texts):
            if self.is_no_opinion(text):
                tag_khac.iloc[i] = 1
        
        return list(zip(tag_hp, tag_dh, tag_kt, tag_khac))


_nlp = VietnameseNLP()


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


# ================= TRANSFORM & NLP =================
def transform_with_nlp_long_format(df_raw: pd.DataFrame) -> tuple:
    print("  -> Transform dữ liệu...")
    start = time.time()
    
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if text_df.empty:
        print("  ⚠️ Không có dữ liệu tự luận!")
        fact_main = pd.DataFrame()
    else:
        print(f"  -> Dòng tự luận thô: {len(text_df):,}")
        
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        print(f"  -> Sau loại bỏ trùng: {len(text_df_unique):,} submissions")
        
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        texts = text_df_unique['NoiDungGopY'].tolist()
        print(f"  -> Đang xử lý NLP cho {len(texts):,} bài tự luận...")
        
        text_df_unique['Sentiment'] = _nlp.analyze_sentiment_vectorized(texts)
        tag_vectors = _nlp.extract_tags_vectorized(texts)
        
        text_df_unique['Tag_HocPhan'] = [v[0] for v in tag_vectors]
        text_df_unique['Tag_DayHoc'] = [v[1] for v in tag_vectors]
        text_df_unique['Tag_KiemTra'] = [v[2] for v in tag_vectors]
        text_df_unique['Tag_Khac'] = [v[3] for v in tag_vectors]
        text_df_unique['Is_Valid'] = 1
        
        fact_main = text_df_unique[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid',
            'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].copy()
        
        duplicates_removed = len(text_df) - len(text_df_unique)
        if duplicates_removed > 0:
            print(f"  ✅ Đã loại bỏ {duplicates_removed:,} dòng trùng lặp")
    
    # Xử lý câu trắc nghiệm
    mcq_df = df_raw[
        df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
        df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != '')
    ].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua, df_raw


# ================= LOAD DIMENSIONS (CÁC BẢNG CÒN LẠI) =================
def load_remaining_dimensions(cursor, df_raw, existing_dim):
    """Load các bảng DIM còn lại: DIM_GIANG_VIEN, DIM_LOP_SINH_VIEN, DIM_SINH_VIEN, DIM_LOP_HOC_PHAN, DIM_HOC_KY"""
    
    print("\n📥 Loading các bảng DIM còn lại...")
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # ==========================================
    # 1. DIM_HOC_KY
    # ==========================================
    print("\n  -> 1. DIM_HOC_KY")
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                      ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {ma_hoc_ky}")
    else:
        print(f"     ✅ {ma_hoc_ky} đã tồn tại")
    
    # ==========================================
    # 2. DIM_GIANG_VIEN
    # ==========================================
    print("\n  -> 2. DIM_GIANG_VIEN")
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_gv = {row[0] for row in cursor.fetchall()}
    
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    data_gv = [(row['MaGV'], row['HoDemGV'] or '', row['TenGV'] or '') 
               for _, row in df_gv.iterrows() if row['MaGV'] not in existing_gv]
    
    if data_gv:
        cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", data_gv)
        cursor.connection.commit()
        print(f"     ✅ Đã insert {len(data_gv)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # 3. DIM_LOP_SINH_VIEN (XỬ LÝ ĐẶC BIỆT)
    # ==========================================
    print("\n  -> 3. DIM_LOP_SINH_VIEN (Xử lý đặc biệt cho CTS và QT)")
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    print(f"     - Tổng số lớp unique từ dữ liệu: {len(df_lop_unique)}")
    
    data_lop = []
    skipped_lop = []
    special_cases = {'CTS': [], 'QT': []}
    
    for _, row in df_lop_unique.iterrows():
        lop = row['Lop']
        if lop in existing_lop:
            continue
        
        # Xác định loại lớp đặc biệt
        lop_upper = lop.upper()
        is_cts = 'CTS' in lop_upper
        is_qt = 'QT' in lop_upper
        
        # Xử lý theo thứ tự ưu tiên: CTS > QT > Thường
        if is_cts:
            # TH1: Lớp có CTS (do Trường ĐH Kinh Tế quản lý)
            ma_cn, ten_cn, ma_nganh, ten_nganh, ma_khoa, ten_khoa = determine_ma_chuyen_nganh_special(lop)
            
            # Kiểm tra và thêm vào DIM_CHUYEN_NGANH nếu chưa có
            if ma_cn not in existing_dim['chuyennganh']:
                # Thêm vào DIM_CHUYEN_NGANH
                cursor.execute("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", 
                              ma_cn, ten_cn, ma_nganh)
                existing_dim['chuyennganh'][ma_cn] = {'TenChuyenNganh': ten_cn, 'MaNganh': ma_nganh}
                print(f"        ✅ Đã thêm Chuyên ngành mới: {ma_cn} - {ten_cn}")
            
            data_lop.append((lop, lop, ma_cn))
            special_cases['CTS'].append(lop)
            
        elif is_qt and not is_cts:
            # TH2: Lớp có QT (do Phòng Đào Tạo quản lý)
            ma_cn, ten_cn, ma_nganh, ten_nganh, ma_khoa, ten_khoa = determine_ma_chuyen_nganh_special(lop)
            
            # Kiểm tra và thêm vào DIM_CHUYEN_NGANH nếu chưa có
            if ma_cn not in existing_dim['chuyennganh']:
                cursor.execute("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", 
                              ma_cn, ten_cn, ma_nganh)
                existing_dim['chuyennganh'][ma_cn] = {'TenChuyenNganh': ten_cn, 'MaNganh': ma_nganh}
                print(f"        ✅ Đã thêm Chuyên ngành mới: {ma_cn} - {ten_cn}")
            
            data_lop.append((lop, lop, ma_cn))
            special_cases['QT'].append(lop)
            
        else:
            # TH3: Lớp thường (Kxx)
            match = re.search(r'K(\d{2})', lop_upper)
            if match:
                ma_cn = f"K{match.group(1)}"
                if ma_cn in existing_dim['chuyennganh']:
                    data_lop.append((lop, lop, ma_cn))
                else:
                    skipped_lop.append(f"{lop} (ma_cn={ma_cn} không tồn tại trong DIM_CHUYEN_NGANH)")
            else:
                skipped_lop.append(f"{lop} (không xác định được mã chuyên ngành)")
    
    # In thống kê các trường hợp đặc biệt
    if special_cases['CTS']:
        print(f"     📌 Lớp CTS (do Trường ĐH Kinh Tế quản lý): {len(special_cases['CTS'])} lớp")
        for lop in special_cases['CTS'][:5]:
            print(f"        - {lop}")
        if len(special_cases['CTS']) > 5:
            print(f"        ... và {len(special_cases['CTS']) - 5} lớp khác")
    
    if special_cases['QT']:
        print(f"     📌 Lớp QT (do Phòng Đào Tạo quản lý): {len(special_cases['QT'])} lớp")
        for lop in special_cases['QT'][:5]:
            print(f"        - {lop}")
        if len(special_cases['QT']) > 5:
            print(f"        ... và {len(special_cases['QT']) - 5} lớp khác")
    
    if skipped_lop:
        print(f"     ⚠️ Bỏ qua {len(skipped_lop)} lớp:")
        for lop in skipped_lop[:10]:
            print(f"        - {lop}")
        if len(skipped_lop) > 10:
            print(f"        ... và {len(skipped_lop) - 10} lớp khác")
    
    if data_lop:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", data_lop)
        cursor.connection.commit()
        print(f"     ✅ Đã insert {len(data_lop)} dòng mới vào DIM_LOP_SINH_VIEN")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # 4. DIM_SINH_VIEN
    # ==========================================
    print("\n  -> 4. DIM_SINH_VIEN")
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    valid_lop = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {row[0] for row in cursor.fetchall()}
    
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    
    data_sv = []
    skipped_sv = []
    
    for _, row in df_sv.iterrows():
        ma_sv = row['MaSV']
        lop = row['Lop']
        
        if ma_sv not in existing_sv:
            if lop not in valid_lop:
                skipped_sv.append(ma_sv)
                continue
            
            ngay_sinh = None
            if row['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            
            data_sv.append((ma_sv, row['HoDem'] or '', row['Ten'] or '', ngay_sinh, lop))
            existing_sv.add(ma_sv)
    
    if skipped_sv:
        print(f"     ⚠️ Bỏ qua {len(skipped_sv)} sinh viên (lớp không hợp lệ)")
    
    if data_sv:
        cursor.executemany("INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES (?, ?, ?, ?, ?)", data_sv)
        cursor.connection.commit()
        print(f"     ✅ Đã insert {len(data_sv)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # 5. DIM_LOP_HOC_PHAN
    # ==========================================
    print("\n  -> 5. DIM_LOP_HOC_PHAN")
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    valid_hp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    valid_gv = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    valid_hocky = cursor.fetchone() is not None
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {row[0] for row in cursor.fetchall()}
    
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    
    data_lhp = []
    skipped_lhp = []
    
    for _, row in df_lhp.iterrows():
        lop_hp = row['LopHP']
        if lop_hp not in existing_lhp:
            if row['MaHP'] not in valid_hp:
                skipped_lhp.append(f"{lop_hp} - MaHP={row['MaHP']} not exist")
                continue
            if row['MaGV'] not in valid_gv:
                skipped_lhp.append(f"{lop_hp} - MaGV={row['MaGV']} not exist")
                continue
            if not valid_hocky:
                skipped_lhp.append(f"{lop_hp} - HocKy={ma_hoc_ky} not exist")
                continue
            
            data_lhp.append((lop_hp, lop_hp, row['MaHP'], row['MaGV'], ma_hoc_ky))
            existing_lhp.add(lop_hp)
    
    if skipped_lhp:
        print(f"     ⚠️ Bỏ qua {len(skipped_lhp)} lớp học phần")
        for item in skipped_lhp[:5]:
            print(f"        - {item}")
    
    if data_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", data_lhp)
        cursor.connection.commit()
        print(f"     ✅ Đã insert {len(data_lhp)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    print("  ✅ Các bảng DIM còn lại đã được load xong!")
    return ma_hoc_ky


# ================= LOAD FACT TABLES =================
def load_fact_tables_optimized(cursor, fact_main, fact_ketqua, ma_hoc_ky):
    print("\n📥 Loading FACT tables...")
    start_time = time.time()
    
    # Kiểm tra dữ liệu hợp lệ
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    valid_sv = {row[0] for row in cursor.fetchall()}
    
    print(f"     - Số LopHP hợp lệ: {len(valid_lophp)}")
    print(f"     - Số MaSV hợp lệ: {len(valid_sv)}")
    
    # TẮT CONSTRAINTS tạm thời
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    count_main = 0
    count_kq = 0
    skipped_main = 0
    missing_count = 0
    
    try:
        cursor.execute("BEGIN TRANSACTION")
        
        # ===== 1. INSERT FACT_GOP_Y_TU_LUAN =====
        if not fact_main.empty:
            data_main = []
            for _, row in fact_main.iterrows():
                if row['MaSV'] not in valid_sv or row['LopHP'] not in valid_lophp:
                    skipped_main += 1
                    continue
                
                noi_dung = row['NoiDungGopY']
                if isinstance(noi_dung, str) and len(noi_dung) > 4000:
                    noi_dung = noi_dung[:4000]
                    
                data_main.append((
                    row['SubmissionID'], row['MaSV'], row['LopHP'], noi_dung,
                    row['Sentiment'], row['Is_Valid'],
                    row['Tag_HocPhan'], row['Tag_DayHoc'], row['Tag_KiemTra'], row['Tag_Khac']
                ))
            
            if data_main:
                sql_main = """INSERT INTO FACT_GOP_Y_TU_LUAN 
                             (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, 
                              Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) 
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                
                batch_size = 50000
                for i in range(0, len(data_main), batch_size):
                    batch = data_main[i:i+batch_size]
                    cursor.executemany(sql_main, batch)
                    count_main += len(batch)
                    print(f"      ✅ FACT_GOP_Y_TU_LUAN: {count_main:,}/{len(data_main):,} dòng")
                cursor.connection.commit()
        
        # ===== 2. INSERT FACT_KET_QUA_DANH_GIA =====
        if not fact_ketqua.empty:
            print(f"      -> Dữ liệu thô: {len(fact_ketqua):,} dòng")
            
            # Lấy tất cả SubmissionID hợp lệ
            cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
            valid_submissions = {row[0] for row in cursor.fetchall()}
            
            # TẠO DỮ LIỆU ĐẦY ĐỦ CHO 12 CÂU HỎI
            all_questions = list(range(1, 13))
            max_score = 5
            
            # Gom dữ liệu theo SubmissionID
            submission_data = {}
            for _, row in fact_ketqua.iterrows():
                sub_id = row['SubmissionID']
                cau_hoi = row['MaCauHoi']
                diem = row['Diem']
                
                if sub_id not in submission_data:
                    submission_data[sub_id] = {}
                submission_data[sub_id][cau_hoi] = diem
            
            # Tạo dữ liệu hoàn chỉnh
            complete_data = []
            for sub_id in valid_submissions:
                if sub_id in submission_data:
                    existing_answers = submission_data[sub_id]
                    for cau_hoi in all_questions:
                        if cau_hoi in existing_answers:
                            diem = existing_answers[cau_hoi]
                        else:
                            diem = max_score
                            missing_count += 1
                        complete_data.append((sub_id, cau_hoi, diem))
                else:
                    for cau_hoi in all_questions:
                        complete_data.append((sub_id, cau_hoi, max_score))
                    missing_count += 12
            
            print(f"      -> Tổng số câu sau khi bổ sung: {len(complete_data):,} dòng")
            if missing_count > 0:
                print(f"      -> Đã bổ sung {missing_count:,} câu bị thiếu (giá trị = {max_score})")
            
            # Xử lý duplicate: lấy giá trị lớn nhất
            from collections import defaultdict
            unique_data = defaultdict(int)
            for sub_id, cau_hoi, diem in complete_data:
                key = (sub_id, cau_hoi)
                if diem > unique_data[key]:
                    unique_data[key] = diem
            
            final_data = [(sub_id, cau_hoi, diem) for (sub_id, cau_hoi), diem in unique_data.items()]
            
            duplicates_removed = len(complete_data) - len(final_data)
            if duplicates_removed > 0:
                print(f"      ⚠️ Đã xử lý {duplicates_removed:,} dòng duplicate (giữ giá trị lớn nhất)")
            
            # Insert dữ liệu
            if final_data:
                sql_kq = """INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) 
                            VALUES (?, ?, ?)"""
                
                batch_size = 100000
                for i in range(0, len(final_data), batch_size):
                    batch = final_data[i:i+batch_size]
                    cursor.executemany(sql_kq, batch)
                    count_kq += len(batch)
                    print(f"      ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,}/{len(final_data):,} dòng")
                
                cursor.connection.commit()
        
        cursor.execute("COMMIT")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"  ❌ Lỗi: {e}")
        raise
    
    # BẬT LẠI CONSTRAINTS
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    elapsed = time.time() - start_time
    print(f"  ✅ FACT tables loaded:")
    print(f"     - FACT_GOP_Y_TU_LUAN: {count_main:,} submissions")
    print(f"     - FACT_KET_QUA_DANH_GIA: {count_kq:,} answers")
    
    if skipped_main > 0:
        print(f"     ⚠️ Bỏ qua: {skipped_main} submissions (FK lỗi)")
    
    print(f"     ⏱️  Thời gian: {elapsed:.1f}s")
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - XỬ LÝ ĐẶC BIỆT CTS & QT")
    print("=" * 60)
    print("⚠️  Các bảng đã có sẵn: DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH, DIM_HOC_PHAN")
    print("📌 Xử lý đặc biệt:")
    print("   - Lớp có CTS -> Trường ĐH Kinh Tế quản lý")
    print("   - Lớp có QT -> Phòng Đào Tạo quản lý")
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
    
    # 2. Đọc dữ liệu survey
    print(f"\n📥 2. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    # 3. Parse dữ liệu
    print("\n📝 3. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    print(f"  ✅ Parse: {len(df_raw):,} dòng câu trả lời trong {parse_time:.1f}s")
    
    # 4. Transform & NLP
    print("\n🔄 4. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua, df_raw = transform_with_nlp_long_format(df_raw)
    transform_time = time.time() - transform_start
    print(f"  ✅ Transform: {transform_time:.1f}s")
    
    # 5. Lưu backup CSV
    print("\n💾 5. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    if not fact_ketqua.empty:
        save_processed(blob_service, fact_ketqua, f"{FILE_NAME}_ketqua_{timestamp}.csv")
    
    # 6. Kết nối Database
    print("\n💾 6. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Kết nối SQL thành công")
    except Exception as e:
        print(f"  ❌ Lỗi kết nối SQL: {e}")
        return
    
    db_start = time.time()
    count_main = 0
    count_kq = 0
    
    try:
        # 7. Load dữ liệu existing từ các DIM đã có
        existing_dim = load_existing_dim_data(cursor)
        
        # 8. Bổ sung dữ liệu còn thiếu vào DIM tables
        insert_missing_dim_data(cursor, existing_dim)
        
        # 9. Load các bảng DIM còn lại (bao gồm xử lý đặc biệt)
        ma_hoc_ky = load_remaining_dimensions(cursor, df_raw, existing_dim)
        
        # 10. Load FACT tables
        count_main, count_kq = load_fact_tables_optimized(cursor, fact_main, fact_ketqua, ma_hoc_ky)
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    db_time = time.time() - db_start
    
    # 11. Thống kê
    total_time = time.time() - total_start
    print("\n📊 11. KẾT QUẢ:")
    print(f"   - Dòng dữ liệu thô: {len(df_raw):,}")
    print(f"   - Số phiếu tự luận: {len(fact_main):,}")
    print(f"   - Số câu trắc nghiệm: {count_kq:,}")
    
    if not fact_main.empty:
        print("\n   - Sentiment phân bố:")
        for sent, cnt in fact_main['Sentiment'].value_counts().items():
            pct = cnt/len(fact_main)*100
            print(f"      {sent}: {cnt:,} ({pct:.1f}%)")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH! Thời gian: {total_time:.1f}s")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform: {transform_time:.1f}s")
    print(f"   - Database: {db_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
