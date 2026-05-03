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


def is_special_lop(lop: str) -> tuple:
    """
    Kiểm tra lớp có phải là lớp đặc biệt (chứa CTS hoặc QT)
    Returns: (is_special, loai, ma_khoa)
    """
    if not lop or not isinstance(lop, str):
        return (False, None, None)
    
    lop_upper = lop.upper().strip()
    
    # TH1: Lop có chứa CTS -> gán cho Trường ĐH Kinh Tế (KHOA19)
    if 'CTS' in lop_upper:
        return (True, 'CTS', 'KHOA19')
    
    # TH2: Lop có chứa QT -> gán cho Phòng Đào Tạo (KHOA11)
    if 'QT' in lop_upper:
        return (True, 'QT', 'KHOA11')
    
    return (False, None, None)


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


# ================= LOAD EXISTING DATA =================
def load_all_existing_data(cursor):
    """Load tất cả existing data từ database"""
    print("  -> Đang load existing data từ database...")
    start = time.time()
    
    # DIM_KHOA
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    existing_khoa = {row[0]: row[1] for row in cursor.fetchall()}
    print(f"     - DIM_KHOA: {existing_khoa}")
    
    # DIM_NGANH
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh = {row[0] for row in cursor.fetchall()}
    
    # DIM_CHUYEN_NGANH
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_chuyennganh = {row[0] for row in cursor.fetchall()}
    
    # DIM_HOC_PHAN
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hocphan = {row[0] for row in cursor.fetchall()}
    
    # DIM_GIANG_VIEN
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_giangvien = {row[0] for row in cursor.fetchall()}
    
    # DIM_LOP_SINH_VIEN
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    # DIM_SINH_VIEN
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sinhvien = {row[0] for row in cursor.fetchall()}
    
    # DIM_LOP_HOC_PHAN
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lophp = {row[0] for row in cursor.fetchall()}
    
    # DIM_HOC_KY
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
    existing_hocky = {row[0] for row in cursor.fetchall()}
    
    print(f"     ✅ Loaded in {time.time()-start:.1f}s")
    print(f"     - DIM_NGANH: {len(existing_nganh)} dòng")
    print(f"     - DIM_CHUYEN_NGANH: {len(existing_chuyennganh)} dòng")
    
    return {
        'khoa': existing_khoa,
        'nganh': existing_nganh,
        'chuyennganh': existing_chuyennganh,
        'hocphan': existing_hocphan,
        'giangvien': existing_giangvien,
        'lop': existing_lop,
        'sinhvien': existing_sinhvien,
        'lophp': existing_lophp,
        'hocky': existing_hocky
    }


def create_null_dim_data(cursor, existing_data):
    """
    Tạo dòng dữ liệu NULL cho DIM_NGANH và DIM_CHUYEN_NGANH 
    cho các lớp đặc biệt CTS và QT (chỉ tạo 1 lần duy nhất)
    """
    print("\n  -> Tạo dòng dữ liệu NULL cho lớp đặc biệt CTS và QT...")
    
    # Dữ liệu NULL cho CTS (KHOA19 - Trường ĐH Kinh Tế)
    cts_ma_nganh = 'NULL_CTS'
    cts_ten_nganh = ''
    cts_ma_chuyen_nganh = 'NULL_CTS'
    cts_ten_chuyen_nganh = ''
    cts_ma_khoa = 'KHOA19'
    
    # Dữ liệu NULL cho QT (KHOA11 - Phòng Đào Tạo)
    qt_ma_nganh = 'NULL_QT'
    qt_ten_nganh = ''
    qt_ma_chuyen_nganh = 'NULL_QT'
    qt_ten_chuyen_nganh = ''
    qt_ma_khoa = 'KHOA11'
    
    # Tạo cho CTS
    if cts_ma_nganh not in existing_data['nganh']:
        try:
            cursor.execute("INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)", 
                          cts_ma_nganh, cts_ten_nganh, cts_ma_khoa)
            existing_data['nganh'].add(cts_ma_nganh)
            print(f"        ✅ Đã tạo Ngành NULL cho CTS: {cts_ma_nganh}")
        except Exception as e:
            print(f"        ⚠️ Lỗi tạo Ngành CTS: {e}")
    
    if cts_ma_chuyen_nganh not in existing_data['chuyennganh']:
        try:
            cursor.execute("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", 
                          cts_ma_chuyen_nganh, cts_ten_chuyen_nganh, cts_ma_nganh)
            existing_data['chuyennganh'].add(cts_ma_chuyen_nganh)
            print(f"        ✅ Đã tạo Chuyên ngành NULL cho CTS: {cts_ma_chuyen_nganh}")
        except Exception as e:
            print(f"        ⚠️ Lỗi tạo Chuyên ngành CTS: {e}")
    
    # Tạo cho QT
    if qt_ma_nganh not in existing_data['nganh']:
        try:
            cursor.execute("INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)", 
                          qt_ma_nganh, qt_ten_nganh, qt_ma_khoa)
            existing_data['nganh'].add(qt_ma_nganh)
            print(f"        ✅ Đã tạo Ngành NULL cho QT: {qt_ma_nganh}")
        except Exception as e:
            print(f"        ⚠️ Lỗi tạo Ngành QT: {e}")
    
    if qt_ma_chuyen_nganh not in existing_data['chuyennganh']:
        try:
            cursor.execute("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", 
                          qt_ma_chuyen_nganh, qt_ten_chuyen_nganh, qt_ma_nganh)
            existing_data['chuyennganh'].add(qt_ma_chuyen_nganh)
            print(f"        ✅ Đã tạo Chuyên ngành NULL cho QT: {qt_ma_chuyen_nganh}")
        except Exception as e:
            print(f"        ⚠️ Lỗi tạo Chuyên ngành QT: {e}")
    
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
    
    def is_no_opinion(self, text):
        if not isinstance(text, str):
            return True
        text_clean = text.lower().strip()
        return any(re.match(p, text_clean) for p in self.no_opinion_patterns)
    
    def process_batch(self, texts):
        sentiments = []
        tags = []
        
        for text in texts:
            if self.is_no_opinion(text):
                sentiments.append('neutral')
                tags.append((0, 0, 0, 1))
                continue
            
            text_lower = text.lower()
            pos_score = sum(w for word, w in self.positive_words.items() if word in text_lower)
            neg_score = sum(w for word, w in self.negative_words.items() if word in text_lower)
            
            if pos_score + neg_score > 0.35:
                sentiments.append('positive')
            elif pos_score + neg_score < -0.35:
                sentiments.append('negative')
            else:
                sentiments.append('neutral')
            
            tag_hp = 1 if re.search(self.tag_hp_regex, text_lower) else 0
            tag_dh = 1 if re.search(self.tag_dh_regex, text_lower) else 0
            tag_kt = 1 if re.search(self.tag_kt_regex, text_lower) else 0
            tag_khac = 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0
            
            tags.append((tag_hp, tag_dh, tag_kt, tag_khac))
        
        return sentiments, tags


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
            
            ho_dem, ten = '', ''
            if ngay_sinh_index > 1:
                name_parts = [p for p in row[2:ngay_sinh_index] if p]
                if name_parts:
                    ten = name_parts[-1]
                    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else ''
            ma_gv, ma_gv_index = '', -1
            
            for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
                if is_ma_gv_format(row[i]):
                    ma_gv, ma_gv_index = row[i], i
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
                'SubmissionID': submission_id, 'Lop': lop, 'MaSV': ma_sv,
                'HoDem': ho_dem, 'Ten': ten, 'NgaySinh': ngay_sinh,
                'MaHP': ma_hp, 'TenHP': ten_hp, 'MaGV': ma_gv,
                'HoDemGV': ho_dem_gv, 'TenGV': ten_gv, 'LopHP': lop_hp,
                'CauHoi': cau_hoi, 'GiaTri': gia_tri, 'EssayText': essay_text
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
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.1f}s)")
    return df


# ================= TRANSFORM =================
def transform_with_nlp_optimized(df_raw: pd.DataFrame) -> tuple:
    print("  -> Transform dữ liệu (batch processing)...")
    start = time.time()
    
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        texts = text_df_unique['NoiDungGopY'].tolist()
        sentiments, tags = _nlp.process_batch(texts)
        
        text_df_unique['Sentiment'] = sentiments
        text_df_unique['Tag_HocPhan'] = [t[0] for t in tags]
        text_df_unique['Tag_DayHoc'] = [t[1] for t in tags]
        text_df_unique['Tag_KiemTra'] = [t[2] for t in tags]
        text_df_unique['Tag_Khac'] = [t[3] for t in tags]
        text_df_unique['Is_Valid'] = 1
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].copy()
    
    mcq_df = df_raw[df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '')].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.1f}s)")
    return fact_main, fact_ketqua, df_raw


# ================= LOAD DIMENSIONS =================
def load_remaining_dimensions_optimized(cursor, df_raw, existing_data, ma_hoc_ky, nam_hoc, hoc_ky):
    """Load các bảng DIM còn lại"""
    print("\n📥 Loading các bảng DIM còn lại...")
    
    # 1. DIM_HOC_KY
    if ma_hoc_ky not in existing_data['hocky']:
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                       ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {ma_hoc_ky} vào DIM_HOC_KY")
    else:
        print(f"     ✅ {ma_hoc_ky} đã tồn tại")
    
    # 2. DIM_GIANG_VIEN
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    new_gv = [(r['MaGV'], r['HoDemGV'] or '', r['TenGV'] or '') 
              for _, r in df_gv.iterrows() if r['MaGV'] not in existing_data['giangvien']]
    if new_gv:
        cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", new_gv)
        cursor.connection.commit()
        print(f"     ✅ Thêm {len(new_gv)} giảng viên mới")
    else:
        print(f"     ✅ Không có giảng viên mới")
    
    # 3. DIM_LOP_SINH_VIEN - XỬ LÝ ĐẶC BIỆT
    print("\n  -> 3. XỬ LÝ ĐẶC BIỆT DIM_LOP_SINH_VIEN")
    print("     - Lớp có chứa 'CTS' -> DIM Ngành & DIM Chuyên ngành NULL, gán cho Trường ĐH Kinh Tế (KHOA19)")
    print("     - Lớp có chứa 'QT' -> DIM Ngành & DIM Chuyên ngành NULL, gán cho Phòng Đào Tạo (KHOA11)")
    
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    print(f"     - Tổng số lớp unique: {len(df_lop_unique)}")
    
    # Thống kê
    special_cts = []
    special_qt = []
    normal_lops = []
    skipped_lops = []
    
    new_lop_data = []
    
    for _, row in df_lop_unique.iterrows():
        lop = row['Lop']
        
        if lop in existing_data['lop']:
            continue
        
        # Kiểm tra lớp đặc biệt
        is_special, loai, ma_khoa = is_special_lop(lop)
        
        if is_special:
            # Lớp đặc biệt: Sử dụng chuyên ngành NULL
            if loai == 'CTS':
                ma_chuyen_nganh = 'NULL_CTS'
                special_cts.append(lop)
            else:
                ma_chuyen_nganh = 'NULL_QT'
                special_qt.append(lop)
            
            if ma_chuyen_nganh in existing_data['chuyennganh']:
                new_lop_data.append((lop, lop, ma_chuyen_nganh))
            else:
                skipped_lops.append(f"{lop} (Chuyên ngành {ma_chuyen_nganh} chưa được tạo)")
        else:
            # Lớp thường: Kiểm tra trong DIM_CHUYEN_NGANH
            match = re.search(r'K(\d{2})', lop.upper())
            if match:
                ma_cn = f"K{match.group(1)}"
                if ma_cn in existing_data['chuyennganh']:
                    new_lop_data.append((lop, lop, ma_cn))
                    normal_lops.append(lop)
                else:
                    skipped_lops.append(f"{lop} (MaChuyenNganh={ma_cn} không tồn tại trong DIM_CHUYEN_NGANH)")
            else:
                skipped_lops.append(f"{lop} (không xác định được mã chuyên ngành)")
    
    # In thống kê
    if special_cts:
        print(f"     📌 Lớp CTS (KHOA19 - Trường ĐH Kinh Tế) - Chuyên ngành NULL: {len(special_cts)} lớp")
        for lop in special_cts[:10]:
            print(f"        - {lop}")
        if len(special_cts) > 10:
            print(f"        ... và {len(special_cts) - 10} lớp khác")
    
    if special_qt:
        print(f"     📌 Lớp QT (KHOA11 - Phòng Đào Tạo) - Chuyên ngành NULL: {len(special_qt)} lớp")
        for lop in special_qt[:10]:
            print(f"        - {lop}")
        if len(special_qt) > 10:
            print(f"        ... và {len(special_qt) - 10} lớp khác")
    
    if normal_lops:
        print(f"     📌 Lớp thường (Kxx): {len(normal_lops)} lớp")
    
    if skipped_lops:
        print(f"     ⚠️ Bỏ qua {len(skipped_lops)} lớp:")
        for lop in skipped_lops[:10]:
            print(f"        - {lop}")
        if len(skipped_lops) > 10:
            print(f"        ... và {len(skipped_lops) - 10} lớp khác")
    
    if new_lop_data:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop_data)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {len(new_lop_data)} lớp mới vào DIM_LOP_SINH_VIEN")
    else:
        print(f"     ✅ Không có lớp mới")
    
    # Refresh existing data
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_data['lop'] = {row[0] for row in cursor.fetchall()}
    
    # 4. DIM_SINH_VIEN
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    new_sv = []
    for _, r in df_sv.iterrows():
        if r['MaSV'] not in existing_data['sinhvien'] and r['Lop'] in existing_data['lop']:
            ngay_sinh = None
            if r['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(r['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            new_sv.append((r['MaSV'], r['HoDem'] or '', r['Ten'] or '', ngay_sinh, r['Lop']))
    
    if new_sv:
        cursor.executemany("INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES (?, ?, ?, ?, ?)", new_sv)
        cursor.connection.commit()
        print(f"     ✅ Thêm {len(new_sv)} sinh viên mới")
    else:
        print(f"     ✅ Không có sinh viên mới")
    
    # 5. DIM_LOP_HOC_PHAN
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    new_lhp = []
    for _, r in df_lhp.iterrows():
        if (r['LopHP'] not in existing_data['lophp'] and 
            r['MaHP'] in existing_data['hocphan'] and 
            r['MaGV'] in existing_data['giangvien']):
            new_lhp.append((r['LopHP'], r['LopHP'], r['MaHP'], r['MaGV'], ma_hoc_ky))
    
    if new_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", new_lhp)
        cursor.connection.commit()
        print(f"     ✅ Thêm {len(new_lhp)} lớp học phần mới")
    else:
        print(f"     ✅ Không có lớp học phần mới")
    
    print("  ✅ Các bảng DIM còn lại đã được load xong!")


# ================= LOAD FACT TABLES =================
def load_fact_tables_optimized(cursor, fact_main, fact_ketqua, existing_data, ma_hoc_ky):
    print("\n📥 Loading FACT tables...")
    start_time = time.time()
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    valid_sv = existing_data['sinhvien']
    
    print(f"     - Số LopHP hợp lệ: {len(valid_lophp)}")
    print(f"     - Số MaSV hợp lệ: {len(valid_sv)}")
    
    # TẮT CONSTRAINTS
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    count_main = 0
    count_kq = 0
    
    try:
        cursor.execute("BEGIN TRANSACTION")
        
        # FACT_GOP_Y_TU_LUAN
        if not fact_main.empty:
            data_main = []
            for _, row in fact_main.iterrows():
                if row['MaSV'] not in valid_sv or row['LopHP'] not in valid_lophp:
                    continue
                noi_dung = row['NoiDungGopY'][:4000] if isinstance(row['NoiDungGopY'], str) else ''
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
                cursor.executemany(sql_main, data_main)
                count_main = len(data_main)
                print(f"      ✅ FACT_GOP_Y_TU_LUAN: {count_main:,} dòng")
        
        # FACT_KET_QUA_DANH_GIA
        if not fact_ketqua.empty:
            cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
            valid_subs = {row[0] for row in cursor.fetchall()}
            
            if valid_subs:
                all_questions = list(range(1, 13))
                submission_data = {}
                
                for _, row in fact_ketqua.iterrows():
                    sub_id = row['SubmissionID']
                    if sub_id not in valid_subs:
                        continue
                    if sub_id not in submission_data:
                        submission_data[sub_id] = {}
                    submission_data[sub_id][row['MaCauHoi']] = row['Diem']
                
                final_data = []
                missing_count = 0
                
                for sub_id in valid_subs:
                    answers = submission_data.get(sub_id, {})
                    for q in all_questions:
                        diem = answers.get(q, 5)
                        if q not in answers:
                            missing_count += 1
                        final_data.append((sub_id, q, diem))
                
                # Loại bỏ duplicate
                unique_data = {}
                for sub_id, q, diem in final_data:
                    key = (sub_id, q)
                    if key not in unique_data or diem > unique_data[key]:
                        unique_data[key] = diem
                
                final_unique = [(k[0], k[1], v) for k, v in unique_data.items()]
                
                if final_unique:
                    sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
                    cursor.executemany(sql_kq, final_unique)
                    count_kq = len(final_unique)
                    print(f"      ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,} dòng (đã bổ sung {missing_count} câu thiếu)")
        
        cursor.execute("COMMIT")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        # BẬT LẠI CONSTRAINTS
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.connection.commit()
    
    elapsed = time.time() - start_time
    print(f"  ✅ FACT loaded in {elapsed:.1f}s")
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - XỬ LÝ LỚP ĐẶC BIỆT (CTS/QT) VỚI DỮ LIỆU NULL")
    print("=" * 60)
    print("📌 Xử lý đặc biệt:")
    print("   - Lớp có chứa 'CTS' -> DIM Ngành & DIM Chuyên ngành = NULL, gán cho Trường ĐH Kinh Tế (KHOA19)")
    print("   - Lớp có chứa 'QT' -> DIM Ngành & DIM Chuyên ngành = NULL, gán cho Phòng Đào Tạo (KHOA11)")
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
    print(f"  ✅ Parse: {len(df_raw):,} dòng trong {parse_time:.1f}s")
    
    # 4. Transform & NLP
    print("\n🔄 4. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua, df_raw = transform_with_nlp_optimized(df_raw)
    transform_time = time.time() - transform_start
    print(f"  ✅ Transform: {transform_time:.1f}s")
    
    # 5. Lưu backup
    print("\n💾 5. Lưu backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    
    # 6. Kết nối Database
    print("\n💾 6. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Kết nối thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    db_start = time.time()
    count_main = 0
    count_kq = 0
    
    try:
        # 7. Load existing data
        existing_data = load_all_existing_data(cursor)
        
        # 8. Tạo dữ liệu NULL cho CTS và QT (chỉ 1 lần)
        create_null_dim_data(cursor, existing_data)
        
        # Refresh existing data sau khi tạo NULL
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing_data['chuyennganh'] = {row[0] for row in cursor.fetchall()}
        
        # 9. Lấy thông tin học kỳ
        ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
        
        # 10. Load các bảng DIM còn lại
        load_remaining_dimensions_optimized(cursor, df_raw, existing_data, ma_hoc_ky, nam_hoc, hoc_ky)
        
        # Refresh existing data
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing_data['sinhvien'] = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing_data['lophp'] = {row[0] for row in cursor.fetchall()}
        
        # 11. Load FACT tables
        count_main, count_kq = load_fact_tables_optimized(cursor, fact_main, fact_ketqua, existing_data, ma_hoc_ky)
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    db_time = time.time() - db_start
    total_time = time.time() - total_start
    
    print("\n" + "=" * 60)
    print("📊 KẾT QUẢ:")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform: {transform_time:.1f}s")
    print(f"   - Database: {db_time:.1f}s")
    print(f"   - TOTAL: {total_time:.1f}s")
    print(f"   - Submissions: {count_main:,}")
    print(f"   - Answers: {count_kq:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
