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


def determine_ma_chuyen_nganh_batch(lop_series):
    """Xử lý batch để có tốc độ nhanh hơn"""
    results = []
    for lop in lop_series:
        if not lop or not isinstance(lop, str):
            results.append((None, None))
            continue
        
        lop_upper = lop.upper().strip()
        
        # TH1: Chứa CTS
        if 'CTS' in lop_upper:
            match = re.search(r'CTS[-_]?(\d{2})K', lop_upper)
            if match:
                ma_cn = f"CTS_{match.group(1)}K"
            else:
                ma_cn = "CTS"
            results.append((ma_cn, 'TĐHKT'))  # Trường ĐH Kinh Tế
            continue
        
        # TH2: Chứa QT (không có CTS)
        if 'QT' in lop_upper:
            match = re.search(r'(\d{2})KQT', lop_upper)
            if match:
                ma_cn = f"QT_{match.group(1)}K"
            else:
                ma_cn = "QT"
            results.append((ma_cn, 'PĐT'))  # Phòng Đào Tạo
            continue
        
        # TH3: Lớp thường Kxx
        match = re.search(r'K(\d{2})', lop_upper)
        if match:
            ma_cn = f"K{match.group(1)}"
            results.append((ma_cn, None))
        else:
            results.append((None, None))
    
    return results


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


# ================= LOAD EXISTING DATA (CHỈ 1 LẦN DUY NHẤT) =================
def load_all_existing_data(cursor):
    """Load tất cả existing data vào dictionary để tránh query nhiều lần"""
    print("  -> Đang load existing data từ database...")
    start = time.time()
    
    # DIM_KHOA
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing_khoa = {row[0] for row in cursor.fetchall()}
    
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
    
    print(f"     ✅ Loaded in {time.time()-start:.1f}s")
    
    return {
        'khoa': existing_khoa,
        'nganh': existing_nganh,
        'chuyennganh': existing_chuyennganh,
        'hocphan': existing_hocphan,
        'giangvien': existing_giangvien,
        'lop': existing_lop,
        'sinhvien': existing_sinhvien,
        'lophp': existing_lophp
    }


def insert_missing_data_batch(cursor, existing_data):
    """Insert dữ liệu thiếu bằng batch (nhanh hơn nhiều)"""
    print("\n  -> Bổ sung dữ liệu còn thiếu...")
    
    # DIM_KHOA cần thiết
    required_khoa = {
        'TĐHKT': 'Trường Đại học Kinh tế',
        'PĐT': 'Phòng Đào Tạo'
    }
    
    new_khoa = [(k, v) for k, v in required_khoa.items() if k not in existing_data['khoa']]
    if new_khoa:
        cursor.executemany("INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)", new_khoa)
        print(f"        ✅ Thêm {len(new_khoa)} khoa mới")
    
    # DIM_NGANH cần thiết
    required_nganh = {
        'CN_CTS': ('Chuyên ngành CTS', 'TĐHKT'),
        'CN_QT': ('Chuyên ngành QT', 'PĐT')
    }
    
    new_nganh = [(k, v[0], v[1]) for k, v in required_nganh.items() if k not in existing_data['nganh']]
    if new_nganh:
        cursor.executemany("INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)", new_nganh)
        print(f"        ✅ Thêm {len(new_nganh)} ngành mới")
    
    # DIM_CHUYEN_NGANH cần thiết
    required_chuyennganh = [
        ('CTS', 'Chuyên ngành CTS', 'CN_CTS'),
        ('QT', 'Chuyên ngành QT', 'CN_QT')
    ]
    
    new_cn = [(m, t, n) for m, t, n in required_chuyennganh if m not in existing_data['chuyennganh']]
    if new_cn:
        cursor.executemany("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", new_cn)
        print(f"        ✅ Thêm {len(new_cn)} chuyên ngành mới")
    
    cursor.connection.commit()


# ================= NLP CLASS =================
class VietnameseNLP:
    def __init__(self):
        self.positive_words = {'tuyệt vời': 2.0, 'xuất sắc': 2.0, 'tốt': 1.0, 'hay': 1.0, 'ổn': 1.0, 'cảm ơn': 1.0}
        self.negative_words = {'tệ': -1.0, 'dở': -1.0, 'kém': -1.0, 'chán': -1.0, 'khó hiểu': -1.0}
        
        self.no_opinion_patterns = [
            r'^không\s*(có)?\s*(gì)?\s*(ý\s*kiến)?\s*(góp\s*ý)?\s*$',
            r'^(ko|k|0|\.\.+|n/?a)$', r'^$'
        ]
        
        self.tag_keywords = {
            'Tag_HocPhan': ['chuẩn đầu ra', 'nội dung', 'học phần', 'môn học'],
            'Tag_DayHoc': ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng'],
            'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'thi', 'đề thi']
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
        """Xử lý batch nhanh"""
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
            
            if pos_score + neg_score > 0.5:
                sentiments.append('positive')
            elif pos_score + neg_score < -0.5:
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


# ================= PARSE SURVEY DATA (TỐI ƯU) =================
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


# ================= TRANSFORM (TỐI ƯU BATCH) =================
def transform_with_nlp_optimized(df_raw: pd.DataFrame) -> tuple:
    print("  -> Transform dữ liệu (batch processing)...")
    start = time.time()
    
    # Xử lý tự luận
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if text_df.empty:
        fact_main = pd.DataFrame()
    else:
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # Batch NLP processing
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
    
    # Xử lý trắc nghiệm
    mcq_df = df_raw[df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '')].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.1f}s)")
    return fact_main, fact_ketqua, df_raw


# ================= LOAD DIM REMAINING (TỐI ƯU BATCH) =================
def load_remaining_dimensions_optimized(cursor, df_raw, existing_data, ma_hoc_ky, nam_hoc, hoc_ky):
    """Load các bảng DIM còn lại bằng batch insert"""
    print("\n📥 Loading các bảng DIM còn lại...")
    
    # 1. DIM_HOC_KY
    if ma_hoc_ky not in existing_data.get('hocky', set()):
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                       ma_hoc_ky, nam_hoc, hoc_ky)
        print(f"     ✅ Đã thêm {ma_hoc_ky}")
    
    # 2. DIM_GIANG_VIEN - BATCH
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    new_gv = [(r['MaGV'], r['HoDemGV'] or '', r['TenGV'] or '') 
              for _, r in df_gv.iterrows() if r['MaGV'] not in existing_data['giangvien']]
    if new_gv:
        cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", new_gv)
        print(f"     ✅ Thêm {len(new_gv)} giảng viên mới")
    
    # 3. DIM_LOP_SINH_VIEN - BATCH với xử lý đặc biệt
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    lops = df_lop_unique['Lop'].tolist()
    
    # Batch xác định chuyên ngành
    cn_results = determine_ma_chuyen_nganh_batch(lops)
    
    new_lop_data = []
    for lop, (ma_cn, ma_khoa) in zip(lops, cn_results):
        if lop in existing_data['lop']:
            continue
        if ma_cn and ma_cn in existing_data['chuyennganh']:
            new_lop_data.append((lop, lop, ma_cn))
    
    if new_lop_data:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop_data)
        print(f"     ✅ Thêm {len(new_lop_data)} lớp mới")
    
    # Cập nhật existing data sau khi insert
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_data['lop'] = {row[0] for row in cursor.fetchall()}
    
    # 4. DIM_SINH_VIEN - BATCH
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
        print(f"     ✅ Thêm {len(new_sv)} sinh viên mới")
    
    # 5. DIM_LOP_HOC_PHAN - BATCH
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    new_lhp = []
    for _, r in df_lhp.iterrows():
        if (r['LopHP'] not in existing_data['lophp'] and 
            r['MaHP'] in existing_data['hocphan'] and 
            r['MaGV'] in existing_data['giangvien']):
            new_lhp.append((r['LopHP'], r['LopHP'], r['MaHP'], r['MaGV'], ma_hoc_ky))
    
    if new_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", new_lhp)
        print(f"     ✅ Thêm {len(new_lhp)} lớp học phần mới")
    
    cursor.connection.commit()
    print("  ✅ Các bảng DIM còn lại đã được load xong!")


# ================= LOAD FACT TABLES (TỐI ƯU) =================
def load_fact_tables_optimized(cursor, fact_main, fact_ketqua, existing_data, ma_hoc_ky):
    print("\n📥 Loading FACT tables...")
    start_time = time.time()
    
    # Lấy danh sách hợp lệ
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    valid_sv = existing_data['sinhvien']
    
    print(f"     - Số LopHP hợp lệ: {len(valid_lophp)}")
    print(f"     - Số MaSV hợp lệ: {len(valid_sv)}")
    
    # TẮT CONSTRAINTS
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    count_main = count_kq = 0
    
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
            # Lấy submission hợp lệ
            cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
            valid_subs = {row[0] for row in cursor.fetchall()}
            
            # Tạo data đầy đủ 12 câu
            all_questions = list(range(1, 13))
            submission_data = fact_ketqua.groupby('SubmissionID').apply(
                lambda x: dict(zip(x['MaCauHoi'], x['Diem']))
            ).to_dict()
            
            final_data = []
            for sub_id in valid_subs:
                answers = submission_data.get(sub_id, {})
                for q in all_questions:
                    diem = answers.get(q, 5)  # Mặc định = 5 nếu thiếu
                    final_data.append((sub_id, q, diem))
            
            if final_data:
                # Loại bỏ duplicate trong memory
                unique_data = {}
                for sub_id, q, diem in final_data:
                    key = (sub_id, q)
                    if key not in unique_data or diem > unique_data[key]:
                        unique_data[key] = diem
                
                final_unique = [(k[0], k[1], v) for k, v in unique_data.items()]
                
                sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
                cursor.executemany(sql_kq, final_unique)
                count_kq = len(final_unique)
                print(f"      ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,} dòng")
        
        cursor.execute("COMMIT")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        raise e
    finally:
        # BẬT LẠI CONSTRAINTS
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.connection.commit()
    
    print(f"  ✅ FACT loaded in {time.time()-start_time:.1f}s")
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - TỐI ƯU TỐC ĐỘ")
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
    
    try:
        # 7. Load existing data (1 lần duy nhất)
        existing_data = load_all_existing_data(cursor)
        
        # 8. Insert missing data
        insert_missing_data_batch(cursor, existing_data)
        
        # Refresh existing data
        existing_data = load_all_existing_data(cursor)
        
        # 9. Get học kỳ
        ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
        
        # 10. Load remaining dimensions
        load_remaining_dimensions_optimized(cursor, df_raw, existing_data, ma_hoc_ky, nam_hoc, hoc_ky)
        
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
    
    print("\n📊 KẾT QUẢ:")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform: {transform_time:.1f}s")
    print(f"   - Database: {db_time:.1f}s")
    print(f"   - TOTAL: {total_time:.1f}s")
    print(f"   - Submissions: {count_main:,}")
    print(f"   - Answers: {count_kq:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
