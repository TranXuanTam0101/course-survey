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
from functools import lru_cache

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER", "HOC_KY_2425")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SURVEY_FILE:
    print("Thiếu biến môi trường SURVEY_FILE")
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

# TỐI ƯU: Tăng số worker và chunk size
NUM_WORKERS = max(4, mp.cpu_count() * 2)  # Tăng gấp đôi
CHUNK_SIZE = 100000  # Tăng lên 100k
BATCH_SIZE = 200000  # Tăng batch size

# Cache
_EXISTING_CACHE = {}

# ================= PATTERNS (COMPILED) =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_ma_cn_cts_pattern = re.compile(r'CTS[-_]?(\d{2})K')
_ma_cn_qt_pattern = re.compile(r'(\d{2})KQT')
_ma_cn_k_pattern = re.compile(r'K(\d{2})')


# ================= UTILS =================
def generate_id(prefix, existing_ids, start=1):
    i = start
    while True:
        new_id = f"{prefix}{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1


def generate_ma_nganh(ma_khoa, existing_ids, start=1):
    i = start
    while True:
        new_id = f"{ma_khoa}NG{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1


def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    nam_hoc = f"{nam_bat_dau}-{nam_ket_thuc}"
    year_part = f"{nam_bat_dau % 100}{nam_ket_thuc % 100}"
    return f"HK{hoc_ky}_{year_part}", nam_hoc, hoc_ky


def download_blob(blob_service, container, path):
    try:
        container_client = blob_service.get_container_client(container)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            return blob.download_blob().readall().decode('utf-8-sig')
        return ""
    except Exception:
        return ""


# ================= FAST BULK INSERT =================
def bulk_insert(cursor, table, df, columns):
    """Bulk insert nhanh nhất"""
    if df.empty:
        return 0
    
    # Chuyển DataFrame thành list of tuples
    data = [tuple(str(v)[:500] if pd.notna(v) else '' for v in row) for row in df[columns].to_numpy()]
    
    placeholders = ', '.join(['?' for _ in columns])
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(data)


def bulk_merge(cursor, table, df, columns, id_col):
    """Merge nhanh: chỉ insert, không update"""
    if df.empty:
        return 0
    
    cache_key = f"{table}.{id_col}"
    if cache_key not in _EXISTING_CACHE:
        cursor.execute(f"SELECT {id_col} FROM {table}")
        _EXISTING_CACHE[cache_key] = {str(row[0]).strip() for row in cursor.fetchall()}
    
    existing = _EXISTING_CACHE[cache_key]
    df_filtered = df[~df[id_col].astype(str).str.strip().isin(existing)]
    
    if df_filtered.empty:
        return 0
    
    data = [tuple(str(v)[:500] if pd.notna(v) else '' for v in row) for row in df_filtered[columns].to_numpy()]
    
    placeholders = ', '.join(['?' for _ in columns])
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    
    # Update cache
    _EXISTING_CACHE[cache_key].update(df_filtered[id_col].astype(str).str.strip().tolist())
    return len(data)


@lru_cache(maxsize=1000)
def cached_determine_ma_chuyen_nganh(lop):
    """Cached version - nhanh hơn nhiều cho các lớp trùng lặp"""
    if not lop or not isinstance(lop, str):
        return None, None
    
    lop_upper = lop.upper().strip()
    
    if 'CTS' in lop_upper:
        match = _ma_cn_cts_pattern.search(lop_upper)
        return (f"CTS_{match.group(1)}K" if match else "CTS"), 'TĐHKT'
    
    if 'QT' in lop_upper:
        match = _ma_cn_qt_pattern.search(lop_upper)
        return (f"QT_{match.group(1)}K" if match else "QT"), 'PĐT'
    
    match = _ma_cn_k_pattern.search(lop_upper)
    return (f"K{match.group(1)}" if match else None), None


def determine_ma_chuyen_nganh_batch(lops):
    """Batch processing với caching"""
    return [cached_determine_ma_chuyen_nganh(lop) for lop in lops]


# ================= PIPELINE 1: MASTER DATA (TỐI ƯU) =================
def pipeline_master_data(blob_service, cursor):
    print("\n" + "=" * 70)
    print("📚 PIPELINE 1: MASTER DATA (TỐI ƯU TỐC ĐỘ)")
    print("=" * 70)
    
    # Load cache
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    khoa_db = {str(row[1]).strip(): str(row[0]).strip() for row in cursor.fetchall()}
    existing_khoa_ids = set(khoa_db.values())
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    print(f"  -> Cache: Khoa={len(khoa_db)}, Nganh={len(existing_nganh_ids)}, CN={len(existing_cn_ids)}, HP={len(existing_hp_ids)}")
    
    # ===== HP-Khoa.csv =====
    content_hp = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    if content_hp:
        df_hp = pd.read_csv(io.StringIO(content_hp))
        df_hp.columns = [c.strip() for c in df_hp.columns]
        
        # Auto detect columns
        cols = [c for c in df_hp.columns if 'unnamed' not in c.lower() and 'stt' not in c.lower()]
        if len(cols) >= 3:
            col_ma_hp, col_khoa_hp, col_ten_hp = cols[0], cols[1], cols[2]
            
            df_hp_data = pd.DataFrame({
                'MaHP': df_hp[col_ma_hp].astype(str).str.strip(),
                'TenHP': df_hp[col_ten_hp].astype(str).str.strip() if col_ten_hp else '',
                'TenKhoa': df_hp[col_khoa_hp].astype(str).str.strip().apply(
                    lambda x: 'Trường Đại học Sư phạm' if isinstance(x, str) and ('Ngữ Văn' in x or 'Toán' in x) else x
                )
            })
            
            # Tạo khoa mới nếu chưa có
            for tk in df_hp_data['TenKhoa'].dropna().unique():
                tk_str = str(tk).strip()
                if tk_str and tk_str not in khoa_db:
                    new_id = generate_id('KHOA', existing_khoa_ids)
                    existing_khoa_ids.add(new_id)
                    khoa_db[tk_str] = new_id
                    cursor.execute("INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)", (new_id, tk_str))
            
            # Map MaKhoa
            df_hp_data['MaKhoa'] = df_hp_data['TenKhoa'].map(lambda x: khoa_db.get(str(x).strip(), 'KHOA01'))
            df_hp_data = df_hp_data[df_hp_data['MaHP'].isin(['', 'nan']) == False].drop_duplicates('MaHP')
            
            # Bulk insert HOC_PHAN
            if not df_hp_data.empty:
                count = bulk_merge(cursor, 'DIM_HOC_PHAN', df_hp_data[['MaHP', 'TenHP', 'MaKhoa']], 
                                   ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
                print(f"  ✅ DIM_HOC_PHAN: {count} mới")
            
            cursor.connection.commit()
    
    # ===== TenChuyenNganh-Khoa.csv =====
    content_cn = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if content_cn:
        df_cn = pd.read_csv(io.StringIO(content_cn))
        df_cn.columns = [c.strip() for c in df_cn.columns]
        
        # Auto detect columns
        cols = df_cn.columns.tolist()
        if len(cols) >= 5:
            col_khoa, col_nganh, col_cn, col_ma_cn = cols[1], cols[2], cols[3], cols[4]
            
            # Tạo NGANH
            nganh_list = []
            for _, row in df_cn.iterrows():
                ten_khoa = str(row[col_khoa]).strip() if pd.notna(row[col_khoa]) else ''
                ten_nganh = str(row[col_nganh]).strip() if pd.notna(row[col_nganh]) else ''
                if ten_khoa and ten_nganh:
                    ma_khoa = khoa_db.get(ten_khoa, generate_id('KHOA', existing_khoa_ids))
                    ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                    existing_nganh_ids.add(ma_nganh)
                    nganh_list.append((ma_nganh, ten_nganh, ma_khoa))
            
            if nganh_list:
                df_nganh = pd.DataFrame(nganh_list, columns=['MaNganh', 'TenNganh', 'MaKhoa'])
                count = bulk_merge(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
                print(f"  ✅ DIM_NGANH: {count} mới")
            
            # Tạo CHUYEN_NGANH
            cn_list = []
            for _, row in df_cn.iterrows():
                ma_cn = str(row[col_ma_cn]).strip() if pd.notna(row[col_ma_cn]) else ''
                ten_cn = str(row[col_cn]).strip() if pd.notna(row[col_cn]) else ''
                ten_nganh = str(row[col_nganh]).strip() if pd.notna(row[col_nganh]) else ''
                
                if ma_cn and ma_cn not in existing_cn_ids:
                    # Tìm ma_nganh
                    ma_nganh = next((n for n in nganh_list if n[1] == ten_nganh), (None,))[0]
                    if ma_nganh:
                        cn_list.append((ma_cn, ten_cn, ma_nganh))
                        existing_cn_ids.add(ma_cn)
            
            if cn_list:
                df_cn_out = pd.DataFrame(cn_list, columns=['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'])
                count = bulk_merge(cursor, 'DIM_CHUYEN_NGANH', df_cn_out, 
                                   ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
                print(f"  ✅ DIM_CHUYEN_NGANH: {count} mới")
            
            cursor.connection.commit()
    
    return khoa_db, existing_khoa_ids, existing_nganh_ids, existing_cn_ids, existing_hp_ids


# ================= NLP CLASS (TỐI ƯU) =================
class FastNLP:
    def __init__(self):
        self.positive_set = {'tuyệt vời', 'xuất sắc', 'hoàn hảo', 'rất tốt', 'rất hay', 
                             'cực kỳ', 'tốt', 'hay', 'ổn', 'hài lòng', 'cảm ơn', 'ok', 'oke'}
        self.negative_set = {'tệ', 'dở', 'kém', 'chán', 'khó hiểu', 'lan man', 'dài dòng'}
        self.no_opinion_patterns = [r'^không\s*(có)?\s*(gì)?\s*(ý\s*kiến)?\s*(góp\s*ý)?\s*$', r'^(ko|k|0|\.\.+|n/?a)$', r'^$']
        
        # Compile regex patterns
        self.no_opinion_regex = re.compile('|'.join(self.no_opinion_patterns))
        self.tag_hp_regex = re.compile(r'chuẩn đầu ra|nội dung|học phần|môn học')
        self.tag_dh_regex = re.compile(r'giảng viên|thầy|cô|dạy|giảng')
        self.tag_kt_regex = re.compile(r'kiểm tra|đánh giá|thi|đề thi')
    
    def process_batch_vectorized(self, texts):
        """Vectorized processing với pandas - NHANH NHẤT"""
        series = pd.Series(texts).fillna('').str.lower().str.strip()
        
        # No opinion detection
        is_no_opinion = series.str.match(self.no_opinion_regex).fillna(False)
        
        # Sentiment
        pos_score = series.apply(lambda t: sum(1 for w in self.positive_set if w in t))
        neg_score = series.apply(lambda t: sum(1 for w in self.negative_set if w in t))
        net_score = pos_score - neg_score
        sentiments = net_score.apply(lambda x: 'positive' if x > 0.5 else ('negative' if x < -0.5 else 'neutral'))
        sentiments[is_no_opinion] = 'neutral'
        
        # Tags
        tag_hp = series.str.contains(self.tag_hp_regex).astype(int)
        tag_dh = series.str.contains(self.tag_dh_regex).astype(int)
        tag_kt = series.str.contains(self.tag_kt_regex).astype(int)
        tag_khac = ((tag_hp + tag_dh + tag_kt) == 0).astype(int)
        tag_khac[is_no_opinion] = 1
        
        return sentiments.tolist(), list(zip(tag_hp, tag_dh, tag_kt, tag_khac))


_nlp = FastNLP()


# ================= PARSE SURVEY DATA (TỐI ƯU) =================
def is_date_format(value):
    return bool(_date_pattern.match(value.strip())) if isinstance(value, str) else False


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH"


def parse_lines_batch(lines_batch, file_name):
    results = []
    for line in lines_batch:
        if not line or not line.strip():
            continue
        row = [x.strip() for x in line.split(',')]
        row_len = len(row)
        if row_len < 15:
            continue
        try:
            # Tìm ngày sinh
            ngay_sinh_index = -1
            for i in range(2, min(row_len, 12)):
                if is_date_format(row[i]):
                    ngay_sinh_index = i
                    break
            if ngay_sinh_index == -1:
                continue
            
            # Tìm mã giảng viên
            ma_gv_index = -1
            for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
                if is_ma_gv_format(row[i]):
                    ma_gv_index = i
                    break
            
            if ma_gv_index == -1:
                ma_gv_index = row_len - 4 if row_len >= 4 else ngay_sinh_index + 2
            
            # Tìm NULL index
            null_index = -1
            for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
                if row[i].upper() == 'NULL' or row[i] == '':
                    null_index = i
                    break
            
            # Tạo kết quả
            submission_id = f"{row[1]}_{row[ma_gv_index + 3]}_{row[ma_gv_index]}_{file_name}"
            
            result = {
                'SubmissionID': submission_id,
                'Lop': row[0],
                'MaSV': row[1],
                'HoDem': '',
                'Ten': '',
                'NgaySinh': row[ngay_sinh_index],
                'MaHP': row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else '',
                'TenHP': ' '.join(row[ngay_sinh_index + 2:ma_gv_index]) if ma_gv_index > ngay_sinh_index + 2 else '',
                'MaGV': row[ma_gv_index] if ma_gv_index >= 0 else '',
                'HoDemGV': row[ma_gv_index + 1] if ma_gv_index + 1 < row_len else '',
                'TenGV': row[ma_gv_index + 2] if ma_gv_index + 2 < row_len else '',
                'LopHP': row[ma_gv_index + 3] if ma_gv_index + 3 < row_len else '',
                'CauHoi': row[ma_gv_index + 4] if ma_gv_index + 4 < row_len else '',
                'GiaTri': row[ma_gv_index + 5] if ma_gv_index + 5 < row_len else '',
                'EssayText': ','.join(row[null_index + 1:]).strip() if null_index != -1 and null_index + 1 < row_len else ''
            }
            
            # Xử lý họ tên sinh viên
            if ngay_sinh_index > 1:
                name_parts = [p for p in row[2:ngay_sinh_index] if p]
                if name_parts:
                    result['Ten'] = name_parts[-1]
                    result['HoDem'] = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            results.append(result)
        except Exception:
            continue
    return results


def parse_survey_fast(content: str, file_name: str) -> pd.DataFrame:
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    all_rows = []
    
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch, batch, file_name) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    df = pd.DataFrame(all_rows)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.1f}s)")
    return df


# ================= PIPELINE 2: SURVEY DATA (TỐI ƯU) =================
def pipeline_survey_data(blob_service, cursor):
    print("\n" + "=" * 70)
    print("📋 PIPELINE 2: SURVEY DATA (TỐI ƯU TỐC ĐỘ)")
    print("=" * 70)
    
    # Đọc survey
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return 0, 0
    
    # Parse
    df_raw = parse_survey_fast(survey_content, FILE_NAME)
    if df_raw.empty:
        return 0, 0
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # ===== 1. DIM_HOC_KY =====
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                       ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
    
    # ===== 2. DIM_GIANG_VIEN (BULK) =====
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    if not df_gv.empty:
        count = bulk_merge(cursor, 'DIM_GIANG_VIEN', df_gv, ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        if count > 0:
            print(f"  ✅ DIM_GIANG_VIEN: {count} mới")
    
    # ===== 3. DIM_LOP_SINH_VIEN (XỬ LÝ ĐẶC BIỆT CTS/QT) =====
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    cn_results = determine_ma_chuyen_nganh_batch(df_lop_unique['Lop'].tolist())
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    new_lop = []
    for lop, (ma_cn, _) in zip(df_lop_unique['Lop'], cn_results):
        if lop not in existing_lop and ma_cn:
            cursor.execute("SELECT 1 FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = ?", ma_cn)
            if cursor.fetchone():
                new_lop.append((lop, lop, ma_cn))
    
    if new_lop:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop)
        cursor.connection.commit()
        print(f"  ✅ DIM_LOP_SINH_VIEN: {len(new_lop)} mới")
    
    # ===== 4. DIM_SINH_VIEN (BULK) =====
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    valid_lop = {row[0] for row in cursor.fetchall()}
    
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    df_sv = df_sv[df_sv['Lop'].isin(valid_lop)]
    
    # Convert NgaySinh
    df_sv['NgaySinh'] = pd.to_datetime(df_sv['NgaySinh'], format='%d/%m/%Y', errors='coerce').dt.date
    
    if not df_sv.empty:
        count = bulk_merge(cursor, 'DIM_SINH_VIEN', df_sv, ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop'], 'MaSV')
        if count > 0:
            print(f"  ✅ DIM_SINH_VIEN: {count} mới")
    
    # ===== 5. DIM_LOP_HOC_PHAN (BULK) =====
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    valid_hp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    valid_gv = {row[0] for row in cursor.fetchall()}
    
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    df_lhp = df_lhp[df_lhp['MaHP'].isin(valid_hp) & df_lhp['MaGV'].isin(valid_gv)]
    df_lhp['MaHocKy'] = ma_hoc_ky
    
    if not df_lhp.empty:
        count = bulk_merge(cursor, 'DIM_LOP_HOC_PHAN', df_lhp, ['LopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'LopHP')
        if count > 0:
            print(f"  ✅ DIM_LOP_HOC_PHAN: {count} mới")
    
    # ===== 6. TRANSFORM NLP =====
    print("\n🔄 Transform NLP...")
    start_nlp = time.time()
    
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    fact_main = pd.DataFrame()
    
    if not text_df.empty:
        text_df_unique = text_df.drop_duplicates('SubmissionID')
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        sentiments, tags = _nlp.process_batch_vectorized(text_df_unique['NoiDungGopY'].tolist())
        
        text_df_unique['Sentiment'] = sentiments
        text_df_unique['Tag_HocPhan'] = [t[0] for t in tags]
        text_df_unique['Tag_DayHoc'] = [t[1] for t in tags]
        text_df_unique['Tag_KiemTra'] = [t[2] for t in tags]
        text_df_unique['Tag_Khac'] = [t[3] for t in tags]
        text_df_unique['Is_Valid'] = 1
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']]
    
    print(f"  ✅ NLP xong ({time.time()-start_nlp:.1f}s)")
    
    # ===== 7. FACT TABLES (BULK) =====
    print("\n📥 Loading FACT tables...")
    start_fact = time.time()
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    valid_sv = {row[0] for row in cursor.fetchall()}
    
    # FACT_GOP_Y_TU_LUAN
    count_main = 0
    if not fact_main.empty:
        fact_main = fact_main[fact_main['MaSV'].isin(valid_sv) & fact_main['LopHP'].isin(valid_lophp)]
        fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].str[:4000]
        
        if not fact_main.empty:
            count_main = bulk_insert(cursor, 'FACT_GOP_Y_TU_LUAN', fact_main,
                                     ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY', 
                                      'Sentiment', 'Is_Valid', 'Tag_HocPhan', 'Tag_DayHoc', 
                                      'Tag_KiemTra', 'Tag_Khac'])
            print(f"  ✅ FACT_GOP_Y_TU_LUAN: {count_main:,} dòng")
    
    # FACT_KET_QUA_DANH_GIA
    count_kq = 0
    mcq_df = df_raw[df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '')].copy()
    
    if not mcq_df.empty and count_main > 0:
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        valid_subs = {row[0] for row in cursor.fetchall()}
        
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        mcq_df = mcq_df[mcq_df['SubmissionID'].isin(valid_subs)]
        
        # Tạo full 12 câu
        all_questions = list(range(1, 13))
        submission_data = mcq_df.groupby('SubmissionID').apply(lambda x: dict(zip(x['MaCauHoi'], x['Diem']))).to_dict()
        
        final_data = []
        for sub_id in valid_subs:
            answers = submission_data.get(sub_id, {})
            for q in all_questions:
                final_data.append((sub_id, q, answers.get(q, 5)))
        
        # Remove duplicates
        unique_data = {}
        for sub_id, q, diem in final_data:
            key = (sub_id, q)
            if key not in unique_data or diem > unique_data[key]:
                unique_data[key] = diem
        
        final_unique = [(k[0], k[1], v) for k, v in unique_data.items()]
        
        if final_unique:
            # Bulk insert
            df_kq = pd.DataFrame(final_unique, columns=['SubmissionID', 'MaCauHoi', 'Diem'])
            count_kq = bulk_insert(cursor, 'FACT_KET_QUA_DANH_GIA', df_kq, ['SubmissionID', 'MaCauHoi', 'Diem'])
            print(f"  ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,} dòng")
    
    print(f"  ✅ FACT loaded in {time.time()-start_fact:.1f}s")
    
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 ETL PIPELINE - TỐI ƯU TỐC ĐỘ CAO")
    print(f"   Workers: {NUM_WORKERS}, Chunk: {CHUNK_SIZE}")
    print("=" * 70)
    
    # Kết nối
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    conn = pyodbc.connect(CONN_STR, autocommit=False)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # PIPELINE 1: MASTER DATA
        pipeline_master_data(blob_service, cursor)
        
        # PIPELINE 2: SURVEY DATA  
        count_main, count_kq = pipeline_survey_data(blob_service, cursor)
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    total_time = time.time() - total_start
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! Thời gian: {total_time:.1f}s")
    print(f"   Submissions: {count_main:,}")
    print(f"   Answers: {count_kq:,}")
    print("=" * 70)


if __name__ == "__main__":
    main()
