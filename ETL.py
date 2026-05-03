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
    if not lop or not isinstance(lop, str):
        return (False, None, None, None, None)
    
    lop_upper = lop.upper().strip()
    
    if 'CTS' in lop_upper:
        return (True, 'CTS', 'KHOA19', 'NULL_CTS', 'NULL_CTS')
    
    if 'QT' in lop_upper:
        return (True, 'QT', 'KHOA11', 'NULL_QT', 'NULL_QT')
    
    return (False, None, None, None, None)


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
    print("  -> Đang load existing data từ database...")
    start = time.time()
    
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    existing_khoa = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaChuyenNganh, MaNganh FROM DIM_CHUYEN_NGANH")
    existing_chuyennganh = {row[0]: row[1] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hocphan = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_giangvien = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sinhvien = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lophp = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
    existing_hocky = {row[0] for row in cursor.fetchall()}
    
    print(f"     ✅ Loaded in {time.time()-start:.1f}s")
    print(f"     - DIM_HOC_PHAN có {len(existing_hocphan)} dòng")
    print(f"     - DIM_GIANG_VIEN có {len(existing_giangvien)} dòng")
    
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


def create_null_special_data(cursor, existing_data):
    print("\n  -> Tạo dòng dữ liệu NULL cho lớp đặc biệt CTS và QT...")
    
    # Dữ liệu NULL cho CTS (KHOA19 - Trường ĐH Kinh Tế)
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_NGANH WHERE MaNganh = 'NULL_CTS')
        INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) 
        VALUES ('NULL_CTS', '', 'KHOA19')
    """)
    
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = 'NULL_CTS')
        INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) 
        VALUES ('NULL_CTS', '', 'NULL_CTS')
    """)
    
    # Dữ liệu NULL cho QT (KHOA11 - Phòng Đào Tạo)
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_NGANH WHERE MaNganh = 'NULL_QT')
        INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) 
        VALUES ('NULL_QT', '', 'KHOA11')
    """)
    
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = 'NULL_QT')
        INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) 
        VALUES ('NULL_QT', '', 'NULL_QT')
    """)
    
    cursor.connection.commit()
    print("        ✅ Đã tạo Ngành và Chuyên ngành NULL cho CTS và QT")
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_data['nganh'] = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_data['chuyennganh'] = {row[0] for row in cursor.fetchall()}


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
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first').copy()
        text_df_unique.loc[:, 'NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        texts = text_df_unique['NoiDungGopY'].tolist()
        sentiments, tags = _nlp.process_batch(texts)
        
        text_df_unique.loc[:, 'Sentiment'] = sentiments
        text_df_unique.loc[:, 'Tag_HocPhan'] = [t[0] for t in tags]
        text_df_unique.loc[:, 'Tag_DayHoc'] = [t[1] for t in tags]
        text_df_unique.loc[:, 'Tag_KiemTra'] = [t[2] for t in tags]
        text_df_unique.loc[:, 'Tag_Khac'] = [t[3] for t in tags]
        text_df_unique.loc[:, 'Is_Valid'] = 1
        
        fact_main = text_df_unique[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                                     'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                     'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].copy()
    
    mcq_df = df_raw[df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '')].copy()
    
    if not mcq_df.empty:
        mcq_df.loc[:, 'MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df.loc[:, 'Diem'] = mcq_df['GiaTri'].astype(int)
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
    
    # 3. DIM_LOP_SINH_VIEN
    print("\n  -> 3. XỬ LÝ ĐẶC BIỆT DIM_LOP_SINH_VIEN")
    print("     - Lớp có chứa 'CTS' -> Chuyên ngành NULL_CTS, gán cho KHOA19")
    print("     - Lớp có chứa 'QT' -> Chuyên ngành NULL_QT, gán cho KHOA11")
    
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    print(f"     - Tổng số lớp unique: {len(df_lop_unique)}")
    
    special_cts = []
    special_qt = []
    normal_lops = []
    skipped_lops = []
    new_lop_data = []
    
    for _, row in df_lop_unique.iterrows():
        lop = row['Lop']
        
        if lop in existing_data['lop']:
            continue
        
        is_special, loai, ma_khoa, ma_chuyen_nganh, ma_nganh = is_special_lop(lop)
        
        if is_special:
            if ma_chuyen_nganh in existing_data['chuyennganh']:
                new_lop_data.append((lop, lop, ma_chuyen_nganh))
                if loai == 'CTS':
                    special_cts.append(lop)
                else:
                    special_qt.append(lop)
            else:
                skipped_lops.append(f"{lop} (Chuyên ngành {ma_chuyen_nganh} chưa được tạo)")
        else:
            match = re.search(r'K(\d{2})', lop.upper())
            if match:
                ma_cn = f"K{match.group(1)}"
                if ma_cn in existing_data['chuyennganh']:
                    new_lop_data.append((lop, lop, ma_cn))
                    normal_lops.append(lop)
                else:
                    skipped_lops.append(f"{lop} (MaChuyenNganh={ma_cn} không tồn tại)")
            else:
                skipped_lops.append(f"{lop} (không xác định được mã chuyên ngành)")
    
    if special_cts:
        print(f"     📌 Lớp CTS (KHOA19): {len(special_cts)} lớp")
        for lop in special_cts[:5]:
            print(f"        - {lop}")
    if special_qt:
        print(f"     📌 Lớp QT (KHOA11): {len(special_qt)} lớp")
        for lop in special_qt[:5]:
            print(f"        - {lop}")
    if normal_lops:
        print(f"     📌 Lớp thường: {len(normal_lops)} lớp")
    
    if new_lop_data:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop_data)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {len(new_lop_data)} lớp mới vào DIM_LOP_SINH_VIEN")
    
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
    
    # 5. DIM_LOP_HOC_PHAN - DEBUG CHI TIẾT
    print("\n  -> 5. DIM_LOP_HOC_PHAN")
    
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    print(f"     - Tổng số LopHP unique từ dữ liệu: {len(df_lhp)}")
    print(f"     - Số MaHP có trong DIM_HOC_PHAN: {len(existing_data['hocphan'])}")
    print(f"     - Số MaGV có trong DIM_GIANG_VIEN: {len(existing_data['giangvien'])}")
    
    # Lấy 5 mẫu dữ liệu
    print("\n     - 5 mẫu dữ liệu từ file raw:")
    for i, (_, r) in enumerate(df_lhp.head(5).iterrows()):
        print(f"        {i+1}. LopHP='{r['LopHP']}', MaHP='{r['MaHP']}', MaGV='{r['MaGV']}'")
    
    # Kiểm tra MaHP và MaGV có tồn tại không
    missing_hp_set = set()
    missing_gv_set = set()
    
    new_lhp = []
    for _, r in df_lhp.iterrows():
        lop_hp = r['LopHP']
        ma_hp = r['MaHP']
        ma_gv = r['MaGV']
        
        if lop_hp in existing_data['lophp']:
            continue
        
        if ma_hp not in existing_data['hocphan']:
            missing_hp_set.add(ma_hp)
            continue
        
        if ma_gv not in existing_data['giangvien']:
            missing_gv_set.add(ma_gv)
            continue
        
        new_lhp.append((lop_hp, lop_hp, ma_hp, ma_gv, ma_hoc_ky))
    
    if missing_hp_set:
        print(f"\n     ⚠️ Có {len(missing_hp_set)} MaHP không tồn tại trong DIM_HOC_PHAN:")
        for hp in list(missing_hp_set)[:10]:
            print(f"        - '{hp}'")
    
    if missing_gv_set:
        print(f"\n     ⚠️ Có {len(missing_gv_set)} MaGV không tồn tại trong DIM_GIANG_VIEN:")
        for gv in list(missing_gv_set)[:10]:
            print(f"        - '{gv}'")
    
    if new_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", new_lhp)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {len(new_lhp)} lớp học phần mới")
    else:
        print(f"     ❌ KHÔNG có lớp học phần nào được thêm!")
        print(f"        Nguyên nhân: MaHP hoặc MaGV không tồn tại trong các DIM tương ứng")
        print(f"        Cần kiểm tra dữ liệu master HP-Khoa.csv và DIM_GIANG_VIEN")
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_data['lophp'] = {row[0] for row in cursor.fetchall()}
    
    print("  ✅ Các bảng DIM còn lại đã được load xong!")


# ================= LOAD FACT TABLES =================
def load_fact_tables_bulk(cursor, conn, fact_main, fact_ketqua, existing_data, ma_hoc_ky):
    """
    Tối ưu insert FACT tables bằng bulk insert
    """
    print("\n📥 Loading FACT tables (BULK INSERT)...")
    start_time = time.time()
    
    # Lấy danh sách hợp lệ
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    valid_sv = existing_data['sinhvien']
    
    print(f"     - Số LopHP hợp lệ: {len(valid_lophp)}")
    print(f"     - Số MaSV hợp lệ: {len(valid_sv)}")
    
    if not valid_lophp:
        print("      ⚠️ Không có LopHP hợp lệ, bỏ qua FACT tables!")
        return 0, 0
    
    # ===== 1. Filter dữ liệu hợp lệ =====
    print("     - Lọc dữ liệu hợp lệ...")
    
    # Filter fact_main
    if not fact_main.empty:
        fact_main_filtered = fact_main[
            fact_main['MaSV'].isin(valid_sv) & 
            fact_main['LopHP'].isin(valid_lophp)
        ].copy()
        print(f"     - fact_main sau lọc: {len(fact_main_filtered):,} dòng (bỏ {len(fact_main) - len(fact_main_filtered):,})")
    else:
        fact_main_filtered = pd.DataFrame()
    
    # ===== 2. INSERT FACT_GOP_Y_TU_LUAN trực tiếp =====
    count_main = 0
    if not fact_main_filtered.empty:
        print("     - Đang insert FACT_GOP_Y_TU_LUAN...")
        
        # Chuẩn bị data
        fact_main_filtered['NoiDungGopY'] = fact_main_filtered['NoiDungGopY'].astype(str).str[:4000]
        
        # Chuyển thành list of tuples
        data_main = list(fact_main_filtered[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid', 'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].itertuples(index=False, name=None))
        
        # Bulk insert
        sql_main = """INSERT INTO FACT_GOP_Y_TU_LUAN 
                     (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, 
                      Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) 
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        
        cursor.fast_executemany = True
        cursor.executemany(sql_main, data_main)
        cursor.connection.commit()
        count_main = len(data_main)
        print(f"      ✅ FACT_GOP_Y_TU_LUAN: {count_main:,} dòng")
    
    # ===== 3. INSERT FACT_KET_QUA_DANH_GIA =====
    count_kq = 0
    if not fact_ketqua.empty and count_main > 0:
        print("     - Đang chuẩn bị FACT_KET_QUA_DANH_GIA...")
        
        # Lấy danh sách SubmissionID hợp lệ
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        valid_subs = {row[0] for row in cursor.fetchall()}
        
        # Filter fact_ketqua
        fact_ketqua_filtered = fact_ketqua[fact_ketqua['SubmissionID'].isin(valid_subs)].copy()
        
        if not fact_ketqua_filtered.empty:
            # Tạo dữ liệu đầy đủ 12 câu hỏi cho mỗi submission
            print("     - Tạo dữ liệu đầy đủ 12 câu hỏi...")
            
            all_questions = list(range(1, 13))
            
            # Group by SubmissionID
            submission_dict = fact_ketqua_filtered.groupby('SubmissionID').apply(
                lambda x: dict(zip(x['MaCauHoi'], x['Diem']))
            ).to_dict()
            
            # Tạo data hoàn chỉnh
            complete_data = []
            missing_count = 0
            
            for sub_id in valid_subs:
                answers = submission_dict.get(sub_id, {})
                for q in all_questions:
                    diem = answers.get(q, 5)
                    if q not in answers:
                        missing_count += 1
                    complete_data.append((sub_id, q, diem))
            
            # Loại bỏ duplicate (giữ giá trị lớn nhất)
            unique_dict = {}
            for sub_id, q, diem in complete_data:
                key = (sub_id, q)
                if key not in unique_dict or diem > unique_dict[key]:
                    unique_dict[key] = diem
            
            final_data = [(k[0], k[1], v) for k, v in unique_dict.items()]
            
            print(f"     - Insert {len(final_data):,} dòng vào FACT_KET_QUA_DANH_GIA...")
            
            sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
            cursor.executemany(sql_kq, final_data)
            cursor.connection.commit()
            count_kq = len(final_data)
            print(f"      ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,} dòng (bổ sung {missing_count} câu thiếu)")
    
    elapsed = time.time() - start_time
    print(f"  ✅ FACT loaded in {elapsed:.1f}s")
    return count_main, count_kq


# ================= LOAD MASTER DATA =================
def load_hp_master(blob_service):
    content = download_blob(blob_service, "tailieu", "HP-Khoa.csv")
    if not content:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 4:
        df = df.iloc[:, 1:4]
        df.columns = ['MaHP', 'TenKhoa', 'TenHP']
    return df


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - FIX DIM_LOP_HOC_PHAN")
    print("=" * 60)
    print("📌 Xử lý đặc biệt:")
    print("   - Lớp có chứa 'CTS' -> Chuyên ngành NULL_CTS, gán cho KHOA19")
    print("   - Lớp có chứa 'QT' -> Chuyên ngành NULL_QT, gán cho KHOA11")
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
    print(f"  ✅ HP-Khoa: {len(hp_master)} dòng")
    
    # 3. Đọc dữ liệu survey
    print(f"\n📥 3. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    # 4. Parse dữ liệu
    print("\n📝 4. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    print(f"  ✅ Parse: {len(df_raw):,} dòng trong {parse_time:.1f}s")
    
    # 5. Transform & NLP
    print("\n🔄 5. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua, df_raw = transform_with_nlp_optimized(df_raw)
    transform_time = time.time() - transform_start
    print(f"  ✅ Transform: {transform_time:.1f}s")
    print(f"     - fact_main: {len(fact_main)} dòng")
    print(f"     - fact_ketqua: {len(fact_ketqua)} dòng")
    
    # 6. Lưu backup
    print("\n💾 6. Lưu backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    
    # 7. Kết nối Database
    print("\n💾 7. Kết nối SQL Database...")
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
        # 8. Load existing data
        existing_data = load_all_existing_data(cursor)
        
        # 9. Tạo dữ liệu NULL cho đặc biệt
        create_null_special_data(cursor, existing_data)
        
        # 10. Lấy thông tin học kỳ
        ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
        
        # 11. Load DIM_HOC_PHAN từ master
        print("\n📥 Loading DIM_HOC_PHAN từ master...")
        if not hp_master.empty:
            for _, row in hp_master.iterrows():
                ma_hp = row['MaHP']
                if ma_hp not in existing_data['hocphan']:
                    cursor.execute("INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES (?, ?, ?)",
                                  ma_hp, row['TenHP'], 'KHOA19')
            cursor.connection.commit()
            print(f"     ✅ Đã load DIM_HOC_PHAN")
        
        # Refresh existing data
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing_data['hocphan'] = {row[0] for row in cursor.fetchall()}
        
        # 12. Load các bảng DIM còn lại
        load_remaining_dimensions_optimized(cursor, df_raw, existing_data, ma_hoc_ky, nam_hoc, hoc_ky)
        
        # Refresh existing data
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing_data['sinhvien'] = {row[0] for row in cursor.fetchall()}
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing_data['lophp'] = {row[0] for row in cursor.fetchall()}
        
        # 13. Load FACT tables
        count_main, count_kq = load_fact_tables_bulk(cursor, conn, fact_main, fact_ketqua, existing_data, ma_hoc_ky)
        
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
    print(f"   - Submissions (FACT_GOP_Y_TU_LUAN): {count_main:,}")
    print(f"   - Answers (FACT_KET_QUA_DANH_GIA): {count_kq:,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
