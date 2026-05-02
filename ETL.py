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

# Số lượng worker
NUM_WORKERS = max(2, mp.cpu_count())
CHUNK_SIZE = 50000
BATCH_SIZE = 100000

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# Cache cho existing IDs
_EXISTING_CACHE = {}

# ================= UTILS =================
def generate_id(prefix, existing_ids, start=1):
    """Tự sinh ID: PREFIX + số (VD: KHOA01, KHOA02...)"""
    i = start
    while True:
        new_id = f"{prefix}{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1


def generate_ma_nganh(ma_khoa, existing_ids, start=1):
    """Tự sinh Mã Ngành: KHOA + NG + số"""
    i = start
    while True:
        new_id = f"{ma_khoa}NG{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1


def derive_ma_hoc_ky():
    """Tạo mã học kỳ từ tên file survey"""
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    nam_hoc = f"{nam_bat_dau}-{nam_ket_thuc}"
    year_part = f"{nam_bat_dau % 100}{nam_ket_thuc % 100}"
    ma_hoc_ky = f"HK{hoc_ky}_{year_part}"
    return ma_hoc_ky, nam_hoc, hoc_ky


def download_blob(blob_service, container, path):
    try:
        container_client = blob_service.get_container_client(container)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            return blob.download_blob().readall().decode('utf-8-sig')
        return ""
    except Exception as e:
        print(f"  ⚠️ Lỗi download {path}: {e}")
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


def load_table_merge(cursor, table, df, columns, id_col, update_cols=None):
    """
    Load dữ liệu dùng UPDATE + INSERT (không DELETE)
    """
    if df.empty:
        print(f"  ⚠️ {table}: No data")
        return 0, 0
    
    if update_cols is None:
        update_cols = [c for c in columns if c != id_col]
    
    # Lấy danh sách ID hiện có
    cache_key = f"{table}.{id_col}"
    if cache_key in _EXISTING_CACHE:
        existing_ids = _EXISTING_CACHE[cache_key]
    else:
        cursor.execute(f"SELECT {id_col} FROM {table}")
        existing_ids = {str(row[0]).strip() for row in cursor.fetchall()}
        _EXISTING_CACHE[cache_key] = existing_ids
    
    # Tách thành UPDATE và INSERT
    df_copy = df.copy()
    df_copy['_id_str'] = df_copy[id_col].astype(str).str.strip()
    df_update = df_copy[df_copy['_id_str'].isin(existing_ids)]
    df_insert = df_copy[~df_copy['_id_str'].isin(existing_ids)]
    
    updated = 0
    inserted = 0
    
    # UPDATE
    if not df_update.empty:
        set_clause = ', '.join([f"{c} = ?" for c in update_cols])
        query = f"UPDATE {table} SET {set_clause} WHERE {id_col} = ?"
        
        for _, row in df_update.iterrows():
            data = []
            for c in update_cols:
                val = row[c]
                data.append(str(val)[:500] if val and pd.notna(val) else '')
            data.append(str(row[id_col]).strip())
            cursor.execute(query, data)
        
        cursor.connection.commit()
        updated = len(df_update)
    
    # INSERT
    if not df_insert.empty:
        placeholders = ', '.join(['?'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        data = []
        for _, row in df_insert.iterrows():
            tuple_data = []
            for c in columns:
                val = row[c]
                tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
            data.append(tuple(tuple_data))
        
        cursor.fast_executemany = True
        cursor.executemany(query, data)
        cursor.connection.commit()
        inserted = len(df_insert)
        
        # Cập nhật cache
        _EXISTING_CACHE[cache_key].update(df_insert['_id_str'].tolist())
    
    if updated > 0 or inserted > 0:
        print(f"    -> Updated: {updated}, Inserted: {inserted}")
    
    return updated, inserted


def get_or_create_ma_khoa(cursor, ten_khoa, khoa_db, existing_khoa_ids):
    """Lấy MaKhoa từ dict hoặc tạo mới nếu chưa có"""
    ten_khoa_str = str(ten_khoa).strip() if pd.notna(ten_khoa) else ''
    if not ten_khoa_str:
        ten_khoa_str = 'Trường Đại học Kinh tế'
    
    # Kiểm tra trong dict đã load từ DB
    if ten_khoa_str in khoa_db:
        return khoa_db[ten_khoa_str]
    
    # Tạo mới
    new_id = generate_id('KHOA', existing_khoa_ids)
    existing_khoa_ids.add(new_id)
    khoa_db[ten_khoa_str] = new_id
    print(f"    -> Tạo Khoa mới: {new_id} - {ten_khoa_str}")
    
    # Insert vào DB ngay
    try:
        cursor.execute(
            "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)",
            (new_id, ten_khoa_str)
        )
        cursor.connection.commit()
    except Exception as e:
        # Nếu lỗi, lấy lại từ DB
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA WHERE TenKhoa = ?", (ten_khoa_str,))
        row = cursor.fetchone()
        if row:
            new_id = str(row[0]).strip()
            khoa_db[ten_khoa_str] = new_id
            existing_khoa_ids.add(new_id)
            print(f"    -> Khoa đã tồn tại: {new_id}")
    
    return new_id


def determine_ma_chuyen_nganh_batch(lop_series):
    """Xử lý batch để xác định chuyên ngành cho lớp"""
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
            results.append((ma_cn, 'TĐHKT'))
            continue
        
        # TH2: Chứa QT (không có CTS)
        if 'QT' in lop_upper:
            match = re.search(r'(\d{2})KQT', lop_upper)
            if match:
                ma_cn = f"QT_{match.group(1)}K"
            else:
                ma_cn = "QT"
            results.append((ma_cn, 'PĐT'))
            continue
        
        # TH3: Lớp thường Kxx
        match = re.search(r'K(\d{2})', lop_upper)
        if match:
            ma_cn = f"K{match.group(1)}"
            results.append((ma_cn, None))
        else:
            results.append((None, None))
    
    return results


# ================= PIPELINE 1: MASTER DATA =================
def pipeline_master_data(blob_service, cursor):
    """Xử lý master data: HP-Khoa.csv và TenChuyenNganh-Khoa.csv"""
    print("\n" + "=" * 70)
    print("📚 PIPELINE 1: MASTER DATA")
    print("   BƯỚC 1: HP-Khoa.csv -> DIM_KHOA (GỐC) + DIM_HOC_PHAN")
    print("   BƯỚC 2: TenChuyenNganh-Khoa.csv -> DIM_NGANH + DIM_CHUYEN_NGANH")
    print("=" * 70)
    
    # Load toàn bộ khoa hiện có từ DB
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    khoa_db = {}
    existing_khoa_ids = set()
    for row in cursor.fetchall():
        ten = str(row[1]).strip()
        ma = str(row[0]).strip()
        khoa_db[ten] = ma
        existing_khoa_ids.add(ma)
    
    print(f"  -> Khoa hiện có: {len(khoa_db)}")
    
    # Load toàn bộ ngành hiện có
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # Load toàn bộ chuyên ngành hiện có
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # Load toàn bộ học phần hiện có
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    print(f"  -> Existing: Nganh={len(existing_nganh_ids)}, CN={len(existing_cn_ids)}, HP={len(existing_hp_ids)}")
    
    # ==========================================
    # BƯỚC 1: HP-Khoa.csv
    # ==========================================
    print("\n" + "=" * 60)
    print("📄 BƯỚC 1: HP-Khoa.csv (Khoa GỐC)")
    print("   -> DIM_KHOA + DIM_HOC_PHAN")
    print("=" * 60)
    
    content_hp = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    
    if not content_hp:
        print("❌ Không tìm thấy file HP-Khoa.csv!")
    else:
        df_hp = pd.read_csv(io.StringIO(content_hp))
        df_hp.columns = [c.strip() for c in df_hp.columns]
        print(f"  -> {len(df_hp)} dòng, columns: {list(df_hp.columns)[:5]}...")
        
        # Tìm cột
        col_ma_hp = None
        col_ten_hp = None
        col_khoa_hp = None
        
        for col in df_hp.columns:
            col_lower = col.lower().strip()
            if 'mã học phần' in col_lower or 'mã hp' in col_lower:
                col_ma_hp = col
            elif 'tên học phần' in col_lower or 'tên hp' in col_lower:
                col_ten_hp = col
            elif 'khoa' in col_lower and 'mã' not in col_lower:
                col_khoa_hp = col
        
        if not col_ma_hp:
            cols = [c for c in df_hp.columns if 'unnamed' not in c.lower() and 'stt' not in c.lower()]
            if len(cols) >= 3:
                col_ma_hp = cols[0]
                col_khoa_hp = cols[1]
                col_ten_hp = cols[2]
        
        print(f"  -> Cột: MaHP={col_ma_hp}, Khoa={col_khoa_hp}, TenHP={col_ten_hp}")
        
        if col_ma_hp:
            # Tạo DataFrame
            df_hp_data = pd.DataFrame()
            df_hp_data['MaHP'] = df_hp[col_ma_hp].astype(str).str.strip()
            df_hp_data['TenHP'] = df_hp[col_ten_hp].astype(str).str.strip() if col_ten_hp else ''
            df_hp_data['Khoa_Original'] = df_hp[col_khoa_hp].astype(str).str.strip() if col_khoa_hp else ''
            
            # Xử lý đặc biệt: Ngữ Văn, Toán -> Trường ĐHSP
            df_hp_data['TenKhoa'] = df_hp_data['Khoa_Original'].apply(
                lambda x: 'Trường Đại học Sư phạm'
                if isinstance(x, str) and ('Ngữ Văn' in x or 'Toán' in x)
                else x
            )
            
            special_mask = df_hp_data['TenKhoa'] != df_hp_data['Khoa_Original']
            if special_mask.sum() > 0:
                print(f"  -> Đặc biệt: {special_mask.sum()} HP đổi Khoa -> Trường ĐHSP")
            
            # Tạo DIM_KHOA từ HP-Khoa.csv
            print("\n  📌 1.1: Tạo DIM_KHOA từ HP-Khoa.csv (GỐC)...")
            khoa_from_hp = df_hp_data['TenKhoa'].dropna().unique()
            print(f"  -> {len(khoa_from_hp)} Khoa từ HP-Khoa.csv")
            
            for tk in khoa_from_hp:
                tk_str = str(tk).strip()
                if tk_str:
                    get_or_create_ma_khoa(cursor, tk_str, khoa_db, existing_khoa_ids)
            
            # Load DIM_HOC_PHAN
            print("\n  📌 1.2: Load DIM_HOC_PHAN...")
            df_hp_data['MaKhoa'] = df_hp_data['TenKhoa'].apply(
                lambda x: get_or_create_ma_khoa(cursor, x, khoa_db, existing_khoa_ids)
            )
            
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != '']
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != 'nan']
            df_hp_data = df_hp_data.drop_duplicates('MaHP')
            
            print(f"  -> {len(df_hp_data)} HP sau xử lý")
            
            df_hp_out = df_hp_data[['MaHP', 'TenHP', 'MaKhoa']]
            updated, inserted = load_table_merge(cursor, 'DIM_HOC_PHAN', df_hp_out,
                                                  ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP',
                                                  ['TenHP', 'MaKhoa'])
            print(f"  ✅ DIM_HOC_PHAN: Updated={updated}, Inserted={inserted}")
    
    # ==========================================
    # BƯỚC 2: TenChuyenNganh-Khoa.csv
    # ==========================================
    print("\n" + "=" * 60)
    print("📄 BƯỚC 2: TenChuyenNganh-Khoa.csv (BỔ SUNG)")
    print("   -> DIM_KHOA (bổ sung) + DIM_NGANH + DIM_CHUYEN_NGANH")
    print("=" * 60)
    
    content_cn = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    
    if not content_cn:
        print("❌ Không tìm thấy file TenChuyenNganh-Khoa.csv!")
    else:
        df_cn = pd.read_csv(io.StringIO(content_cn))
        df_cn.columns = [c.strip() for c in df_cn.columns]
        print(f"  -> {len(df_cn)} dòng, columns: {list(df_cn.columns)}")
        
        # Tìm cột
        col_ma_cn = None
        col_ten_cn = None
        col_ten_nganh = None
        col_ten_khoa = None
        
        for col in df_cn.columns:
            col_lower = col.lower().strip()
            if 'mã cn' in col_lower:
                col_ma_cn = col
            elif 'chuyên ngành' in col_lower:
                col_ten_cn = col
            elif 'ngành' in col_lower and 'chuyên' not in col_lower and 'khối' not in col_lower:
                col_ten_nganh = col
            elif 'khoa' in col_lower and 'mã' not in col_lower:
                col_ten_khoa = col
        
        if not col_ma_cn:
            cols = df_cn.columns.tolist()
            if len(cols) >= 5:
                col_ten_khoa = col_ten_khoa or cols[1]
                col_ten_nganh = col_ten_nganh or cols[2]
                col_ten_cn = col_ten_cn or cols[3]
                col_ma_cn = cols[4]
        
        print(f"  -> Cột: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
        
        if col_ma_cn:
            # Bổ sung khoa từ file CN
            print("\n  📌 2.1: Bổ sung Khoa từ TenChuyenNganh-Khoa.csv...")
            
            khoa_from_cn = set()
            if col_ten_khoa:
                for tk in df_cn[col_ten_khoa].dropna():
                    tk_str = str(tk).strip()
                    if tk_str:
                        khoa_from_cn.add(tk_str)
            
            default_khoas = ['Trường Đại học Kinh tế', 'Trường Đại học Sư phạm', 'Phòng Đào tạo']
            for dk in default_khoas:
                khoa_from_cn.add(dk)
            
            khoa_moi = 0
            for tk in khoa_from_cn:
                if tk not in khoa_db:
                    get_or_create_ma_khoa(cursor, tk, khoa_db, existing_khoa_ids)
                    khoa_moi += 1
            
            print(f"  -> Bổ sung {khoa_moi} Khoa mới từ file CN" if khoa_moi > 0 else "  -> Không có Khoa mới")
            
            # Tạo ngành
            print("\n  📌 2.2: Tạo DIM_NGANH...")
            nganh_list = []
            
            if col_ten_nganh and col_ten_khoa:
                for _, row in df_cn.iterrows():
                    ten_khoa = str(row[col_ten_khoa]).strip() if pd.notna(row[col_ten_khoa]) else ''
                    ten_nganh = str(row[col_ten_nganh]).strip() if pd.notna(row[col_ten_nganh]) else ''
                    
                    if ten_khoa and ten_nganh:
                        ma_khoa = get_or_create_ma_khoa(cursor, ten_khoa, khoa_db, existing_khoa_ids)
                        key = f"{ma_khoa}_{ten_nganh}"
                        
                        existing_keys = [f"{n['MaKhoa']}_{n['TenNganh']}" for n in nganh_list]
                        if key not in existing_keys:
                            ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                            existing_nganh_ids.add(ma_nganh)
                            nganh_list.append({
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh,
                                'MaKhoa': ma_khoa
                            })
            
            print(f"  -> {len(nganh_list)} Ngành")
            
            if nganh_list:
                df_nganh = pd.DataFrame(nganh_list)[['MaNganh', 'TenNganh', 'MaKhoa']]
                updated, inserted = load_table_merge(cursor, 'DIM_NGANH', df_nganh,
                                                     ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh',
                                                     ['TenNganh', 'MaKhoa'])
                print(f"  ✅ DIM_NGANH: Updated={updated}, Inserted={inserted}")
            
            # Tạo chuyên ngành
            print("\n  📌 2.3: Tạo DIM_CHUYEN_NGANH...")
            cn_list = []
            
            if col_ma_cn and col_ten_cn:
                for _, row in df_cn.iterrows():
                    ma_cn = str(row[col_ma_cn]).strip() if pd.notna(row[col_ma_cn]) else ''
                    ten_cn = str(row[col_ten_cn]).strip() if pd.notna(row[col_ten_cn]) else ''
                    ten_nganh = str(row[col_ten_nganh]).strip() if col_ten_nganh and pd.notna(row[col_ten_nganh]) else ''
                    ten_khoa = str(row[col_ten_khoa]).strip() if col_ten_khoa and pd.notna(row[col_ten_khoa]) else ''
                    
                    if ma_cn and ma_cn not in existing_cn_ids:
                        ma_khoa = get_or_create_ma_khoa(cursor, ten_khoa, khoa_db, existing_khoa_ids) if ten_khoa else 'KHOA01'
                        
                        # Tìm MaNganh
                        ma_nganh = ''
                        for n in nganh_list:
                            if n['TenNganh'] == ten_nganh and n['MaKhoa'] == ma_khoa:
                                ma_nganh = n['MaNganh']
                                break
                        
                        if not ma_nganh:
                            ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                            existing_nganh_ids.add(ma_nganh)
                            nganh_list.append({
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh if ten_nganh else 'Ngành mặc định',
                                'MaKhoa': ma_khoa
                            })
                            
                            df_nganh_new = pd.DataFrame([{
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh if ten_nganh else 'Ngành mặc định',
                                'MaKhoa': ma_khoa
                            }])
                            load_table_merge(cursor, 'DIM_NGANH', df_nganh_new,
                                           ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh',
                                           ['TenNganh', 'MaKhoa'])
                        
                        if ma_cn not in [c['MaChuyenNganh'] for c in cn_list]:
                            cn_list.append({
                                'MaChuyenNganh': ma_cn,
                                'TenChuyenNganh': ten_cn if ten_cn else f'CN {ma_cn}',
                                'MaNganh': ma_nganh
                            })
                            existing_cn_ids.add(ma_cn)
            
            print(f"  -> {len(cn_list)} Chuyên ngành mới")
            
            if cn_list:
                df_cn_out = pd.DataFrame(cn_list)[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']]
                updated, inserted = load_table_merge(cursor, 'DIM_CHUYEN_NGANH', df_cn_out,
                                                     ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh',
                                                     ['TenChuyenNganh', 'MaNganh'])
                print(f"  ✅ DIM_CHUYEN_NGANH: Updated={updated}, Inserted={inserted}")
    
    return khoa_db, existing_khoa_ids, existing_nganh_ids, existing_cn_ids, existing_hp_ids


# ================= NLP CLASS =================
class VietnameseNLP:
    def __init__(self):
        self.positive_words = {
            'tuyệt vời': 2.0, 'xuất sắc': 2.0, 'hoàn hảo': 2.0,
            'rất tốt': 1.5, 'rất hay': 1.5, 'cực kỳ': 1.5,
            'tốt': 1.0, 'hay': 1.0, 'ổn': 1.0, 'hài lòng': 1.0,
            'cảm ơn': 1.0, 'ok': 1.0, 'oke': 1.0
        }
        
        self.negative_words = {
            'tệ': -1.0, 'dở': -1.0, 'kém': -1.0, 'chán': -1.0,
            'khó hiểu': -1.0, 'lan man': -1.0, 'dài dòng': -1.0
        }
        
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
        """Xử lý batch NLP nhanh"""
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


# ================= PARSE SURVEY DATA =================
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
            
            submission_id = f"{ma_sv}_{lop_hp}_{ma_gv}_{file_name}"
            
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


def parse_survey_to_long_format(content: str, file_name: str) -> pd.DataFrame:
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


# ================= PIPELINE 2: SURVEY DATA =================
def pipeline_survey_data(blob_service, cursor, khoa_db, existing_khoa_ids, 
                         existing_nganh_ids, existing_cn_ids, existing_hp_ids):
    """Xử lý survey data"""
    print("\n" + "=" * 70)
    print("📋 PIPELINE 2: SURVEY DATA")
    print("   BƯỚC 3: Parse survey -> Transform NLP -> Load dimensions -> Load facts")
    print("=" * 70)
    
    # Đọc dữ liệu survey
    print(f"\n📥 Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return 0, 0
    
    # Parse dữ liệu
    print("\n📝 Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content, FILE_NAME)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return 0, 0
    print(f"  ✅ Parse: {len(df_raw):,} dòng trong {parse_time:.1f}s")
    
    # Transform & NLP
    print("\n🔄 Transform & NLP...")
    transform_start = time.time()
    
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
    
    transform_time = time.time() - transform_start
    print(f"  ✅ Transform: {transform_time:.1f}s")
    
    # Lưu backup
    print("\n💾 Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    
    # Load dimensions còn lại
    print("\n📥 Load các bảng DIM còn lại...")
    db_start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # DIM_HOC_KY
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                       ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
        print(f"     ✅ Đã thêm {ma_hoc_ky}")
    
    # DIM_GIANG_VIEN
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    if not df_gv.empty:
        existing_gv = set()
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        for row in cursor.fetchall():
            existing_gv.add(str(row[0]).strip())
        
        new_gv = [(r['MaGV'], r['HoDemGV'] or '', r['TenGV'] or '') 
                  for _, r in df_gv.iterrows() if r['MaGV'] not in existing_gv]
        if new_gv:
            cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", new_gv)
            cursor.connection.commit()
            print(f"     ✅ Thêm {len(new_gv)} giảng viên mới")
    
    # DIM_LOP_SINH_VIEN (xử lý đặc biệt CTS/QT)
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    lops = df_lop_unique['Lop'].tolist()
    
    # Batch xác định chuyên ngành
    cn_results = determine_ma_chuyen_nganh_batch(lops)
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    new_lop_data = []
    for lop, (ma_cn, ma_khoa) in zip(lops, cn_results):
        if lop in existing_lop:
            continue
        if ma_cn:
            # Kiểm tra chuyên ngành tồn tại
            cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = ?", ma_cn)
            if cursor.fetchone():
                new_lop_data.append((lop, lop, ma_cn))
    
    if new_lop_data:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop_data)
        cursor.connection.commit()
        print(f"     ✅ Thêm {len(new_lop_data)} lớp mới")
    
    # DIM_SINH_VIEN
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    valid_lop = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {row[0] for row in cursor.fetchall()}
    
    new_sv = []
    for _, r in df_sv.iterrows():
        if r['MaSV'] not in existing_sv and r['Lop'] in valid_lop:
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
    
    # DIM_LOP_HOC_PHAN
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    valid_hp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    valid_gv = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {row[0] for row in cursor.fetchall()}
    
    new_lhp = []
    for _, r in df_lhp.iterrows():
        if (r['LopHP'] not in existing_lhp and 
            r['MaHP'] in valid_hp and 
            r['MaGV'] in valid_gv):
            new_lhp.append((r['LopHP'], r['LopHP'], r['MaHP'], r['MaGV'], ma_hoc_ky))
    
    if new_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", new_lhp)
        cursor.connection.commit()
        print(f"     ✅ Thêm {len(new_lhp)} lớp học phần mới")
    
    # Load FACT tables
    print("\n📥 Loading FACT tables...")
    
    # Lấy danh sách hợp lệ
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    valid_sv_db = {row[0] for row in cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")}
    
    print(f"     - Số LopHP hợp lệ: {len(valid_lophp)}")
    print(f"     - Số MaSV hợp lệ: {len(valid_sv_db)}")
    
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
                if row['MaSV'] not in valid_sv_db or row['LopHP'] not in valid_lophp:
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
            
            # Tạo data đầy đủ 12 câu
            all_questions = list(range(1, 13))
            submission_data = fact_ketqua.groupby('SubmissionID').apply(
                lambda x: dict(zip(x['MaCauHoi'], x['Diem']))
            ).to_dict()
            
            final_data = []
            for sub_id in valid_subs:
                answers = submission_data.get(sub_id, {})
                for q in all_questions:
                    diem = answers.get(q, 5)
                    final_data.append((sub_id, q, diem))
            
            if final_data:
                # Loại bỏ duplicate
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
        print(f"  ❌ Lỗi: {e}")
        raise e
    finally:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.connection.commit()
    
    db_time = time.time() - db_start
    print(f"  ✅ Database load completed in {db_time:.1f}s")
    
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 ETL PIPELINE - FULL PROCESSING")
    print("   PIPELINE 1: Master Data (HP-Khoa.csv + TenChuyenNganh-Khoa.csv)")
    print("   PIPELINE 2: Survey Data (Parse + NLP + Load)")
    print("=" * 70)
    print(f"Survey file: {SURVEY_FILE}")
    print(f"Semester: {SEMESTER}")
    print("=" * 70)
    
    # Kết nối
    print("\n📥 Kết nối Azure & Database...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    count_main = 0
    count_kq = 0
    
    try:
        # PIPELINE 1: MASTER DATA
        khoa_db, existing_khoa_ids, existing_nganh_ids, existing_cn_ids, existing_hp_ids = \
            pipeline_master_data(blob_service, cursor)
        
        # PIPELINE 2: SURVEY DATA
        count_main, count_kq = pipeline_survey_data(blob_service, cursor, khoa_db, 
                                                     existing_khoa_ids, existing_nganh_ids,
                                                     existing_cn_ids, existing_hp_ids)
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    # Tổng kết
    total_time = time.time() - total_start
    print("\n" + "=" * 70)
    print("📊 TỔNG KẾT")
    print("=" * 70)
    print(f"  SUBMISSIONS: {count_main:,}")
    print(f"  ANSWERS: {count_kq:,}")
    print(f"  TOTAL TIME: {total_time:.1f}s")
    print("=" * 70)
    print("🎉 HOÀN THÀNH!")
    print("=" * 70)


if __name__ == "__main__":
    main()
