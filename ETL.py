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
CHUNK_SIZE = 100000  # Tăng lên 100k
BATCH_SIZE = 100000  # Batch insert lớn

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')


# ================= UTILS =================
def generate_id(prefix, existing_ids, start=1):
    i = start
    while True:
        new_id = f"{prefix}{i:02d}"
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
        print(f"  ⚠️ Lỗi: {e}")
        return ""


# ================= PIPELINE 1: MASTER DATA (TỐI ƯU) =================
def pipeline_master_data(blob_service, cursor):
    """Xử lý master data - CHỈ INSERT HÀNG LOẠT"""
    print("\n" + "=" * 60)
    print("📚 PIPELINE 1: MASTER DATA (BATCH MODE)")
    print("=" * 60)
    
    # Đọc file HP-Khoa.csv
    content_hp = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    if not content_hp:
        print("❌ Không tìm thấy HP-Khoa.csv!")
        return
    
    df_hp = pd.read_csv(io.StringIO(content_hp))
    df_hp.columns = [c.strip() for c in df_hp.columns]
    
    # Tìm cột
    col_ma_hp = None
    for col in df_hp.columns:
        if 'mã học phần' in col.lower() or 'mã hp' in col.lower():
            col_ma_hp = col
            break
    if not col_ma_hp:
        cols = [c for c in df_hp.columns if 'unnamed' not in c.lower()]
        col_ma_hp = cols[0] if cols else None
    
    if not col_ma_hp:
        print("❌ Không tìm thấy cột Mã HP!")
        return
    
    # Lấy dữ liệu
    ma_hp_list = df_hp[col_ma_hp].astype(str).str.strip().tolist()
    
    # INSERT HÀNG LOẠT vào DIM_HOC_PHAN (nếu chưa có)
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp = {row[0] for row in cursor.fetchall()}
    
    new_hp = [ma for ma in ma_hp_list if ma and ma != 'nan' and ma not in existing_hp]
    
    if new_hp:
        # Batch insert
        data_hp = [(ma, f'Học phần {ma}', 'KHOA01') for ma in new_hp]
        cursor.executemany("INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES (?, ?, ?)", data_hp)
        cursor.connection.commit()
        print(f"  ✅ DIM_HOC_PHAN: Inserted {len(new_hp)} records")
    
    # Đọc file TenChuyenNganh-Khoa.csv
    content_cn = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if content_cn:
        df_cn = pd.read_csv(io.StringIO(content_cn))
        df_cn.columns = [c.strip() for c in df_cn.columns]
        
        # Tìm cột mã chuyên ngành
        col_ma_cn = None
        for col in df_cn.columns:
            if 'mã cn' in col.lower():
                col_ma_cn = col
                break
        if not col_ma_cn and len(df_cn.columns) >= 5:
            col_ma_cn = df_cn.columns[4]
        
        if col_ma_cn:
            ma_cn_list = df_cn[col_ma_cn].astype(str).str.strip().tolist()
            
            cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
            existing_cn = {row[0] for row in cursor.fetchall()}
            
            new_cn = [ma for ma in ma_cn_list if ma and ma != 'nan' and ma not in existing_cn]
            
            if new_cn:
                data_cn = [(ma, f'Chuyên ngành {ma}', 'CN_CTS') for ma in new_cn]
                cursor.executemany("INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)", data_cn)
                cursor.connection.commit()
                print(f"  ✅ DIM_CHUYEN_NGANH: Inserted {len(new_cn)} records")


# ================= PARSE SURVEY DATA (TỐI ƯU) =================
def parse_lines_batch(lines_batch, file_name):
    """Parse một batch dòng"""
    results = []
    for line in lines_batch:
        if not line or not line.strip():
            continue
        row = [x.strip() for x in line.split(',')]
        row_len = len(row)
        if row_len < 15:
            continue
        try:
            # Lấy thông tin cơ bản
            lop = row[0]
            ma_sv = row[1]
            
            # Tìm ngày sinh
            ngay_sinh = ''
            ngay_sinh_idx = -1
            for i in range(2, min(row_len, 12)):
                if _date_pattern.match(row[i]):
                    ngay_sinh = row[i]
                    ngay_sinh_idx = i
                    break
            if ngay_sinh_idx == -1:
                continue
            
            # Tách họ tên
            ho_dem, ten = '', ''
            if ngay_sinh_idx > 1:
                name_parts = [p for p in row[2:ngay_sinh_idx] if p]
                if name_parts:
                    ten = name_parts[-1]
                    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            # Mã HP
            ma_hp = row[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < row_len else ''
            
            # Tìm mã GV
            ma_gv = ''
            ma_gv_idx = -1
            for i in range(ngay_sinh_idx + 2, min(row_len, ngay_sinh_idx + 25)):
                v = row[i]
                if (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH":
                    ma_gv = v
                    ma_gv_idx = i
                    break
            
            if ma_gv_idx == -1:
                ma_gv_idx = row_len - 4 if row_len >= 4 else ngay_sinh_idx + 2
            
            # Tên HP
            ten_hp = ' '.join(row[ngay_sinh_idx + 2:ma_gv_idx]) if ma_gv_idx > ngay_sinh_idx + 2 else ''
            
            # Thông tin GV
            ho_dem_gv = row[ma_gv_idx + 1] if ma_gv_idx + 1 < row_len else ''
            ten_gv = row[ma_gv_idx + 2] if ma_gv_idx + 2 < row_len else ''
            lop_hp = row[ma_gv_idx + 3] if ma_gv_idx + 3 < row_len else ''
            cau_hoi = row[ma_gv_idx + 4] if ma_gv_idx + 4 < row_len else ''
            gia_tri = row[ma_gv_idx + 5] if ma_gv_idx + 5 < row_len else ''
            
            # Tìm phần tự luận
            essay_text = ''
            for i in range(ma_gv_idx + 6, min(row_len, ma_gv_idx + 20)):
                if row[i].upper() == 'NULL' or row[i] == '':
                    if i + 1 < row_len:
                        essay_text = ','.join(row[i+1:]).strip()
                    break
            
            submission_id = f"{ma_sv}_{lop_hp}_{ma_gv}_{file_name}"
            
            results.append({
                'SubmissionID': submission_id,
                'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
                'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
                'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
                'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri,
                'EssayText': essay_text
            })
        except Exception:
            continue
    return results


def parse_survey_parallel(content: str, file_name: str) -> pd.DataFrame:
    """Parse song song với nhiều worker"""
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


# ================= NLP (VECTORIZED - RẤT NHANH) =================
def process_nlp_vectorized(df_text: pd.DataFrame) -> pd.DataFrame:
    """Xử lý NLP bằng vectorized operations - cực nhanh"""
    print("  -> Xử lý NLP...")
    start = time.time()
    
    # Keywords
    positive_words = ['tuyệt vời', 'xuất sắc', 'hoàn hảo', 'rất tốt', 'rất hay', 
                      'tốt', 'hay', 'ổn', 'hài lòng', 'cảm ơn', 'ok', 'oke']
    negative_words = ['tệ', 'dở', 'kém', 'chán', 'khó hiểu', 'lan man', 'dài dòng']
    
    tag_hp_keywords = ['chuẩn đầu ra', 'nội dung', 'học phần', 'môn học']
    tag_dh_keywords = ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng']
    tag_kt_keywords = ['kiểm tra', 'đánh giá', 'thi', 'đề thi']
    
    # Vectorized string operations
    text_lower = df_text['EssayText'].fillna('').str.lower()
    
    # Sentiment (dùng vectorized)
    pos_score = text_lower.apply(lambda x: sum(1 for w in positive_words if w in x))
    neg_score = text_lower.apply(lambda x: sum(1 for w in negative_words if w in x))
    
    df_text['Sentiment'] = 'neutral'
    df_text.loc[pos_score > neg_score, 'Sentiment'] = 'positive'
    df_text.loc[neg_score > pos_score, 'Sentiment'] = 'negative'
    
    # No opinion
    no_opinion_pattern = '|'.join([r'không.*ý kiến', r'^ko$', r'^k$', r'^$'])
    no_opinion_mask = text_lower.str.contains(no_opinion_pattern, na=False, regex=True)
    df_text.loc[no_opinion_mask, 'Sentiment'] = 'neutral'
    
    # Tags
    df_text['Tag_HocPhan'] = text_lower.str.contains('|'.join(tag_hp_keywords), na=False, regex=True).astype(int)
    df_text['Tag_DayHoc'] = text_lower.str.contains('|'.join(tag_dh_keywords), na=False, regex=True).astype(int)
    df_text['Tag_KiemTra'] = text_lower.str.contains('|'.join(tag_kt_keywords), na=False, regex=True).astype(int)
    df_text['Tag_Khac'] = ((df_text['Tag_HocPhan'] + df_text['Tag_DayHoc'] + df_text['Tag_KiemTra']) == 0).astype(int)
    df_text['Is_Valid'] = 1
    
    print(f"  -> NLP done in {time.time()-start:.1f}s")
    return df_text


# ================= PIPELINE 2: SURVEY DATA (TỐI ƯU) =================
def pipeline_survey_data(blob_service, cursor):
    """Xử lý survey data - TỐI ƯU TỐC ĐỘ"""
    print("\n" + "=" * 60)
    print("📋 PIPELINE 2: SURVEY DATA (BATCH MODE)")
    print("=" * 60)
    
    # 1. Đọc và parse
    print("\n📥 Đọc survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("❌ Không đọc được file survey!")
        return 0, 0
    
    df_raw = parse_survey_parallel(survey_content, FILE_NAME)
    if df_raw.empty:
        print("❌ Không có dữ liệu!")
        return 0, 0
    
    # 2. Xử lý tự luận (deduplicate + NLP)
    print("\n🔄 Xử lý tự luận...")
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if not text_df.empty:
        # Deduplicate
        text_df = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        text_df['NoiDungGopY'] = text_df['EssayText'].str[:4000]
        
        # NLP vectorized
        text_df = process_nlp_vectorized(text_df)
        
        fact_main = text_df[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                              'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                              'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].copy()
        print(f"  -> {len(fact_main):,} submissions tự luận")
    else:
        fact_main = pd.DataFrame()
    
    # 3. Xử lý trắc nghiệm
    print("\n🔄 Xử lý trắc nghiệm...")
    mcq_df = df_raw[df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '')].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
        print(f"  -> {len(fact_ketqua):,} câu trắc nghiệm")
    else:
        fact_ketqua = pd.DataFrame()
    
    # 4. Load DIM tables (BATCH)
    print("\n📥 Load DIM tables...")
    db_start = time.time()
    
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # TẮT CONSTRAINTS để insert nhanh
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_LOP_HOC_PHAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_SINH_VIEN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_LOP_SINH_VIEN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_GIANG_VIEN NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    # DIM_HOC_KY
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", 
                       ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
    
    # DIM_GIANG_VIEN - BATCH
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    if not df_gv.empty:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing_gv = {row[0] for row in cursor.fetchall()}
        new_gv = [(r['MaGV'], r['HoDemGV'] or '', r['TenGV'] or '') 
                  for _, r in df_gv.iterrows() if r['MaGV'] not in existing_gv]
        if new_gv:
            cursor.executemany("INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)", new_gv)
            cursor.connection.commit()
            print(f"  ✅ DIM_GIANG_VIEN: +{len(new_gv)}")
    
    # DIM_LOP_SINH_VIEN - BATCH (xử lý nhanh bằng dictionary)
    df_lop = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    # Xác định chuyên ngành cho lớp
    def get_ma_cn(lop):
        if not lop:
            return None
        lop_u = lop.upper()
        if 'CTS' in lop_u:
            return 'CTS'
        if 'QT' in lop_u:
            return 'QT'
        match = re.search(r'K(\d{2})', lop_u)
        return f"K{match.group(1)}" if match else None
    
    new_lop = []
    for lop in df_lop['Lop'].tolist():
        if lop not in existing_lop:
            ma_cn = get_ma_cn(lop)
            if ma_cn:
                new_lop.append((lop, lop, ma_cn))
                existing_lop.add(lop)
    
    if new_lop:
        cursor.executemany("INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)", new_lop)
        cursor.connection.commit()
        print(f"  ✅ DIM_LOP_SINH_VIEN: +{len(new_lop)}")
    
    # DIM_SINH_VIEN - BATCH
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {row[0] for row in cursor.fetchall()}
    
    new_sv = []
    for _, r in df_sv.iterrows():
        if r['MaSV'] not in existing_sv and r['Lop'] in existing_lop:
            ngay_sinh = None
            if r['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(r['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            new_sv.append((r['MaSV'], r['HoDem'] or '', r['Ten'] or '', ngay_sinh, r['Lop']))
            existing_sv.add(r['MaSV'])
    
    if new_sv:
        cursor.executemany("INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES (?, ?, ?, ?, ?)", new_sv)
        cursor.connection.commit()
        print(f"  ✅ DIM_SINH_VIEN: +{len(new_sv)}")
    
    # DIM_LOP_HOC_PHAN - BATCH
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {row[0] for row in cursor.fetchall()}
    
    new_lhp = []
    for _, r in df_lhp.iterrows():
        if r['LopHP'] not in existing_lhp:
            new_lhp.append((r['LopHP'], r['LopHP'], r['MaHP'], r['MaGV'], ma_hoc_ky))
            existing_lhp.add(r['LopHP'])
    
    if new_lhp:
        cursor.executemany("INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)", new_lhp)
        cursor.connection.commit()
        print(f"  ✅ DIM_LOP_HOC_PHAN: +{len(new_lhp)}")
    
    # 5. Load FACT tables
    print("\n📥 Load FACT tables...")
    
    # Lấy danh sách hợp lệ
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN WHERE MaHocKy = ?", ma_hoc_ky)
    valid_lophp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    valid_sv = {row[0] for row in cursor.fetchall()}
    
    # FACT_GOP_Y_TU_LUAN
    if not fact_main.empty:
        data_main = []
        for _, row in fact_main.iterrows():
            if row['MaSV'] in valid_sv and row['LopHP'] in valid_lophp:
                data_main.append((
                    row['SubmissionID'], row['MaSV'], row['LopHP'], row['NoiDungGopY'],
                    row['Sentiment'], row['Is_Valid'],
                    row['Tag_HocPhan'], row['Tag_DayHoc'], row['Tag_KiemTra'], row['Tag_Khac']
                ))
        
        if data_main:
            sql_main = """INSERT INTO FACT_GOP_Y_TU_LUAN 
                         (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, 
                          Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) 
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
            cursor.executemany(sql_main, data_main)
            cursor.connection.commit()
            print(f"  ✅ FACT_GOP_Y_TU_LUAN: {len(data_main):,} records")
            count_main = len(data_main)
        else:
            count_main = 0
    else:
        count_main = 0
    
    # FACT_KET_QUA_DANH_GIA
    if not fact_ketqua.empty and count_main > 0:
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        valid_subs = {row[0] for row in cursor.fetchall()}
        
        # Lọc và tạo đủ 12 câu
        all_q = list(range(1, 13))
        sub_data = fact_ketqua[fact_ketqua['SubmissionID'].isin(valid_subs)]
        
        if not sub_data.empty:
            # Groupby để tạo map
            sub_map = {}
            for _, row in sub_data.iterrows():
                sid = row['SubmissionID']
                q = row['MaCauHoi']
                d = row['Diem']
                if sid not in sub_map:
                    sub_map[sid] = {}
                sub_map[sid][q] = d
            
            # Tạo data đầy đủ
            final_data = []
            for sid in valid_subs:
                answers = sub_map.get(sid, {})
                for q in all_q:
                    diem = answers.get(q, 5)
                    final_data.append((sid, q, diem))
            
            if final_data:
                # Batch insert
                sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
                cursor.executemany(sql_kq, final_data)
                cursor.connection.commit()
                count_kq = len(final_data)
                print(f"  ✅ FACT_KET_QUA_DANH_GIA: {count_kq:,} records")
            else:
                count_kq = 0
        else:
            count_kq = 0
    else:
        count_kq = 0
    
    # BẬT LẠI CONSTRAINTS
    cursor.execute("ALTER TABLE DIM_GIANG_VIEN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_LOP_SINH_VIEN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_SINH_VIEN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE DIM_LOP_HOC_PHAN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    db_time = time.time() - db_start
    print(f"\n  ✅ Database loading completed in {db_time:.1f}s")
    
    return count_main, count_kq


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 ETL PIPELINE - TỐI ƯU TỐC ĐỘ")
    print("   - Batch processing")
    print("   - Vectorized NLP")
    print("   - Large batch inserts")
    print("=" * 70)
    print(f"Survey: {SURVEY_FILE}")
    print(f"Workers: {NUM_WORKERS}")
    print("=" * 70)
    
    # Kết nối
    print("\n📥 Kết nối...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    conn = pyodbc.connect(CONN_STR, autocommit=False)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Pipeline 1: Master data
        pipeline_master_data(blob_service, cursor)
        
        # Pipeline 2: Survey data
        count_main, count_kq = pipeline_survey_data(blob_service, cursor)
        
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    total_time = time.time() - total_start
    print("\n" + "=" * 70)
    print("📊 TỔNG KẾT")
    print("=" * 70)
    print(f"  Submissions: {count_main:,}")
    print(f"  Answers: {count_kq:,}")
    print(f"  Total time: {total_time:.1f}s")
    print("=" * 70)
    print("🎉 HOÀN THÀNH!")
    print("=" * 70)


if __name__ == "__main__":
    main()
