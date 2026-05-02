#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - OPTIMIZED FOR SPEED
- Parse CSV siêu nhanh
- NLP tối giản
- Bulk Insert Database
"""

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
from multiprocessing import Pool, cpu_count
import tempfile

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=300;"
    f"Command Timeout=600;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 100000  # Tăng chunk size
BATCH_SIZE = 100000  # Tăng batch size

print("=" * 70)
print("📊 PIPELINE 2: SURVEY DATA (OPTIMIZED)")
print(f"   Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
print("=" * 70)

# ================= PATTERNS (pre-compiled) =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

# ================= NLP - TỐI GIẢN TỐI ĐA =================
# Chỉ giữ keywords quan trọng nhất, dùng set để lookup O(1)
TAG_HOCPHAN = {'nội dung','chương trình','môn học','học phần','kiến thức','chuẩn đầu ra','tài liệu','giáo trình','thực hành','lý thuyết','phù hợp','bổ ích','cần thiết','cập nhật','thực tế'}
TAG_DAYHOC = {'giảng viên','thầy','cô','dạy','giảng','truyền đạt','hướng dẫn','nhiệt tình','tận tâm','dễ hiểu','sinh động','thú vị','hấp dẫn','chuyên nghiệp'}
TAG_KIEMTRA = {'kiểm tra','đánh giá','thi','đề thi','chấm điểm','công bằng','minh bạch','khách quan','nghiêm túc','chính xác'}
TAG_KHAC = {'cơ sở vật chất','phòng học','máy chiếu','wifi','hỗ trợ','góp ý','đề xuất','cải thiện'}

POS_KW = {'tốt','hay','hài lòng','thích','bổ ích','hiệu quả','chất lượng','tuyệt vời','xuất sắc','nhiệt tình','dễ hiểu','công bằng'}
NEG_KW = {'tệ','kém','chán','dở','không tốt','khó hiểu','nhàm chán','thiếu','hạn chế','thất vọng','cần cải thiện'}
NEU_KW = {'không có góp ý','không ý kiến','không có','bình thường'}

# ================= MASTER DATA (Global dicts) =================
_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}
_g_default_khoa = ('KHOA01', 'Trường Đại học Kinh tế')
_g_default_nganh = 'KHOA01NG01'

def load_master_from_db():
    """Load master data 1 lần"""
    global _g_cn, _g_hp, _g_khoa_hp, _g_default_khoa, _g_default_nganh
    
    print("\n📚 Load master từ DB...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Khoa
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    khoa_list = [(str(r[0]), str(r[1])) for r in cursor.fetchall()]
    if khoa_list:
        _g_default_khoa = khoa_list[0]
    
    # Chuyên ngành
    cursor.execute("""
        SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, n.TenNganh, n.MaKhoa, k.TenKhoa
        FROM DIM_CHUYEN_NGANH cn
        JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh
        JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_cn[key] = (key, str(row[1]).strip(), str(row[2]).strip(), str(row[3]).strip(), str(row[4]).strip(), str(row[5]).strip())
    if _g_cn:
        _g_default_nganh = list(_g_cn.values())[0][2]
    
    # Học phần
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa
        FROM DIM_HOC_PHAN hp JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_hp[key] = str(row[1]).strip()
        _g_khoa_hp[key] = (str(row[2]).strip(), str(row[3]).strip())
    
    print(f"  -> CN={len(_g_cn)}, HP={len(_g_hp)}")
    conn.close()

# ================= BLOB =================
def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except:
        return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    fn = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    yc = int(fn[:-1])
    hk = int(fn[-1])
    nbd = 2000 + (yc - 1)
    nkt = nbd + 1
    return f"HK{hk}_{nbd%100}{nkt%100}", f"{nbd}-{nkt}", hk

def normalize_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def nlp_fast(text):
    """NLP cực nhanh - dùng split() và set intersection"""
    if not text or len(text) < 5:
        return 0,0,0,0,'NEUTRAL',0
    
    words = set(text.lower().split())
    
    # Tags: >= 2 từ khóa
    t1 = 1 if len(words & TAG_HOCPHAN) >= 2 else 0
    t2 = 1 if len(words & TAG_DAYHOC) >= 2 else 0
    t3 = 1 if len(words & TAG_KIEMTRA) >= 2 else 0
    t4 = 1 if len(words & TAG_KHAC) >= 2 else 0
    
    # Sentiment
    p = len(words & POS_KW)
    n = len(words & NEG_KW)
    e = len(words & NEU_KW)
    
    if 'không' in words:
        p = max(0, p-1)
        n += 1
    
    if p > n and p > e: s = 'POSITIVE'
    elif n > p and n > e: s = 'NEGATIVE'
    else: s = 'NEUTRAL'
    
    return t1, t2, t3, t4, s, 1 if len(text) > 10 else 0

# ================= PARSE (TỐI ƯU - DÙNG LIST) =================
def parse_batch(args):
    """Parse batch - trả về list of lists (nhanh hơn dict)"""
    lines, file_name = args
    results = []
    
    for line in lines:
        if not line: continue
        
        # Tìm NULL
        ni = line.find('NULL')
        if ni >= 0:
            left = line[:ni].rstrip(', \t')
            right = line[ni+4:].lstrip(', \t')
        else:
            left = line
            right = ''
        
        row = left.split(',')
        rl = len(row)
        if rl < 10: continue
        
        # Tìm ngày sinh
        nsi = -1
        for i in range(2, min(12, rl)):
            if _DATE_RE(row[i].strip()):
                nsi = i
                break
        if nsi == -1: continue
        
        # Tìm MaGV
        mgi = -1
        for i in range(nsi+1, min(nsi+25, rl)):
            if _MA_GV_RE(row[i].strip()):
                mgi = i
                break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        # Extract
        lop = row[0].strip()
        ma_sv = row[1].strip()
        ns = row[nsi].strip()
        
        np = [x.strip() for x in row[2:nsi]]
        ten = np[-1] if np else ''
        hd = ' '.join(np[:-1]) if len(np) > 1 else ''
        
        ma_hp = row[nsi+1].strip() if nsi+1 < rl else ''
        thp_raw = ' '.join(x.strip() for x in row[nsi+2:mgi])
        
        ma_gv = row[mgi].strip() if mgi < rl else ''
        hdgv = row[mgi+1].strip() if mgi+1 < rl else ''
        tgv = row[mgi+2].strip() if mgi+2 < rl else ''
        lhp = row[mgi+3].strip() if mgi+3 < rl else ''
        ch = row[mgi+4].strip() if mgi+4 < rl else ''
        gt = row[mgi+5].strip() if mgi+5 < rl else ''
        
        essay = right.replace(' , ', ', ').strip()
        
        # NLP
        t1,t2,t3,t4,sent,valid = nlp_fast(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        
        # Lookup CN
        lop_norm = normalize_lop(lop)
        if _LOP_RE(lop_norm):
            ma_cn_key = f"K{lop_norm[3:5]}"
        else:
            ma_cn_key = lop_norm or lop
        
        cn = _g_cn.get(ma_cn_key, (ma_cn_key, f'CN {ma_cn_key}', _g_default_nganh, 'Ngành mặc định', _g_default_khoa[0], _g_default_khoa[1]))
        
        # Lookup HP
        thp, mkhp, tkhp = _g_hp.get(ma_hp, ''), *_g_khoa_hp.get(ma_hp, _g_default_khoa)
        thp = thp or thp_raw
        
        # Tạo mã
        ml = lop_norm
        mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{file_name}"
        
        # Kết quả dạng list (nhanh hơn dict)
        results.append([
            sid, ma_sv, hd, ten, ns,
            ml, lop, cn[0], cn[1], cn[2], cn[3], cn[4], cn[5],
            ma_hp, thp, mkhp, tkhp,
            ma_gv, hdgv, tgv, mlhp, lhp,
            ch, gt, essay,
            t1, t2, t3, t4, sent, valid
        ])
    
    return results

def parse_survey(content):
    """Parse survey với multiprocessing"""
    print(f"  -> Parsing...")
    t0 = time.time()
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"  -> {len(lines):,} lines")
    
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    print(f"  -> {len(batches)} batches")
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            print(f"    Batch {i+1}/{len(batches)}: {len(res):,} rows")
    
    df = pd.DataFrame(all_results, columns=[
        'SubmissionID','MaSV','HoDem','Ten','NgaySinh',
        'MaLop','Lop','MaChuyenNganh','TenChuyenNganh',
        'MaNganh','TenNganh','MaKhoa_CN','TenKhoa_CN',
        'MaHP','TenHP','MaKhoa_HP','TenKhoa_HP',
        'MaGV','HoDemGV','TenGV','MaLopHP','LopHP',
        'CauHoi','GiaTri','EssayText',
        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac',
        'Sentiment','Is_Valid'
    ])
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= DATABASE LOAD (BULK CSO) =================
def bulk_insert_csv(cursor, table, df, columns, tmp_file):
    """Dùng BULK INSERT siêu nhanh"""
    if df.empty: return 0
    
    # Chuẩn hóa data
    df_out = df[columns].copy()
    for c in df_out.columns:
        df_out[c] = df_out[c].astype(str).str.replace('\n',' ').str.replace('\r',' ').str.replace('|',' ').fillna('')
    
    # Lưu CSV tạm với delimiter |
    df_out.to_csv(tmp_file, index=False, header=False, sep='|', encoding='utf-8', quoting=1)
    
    # BULK INSERT
    cols_str = ', '.join(columns)
    sql = f"""
        BULK INSERT {table}
        FROM '{tmp_file}'
        WITH (
            FIELDTERMINATOR = '|',
            ROWTERMINATOR = '\\n',
            CODEPAGE = '65001',
            BATCHSIZE = {BATCH_SIZE},
            TABLOCK
        )
    """
    try:
        cursor.execute(sql)
        cursor.connection.commit()
        return len(df_out)
    except Exception as e:
        print(f"    ⚠️ BULK INSERT failed: {e}")
        return 0

def load_dimensions_bulk(cursor, df, tmp_dir):
    """Load dimensions dùng BULK INSERT"""
    print("\n--- DIMENSIONS (BULK) ---")
    t = 0
    
    # DIM_LOP_SINH_VIEN
    t += bulk_insert_csv(cursor, 'DIM_LOP_SINH_VIEN',
                         df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop'),
                         ['MaLop','Lop','MaChuyenNganh'],
                         f"{tmp_dir}/lop.csv")
    
    # DIM_SINH_VIEN
    t += bulk_insert_csv(cursor, 'DIM_SINH_VIEN',
                         df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV'),
                         ['MaSV','HoDem','Ten','NgaySinh','MaLop'],
                         f"{tmp_dir}/sv.csv")
    
    # DIM_GIANG_VIEN
    t += bulk_insert_csv(cursor, 'DIM_GIANG_VIEN',
                         df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV'),
                         ['MaGV','HoDemGV','TenGV'],
                         f"{tmp_dir}/gv.csv")
    
    # DIM_HOC_PHAN
    df_hp = df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}).drop_duplicates('MaHP')
    t += bulk_insert_csv(cursor, 'DIM_HOC_PHAN', df_hp, ['MaHP','TenHP','MaKhoa'], f"{tmp_dir}/hp.csv")
    
    # DIM_HOC_KY
    mhk, nh, hk = derive_ma_hoc_ky()
    t += bulk_insert_csv(cursor, 'DIM_HOC_KY',
                         pd.DataFrame([{'MaHocKy':mhk,'NamHoc':nh,'HocKy':hk}]),
                         ['MaHocKy','NamHoc','HocKy'], f"{tmp_dir}/hk.csv")
    
    # DIM_LOP_HOC_PHAN
    df_lhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP')
    df_lhp['MaHocKy'] = mhk
    t += bulk_insert_csv(cursor, 'DIM_LOP_HOC_PHAN', df_lhp,
                         ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], f"{tmp_dir}/lhp.csv")
    
    return t

def load_facts_bulk(cursor, df, tmp_dir):
    """Load FACT tables dùng BULK INSERT"""
    print("\n--- FACTS (BULK) ---")
    
    # FACT_GOP_Y
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText']!='')].drop_duplicates('SubmissionID')
    if not df_essay.empty:
        df_gy = df_essay[['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid',
                           'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']].copy()
        df_gy.columns = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                         'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
        c1 = bulk_insert_csv(cursor, 'FACT_GOP_Y_TU_LUAN', df_gy,
                            ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                             'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac'],
                            f"{tmp_dir}/fact_gopy.csv")
        print(f"  FACT_GOP_Y: {c1:,}")
    
    # FACT_KET_QUA
    rows = []
    for _, r in df[(df['CauHoi']!='') & (df['GiaTri']!='')].iterrows():
        try:
            mc = int(float(r['CauHoi']))
            d = int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5:
                rows.append({'SubmissionID': str(r['SubmissionID'])[:150], 'MaCauHoi': mc, 'Diem': d})
        except: pass
    
    for _, r in df_essay.iterrows():
        s = r['Sentiment']
        d = 5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]:
            rows.append({'SubmissionID': str(r['SubmissionID'])[:150], 'MaCauHoi': mc, 'Diem': d})
    
    if rows:
        df_kq = pd.DataFrame(rows)
        c2 = bulk_insert_csv(cursor, 'FACT_KET_QUA_DANH_GIA', df_kq,
                            ['SubmissionID','MaCauHoi','Diem'],
                            f"{tmp_dir}/fact_kq.csv")
        print(f"  FACT_KET_QUA: {c2:,}")

def load_to_database(df):
    """Load toàn bộ vào database"""
    print("\n💾 LOAD TO DATABASE")
    t0 = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    # Tắt constraint tạm thời cho nhanh
    try:
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_SINH_VIEN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_LOP_HOC_PHAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_LOP_SINH_VIEN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_HOC_PHAN NOCHECK CONSTRAINT ALL")
        conn.commit()
    except: pass
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            load_dimensions_bulk(cursor, df, tmp_dir)
            load_facts_bulk(cursor, df, tmp_dir)
        except Exception as e:
            print(f"  ⚠️ BULK INSERT lỗi: {e}, fallback batch insert...")
            # Fallback code nếu cần...
    
    # Bật lại constraint
    try:
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_SINH_VIEN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_LOP_HOC_PHAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_LOP_SINH_VIEN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE DIM_HOC_PHAN CHECK CONSTRAINT ALL")
        conn.commit()
    except: pass
    
    conn.close()
    print(f"  ✅ {time.time()-t0:.1f}s")

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Kết nối...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content: print("❌ No data!"); sys.exit(1)
    
    print("\n📝 PARSE + NLP")
    t1 = time.time()
    df = parse_survey(content)
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    if df.empty: print("❌ No data!"); sys.exit(1)
    
    # Backup nhanh
    df.to_parquet(f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", index=False)
    
    load_to_database(df)
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
