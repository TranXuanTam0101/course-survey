#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA (KHÔNG LOAD MASTER)
- Chỉ load: DIM_SINH_VIEN, DIM_LOP_SINH_VIEN, DIM_GIANG_VIEN, DIM_LOP_HOC_PHAN
- FACT_GOP_Y_TU_LUAN, FACT_KET_QUA_DANH_GIA
- DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH, DIM_HOC_PHAN đã có từ Pipeline 1
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
CHUNK_SIZE = 100000
BATCH_SIZE = 50000

print("=" * 70)
print("📊 PIPELINE 2: SURVEY DATA")
print(f"   Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
print("=" * 70)

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

# ================= NLP =================
TAG_HOCPHAN = {'nội dung','chương trình','môn học','học phần','kiến thức','chuẩn đầu ra','tài liệu','giáo trình','thực hành','lý thuyết','phù hợp','bổ ích','cần thiết','cập nhật','thực tế'}
TAG_DAYHOC = {'giảng viên','thầy','cô','dạy','giảng','truyền đạt','hướng dẫn','nhiệt tình','tận tâm','dễ hiểu','sinh động','thú vị','hấp dẫn','chuyên nghiệp'}
TAG_KIEMTRA = {'kiểm tra','đánh giá','thi','đề thi','chấm điểm','công bằng','minh bạch','khách quan','nghiêm túc','chính xác'}
TAG_KHAC = {'cơ sở vật chất','phòng học','máy chiếu','wifi','hỗ trợ','góp ý','đề xuất','cải thiện'}
POS_KW = {'tốt','hay','hài lòng','thích','bổ ích','hiệu quả','chất lượng','tuyệt vời','xuất sắc','nhiệt tình','dễ hiểu','công bằng'}
NEG_KW = {'tệ','kém','chán','dở','không tốt','khó hiểu','nhàm chán','thiếu','hạn chế','thất vọng','cần cải thiện'}
NEU_KW = {'không có góp ý','không ý kiến','không có','bình thường'}

# ================= MASTER LOOKUP (từ DB) =================
_g_cn = {}          # MaChuyenNganh -> MaNganh
_g_valid_cn = set() # MaChuyenNganh hợp lệ
_g_valid_hp = set() # MaHP hợp lệ
_g_valid_gv = set() # MaGV hợp lệ
_g_valid_lop = set() # MaLop hợp lệ
_g_valid_hk = None   # MaHocKy hợp lệ

def load_master_from_db():
    """Load master data từ DB (đã có từ Pipeline 1)"""
    global _g_cn, _g_valid_cn, _g_valid_hp, _g_valid_gv, _g_valid_lop, _g_valid_hk
    
    print("\n📚 Load master từ DB...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Chuyên ngành
    cursor.execute("SELECT MaChuyenNganh, MaNganh FROM DIM_CHUYEN_NGANH")
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_cn[key] = str(row[1]).strip()
        _g_valid_cn.add(key)
    print(f"  -> Chuyên ngành: {len(_g_cn)}")
    
    # Học phần
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    _g_valid_hp.update(str(r[0]).strip() for r in cursor.fetchall())
    print(f"  -> Học phần: {len(_g_valid_hp)}")
    
    # Giảng viên
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    _g_valid_gv.update(str(r[0]).strip() for r in cursor.fetchall())
    print(f"  -> Giảng viên: {len(_g_valid_gv)}")
    
    # Lớp
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    _g_valid_lop.update(str(r[0]).strip() for r in cursor.fetchall())
    print(f"  -> Lớp: {len(_g_valid_lop)}")
    
    # Học kỳ
    mhk, _, _ = derive_ma_hoc_ky()
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", mhk)
    row = cursor.fetchone()
    if row:
        _g_valid_hk = str(row[0]).strip()
    
    conn.close()

# ================= BLOB =================
def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except: return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    fn = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    yc = int(fn[:-1]); hk = int(fn[-1])
    nbd = 2000 + (yc - 1); nkt = nbd + 1
    return f"HK{hk}_{nbd%100}{nkt%100}", f"{nbd}-{nkt}", hk

def normalize_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def determine_ma_chuyen_nganh(lop):
    """Xác định MaChuyenNganh từ Lop"""
    lop_upper = lop.upper().strip()
    lop_norm = normalize_lop(lop)
    
    if 'ACCA' in lop_upper:
        match = re.search(r'K(\d{2})', lop_upper)
        if match: return f"K{match.group(1)}-ACCA"
    
    if 'CTS' in lop_upper: return "CTS"
    if 'QT' in lop_upper: return "QT"
    
    match = re.search(r'K(\d{2})', lop_upper)
    if match: return f"K{match.group(1)}"
    
    return lop_norm if lop_norm else lop

def nlp_fast(text):
    if not text or len(text) < 5: return 0,0,0,0,'NEUTRAL',0
    words = set(text.lower().split())
    t1 = 1 if len(words & TAG_HOCPHAN) >= 2 else 0
    t2 = 1 if len(words & TAG_DAYHOC) >= 2 else 0
    t3 = 1 if len(words & TAG_KIEMTRA) >= 2 else 0
    t4 = 1 if len(words & TAG_KHAC) >= 2 else 0
    p = len(words & POS_KW); n = len(words & NEG_KW); e = len(words & NEU_KW)
    if 'không' in words: p = max(0, p-1); n += 1
    if p > n and p > e: s = 'POSITIVE'
    elif n > p and n > e: s = 'NEGATIVE'
    else: s = 'NEUTRAL'
    return t1, t2, t3, t4, s, 1 if len(text) > 10 else 0

# ================= PARSE =================
def parse_batch(args):
    lines, file_name = args
    results = []
    for line in lines:
        if not line: continue
        ni = line.find('NULL')
        left = line[:ni].rstrip(', \t') if ni >= 0 else line
        right = line[ni+4:].lstrip(', \t') if ni >= 0 else ''
        row = left.split(','); rl = len(row)
        if rl < 10: continue
        nsi = -1
        for i in range(2, min(12, rl)):
            if _DATE_RE(row[i].strip()): nsi = i; break
        if nsi == -1: continue
        mgi = -1
        for i in range(nsi+1, min(nsi+25, rl)):
            if _MA_GV_RE(row[i].strip()): mgi = i; break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        lop = row[0].strip(); ma_sv = row[1].strip(); ns = row[nsi].strip()
        np = [x.strip() for x in row[2:nsi]]
        ten = np[-1] if np else ''; hd = ' '.join(np[:-1]) if len(np) > 1 else ''
        ma_hp = row[nsi+1].strip() if nsi+1 < rl else ''
        thp_raw = ' '.join(x.strip() for x in row[nsi+2:mgi])
        ma_gv = row[mgi].strip() if mgi < rl else ''
        hdgv = row[mgi+1].strip() if mgi+1 < rl else ''
        tgv = row[mgi+2].strip() if mgi+2 < rl else ''
        lhp = row[mgi+3].strip() if mgi+3 < rl else ''
        ch = row[mgi+4].strip() if mgi+4 < rl else ''
        gt = row[mgi+5].strip() if mgi+5 < rl else ''
        essay = right.replace(' , ', ', ').strip()
        t1,t2,t3,t4,sent,valid = nlp_fast(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        
        ma_cn = determine_ma_chuyen_nganh(lop)
        ml = normalize_lop(lop)
        mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{file_name}"
        
        results.append([
            sid, ma_sv, hd, ten, ns, ml, lop, ma_cn,
            ma_hp, thp_raw, ma_gv, hdgv, tgv, mlhp, lhp,
            ch, gt, essay, t1, t2, t3, t4, sent, valid
        ])
    return results

def parse_survey(content):
    print(f"  -> Parsing..."); t0 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"  -> {len(lines):,} lines")
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches):
            all_results.extend(res)
    df = pd.DataFrame(all_results, columns=[
        'SubmissionID','MaSV','HoDem','Ten','NgaySinh',
        'MaLop','Lop','MaChuyenNganh',
        'MaHP','TenHP','MaGV','HoDemGV','TenGV','MaLopHP','LopHP',
        'CauHoi','GiaTri','EssayText',
        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac',
        'Sentiment','Is_Valid'
    ])
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= DATABASE LOAD =================
def insert_batch(cursor, table, columns, data, batch_size=BATCH_SIZE):
    if not data: return 0
    ph = ', '.join(['?'] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({ph})"
    inserted = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            cursor.executemany(sql, batch)
            cursor.connection.commit()
            inserted += len(batch)
        except:
            for d in batch:
                try:
                    cursor.execute(sql, d)
                    cursor.connection.commit()
                    inserted += 1
                except: pass
    return inserted

def load_dimensions(cursor, df):
    """Chỉ load các DIM cần thiết (không load Khoa, Ngành, Chuyên ngành, Học phần)"""
    print("\n--- DIMENSIONS ---")
    t0 = time.time()
    mhk, nh, hk = derive_ma_hoc_ky()
    total = 0
    
    # DIM_HOC_KY
    data_hk = [(mhk, nh, hk)]
    c = insert_batch(cursor, 'DIM_HOC_KY', ['MaHocKy','NamHoc','HocKy'], data_hk)
    print(f"  DIM_HOC_KY: {c} rows")
    total += c
    
    # DIM_LOP_SINH_VIEN
    df_lop = df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop').fillna('')
    # Chỉ lấy MaChuyenNganh có trong DB
    data_lop = [(str(r['MaLop'])[:20], str(r['Lop'])[:50], str(r['MaChuyenNganh'])[:20])
                for _, r in df_lop.iterrows()
                if str(r['MaLop']).strip() and str(r['MaChuyenNganh']).strip() in _g_valid_cn]
    c = insert_batch(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop','Lop','MaChuyenNganh'], data_lop)
    print(f"  DIM_LOP_SINH_VIEN: {c} rows ({time.time()-t0:.1f}s)")
    total += c
    
    # DIM_SINH_VIEN
    df_sv = df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV').fillna('')
    data_sv = []
    for _, r in df_sv.iterrows():
        if not str(r['MaSV']).strip(): continue
        if str(r['MaLop']).strip() not in _g_valid_lop: continue
        try: ns = pd.to_datetime(r['NgaySinh'], format='%d/%m/%Y').strftime('%Y-%m-%d')
        except: ns = None
        data_sv.append((str(r['MaSV'])[:20], str(r['HoDem'])[:100], str(r['Ten'])[:50], ns, str(r['MaLop'])[:20]))
    c = insert_batch(cursor, 'DIM_SINH_VIEN', ['MaSV','HoDem','Ten','NgaySinh','MaLop'], data_sv)
    print(f"  DIM_SINH_VIEN: {c} rows ({time.time()-t0:.1f}s)")
    total += c
    
    # DIM_GIANG_VIEN
    df_gv = df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV').fillna('')
    data_gv = [(str(r['MaGV'])[:20], str(r['HoDemGV'])[:100], str(r['TenGV'])[:50])
               for _, r in df_gv.iterrows() if str(r['MaGV']).strip()]
    c = insert_batch(cursor, 'DIM_GIANG_VIEN', ['MaGV','HoDemGV','TenGV'], data_gv)
    print(f"  DIM_GIANG_VIEN: {c} rows ({time.time()-t0:.1f}s)")
    total += c
    
    # DIM_LOP_HOC_PHAN
    df_lhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    data_lhp = [(str(r['MaLopHP'])[:50], str(r['LopHP'])[:100], str(r['MaHP'])[:20], str(r['MaGV'])[:20], mhk)
                for _, r in df_lhp.iterrows()
                if str(r['MaLopHP']).strip() and str(r['MaHP']).strip() in _g_valid_hp]
    c = insert_batch(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], data_lhp)
    print(f"  DIM_LOP_HOC_PHAN: {c} rows ({time.time()-t0:.1f}s)")
    total += c
    
    print(f"  📊 Total dimensions: {total} ({time.time()-t0:.1f}s)")
    return total

def load_facts(cursor, df):
    """Load FACT tables"""
    print("\n--- FACTS ---")
    t0 = time.time()
    
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    # FACT_GOP_Y
    if not df_essay.empty:
        t1 = time.time()
        data_gy = []
        for _, r in df_essay.iterrows():
            data_gy.append((
                str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                str(r['EssayText'])[:4000] if pd.notna(r['EssayText']) else '',
                str(r['Sentiment'])[:20] if pd.notna(r['Sentiment']) else 'NEUTRAL',
                int(r['Is_Valid']) if pd.notna(r['Is_Valid']) else 0,
                int(r['Tag_HocPhan']) if pd.notna(r['Tag_HocPhan']) else 0,
                int(r['Tag_DayHoc']) if pd.notna(r['Tag_DayHoc']) else 0,
                int(r['Tag_KiemTra']) if pd.notna(r['Tag_KiemTra']) else 0,
                int(r['Tag_Khac']) if pd.notna(r['Tag_Khac']) else 0
            ))
        c1 = insert_batch(cursor, 'FACT_GOP_Y_TU_LUAN',
                         ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                          'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac'], data_gy)
        print(f"  FACT_GOP_Y: {c1:,} rows ({time.time()-t1:.1f}s)")
    else:
        c1 = 0
    
    # FACT_KET_QUA
    t1 = time.time()
    data_kq = []
    for _, r in df[(df['CauHoi']!='') & (df['GiaTri']!='')].iterrows():
        try:
            mc = int(float(r['CauHoi'])); d = int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: data_kq.append((str(r['SubmissionID'])[:150], mc, d))
        except: pass
    for _, r in df_essay.iterrows():
        s = r['Sentiment']; d = 5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: data_kq.append((str(r['SubmissionID'])[:150], mc, d))
    
    if data_kq:
        c2 = insert_batch(cursor, 'FACT_KET_QUA_DANH_GIA', ['SubmissionID','MaCauHoi','Diem'], data_kq)
        print(f"  FACT_KET_QUA: {c2:,} rows ({time.time()-t1:.1f}s)")
    else:
        c2 = 0
    
    print(f"  📊 Facts: {c1+c2:,} ({time.time()-t0:.1f}s)")
    return c1 + c2

def load_to_database(df):
    print("\n💾 LOAD TO DATABASE")
    t0 = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Tắt constraint
        for t in ['DIM_SINH_VIEN','DIM_LOP_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
                   'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cursor.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL"); conn.commit()
            except: pass
        
        load_dimensions(cursor, df)
        load_facts(cursor, df)
        
        # Bật constraint
        for t in ['DIM_SINH_VIEN','DIM_LOP_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
                   'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cursor.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL"); conn.commit()
            except: pass
    finally:
        conn.close()
    
    print(f"  ✅ Total load: {time.time()-t0:.1f}s")

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Kết nối & Load Master...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content: print("❌ No data!"); sys.exit(1)
    
    print("\n📝 PARSE + NLP")
    t1 = time.time()
    df = parse_survey(content)
    print(f"  ✅ Parse: {time.time()-t1:.1f}s")
    
    if df.empty: print("❌ No data!"); sys.exit(1)
    
    df.to_parquet(f"/tmp/{FILE_NAME}.parquet", index=False)
    
    load_to_database(df)
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
