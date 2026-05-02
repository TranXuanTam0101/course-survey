#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - FIXED THREADING
- Mỗi thread có connection riêng
- Không dùng MARS
- Tối ưu thời gian load
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
from concurrent.futures import ThreadPoolExecutor, as_completed

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
print("📊 PIPELINE 2: SURVEY DATA (THREADING FIXED)")
print(f"   Workers: {NUM_WORKERS} | Batch: {BATCH_SIZE:,}")
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

# ================= MASTER DATA =================
_g_cn = {}; _g_hp = {}; _g_khoa_hp = {}
_g_default_khoa = ('KHOA01', 'Trường Đại học Kinh tế')
_g_default_nganh = 'KHOA01NG01'

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp, _g_default_khoa, _g_default_nganh
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    kl = [(str(r[0]), str(r[1])) for r in cursor.fetchall()]
    if kl: _g_default_khoa = kl[0]
    cursor.execute("SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, n.TenNganh, n.MaKhoa, k.TenKhoa FROM DIM_CHUYEN_NGANH cn JOIN DIM_NGANH n ON cn.MaNganh=n.MaNganh JOIN DIM_KHOA k ON n.MaKhoa=k.MaKhoa")
    for r in cursor.fetchall():
        k = str(r[0]).strip()
        _g_cn[k] = (k, str(r[1]).strip(), str(r[2]).strip(), str(r[3]).strip(), str(r[4]).strip(), str(r[5]).strip())
    if _g_cn: _g_default_nganh = list(_g_cn.values())[0][2]
    cursor.execute("SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa FROM DIM_HOC_PHAN hp JOIN DIM_KHOA k ON hp.MaKhoa=k.MaKhoa")
    for r in cursor.fetchall():
        k = str(r[0]).strip()
        _g_hp[k] = str(r[1]).strip()
        _g_khoa_hp[k] = (str(r[2]).strip(), str(r[3]).strip())
    conn.close()
    print(f"  -> CN={len(_g_cn)}, HP={len(_g_hp)}")

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
        lop_norm = normalize_lop(lop)
        ma_cn_key = f"K{lop_norm[3:5]}" if _LOP_RE(lop_norm) else (lop_norm or lop)
        cn = _g_cn.get(ma_cn_key, (ma_cn_key, f'CN {ma_cn_key}', _g_default_nganh, 'Ngành mặc định', _g_default_khoa[0], _g_default_khoa[1]))
        thp, mkhp, tkhp = _g_hp.get(ma_hp, ''), *_g_khoa_hp.get(ma_hp, _g_default_khoa)
        thp = thp or thp_raw
        ml = lop_norm; mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{file_name}"
        results.append([sid, ma_sv, hd, ten, ns, ml, lop, cn[0], cn[1], cn[2], cn[3], cn[4], cn[5],
                       ma_hp, thp, mkhp, tkhp, ma_gv, hdgv, tgv, mlhp, lhp, ch, gt, essay,
                       t1, t2, t3, t4, sent, valid])
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
        'SubmissionID','MaSV','HoDem','Ten','NgaySinh','MaLop','Lop','MaChuyenNganh','TenChuyenNganh',
        'MaNganh','TenNganh','MaKhoa_CN','TenKhoa_CN','MaHP','TenHP','MaKhoa_HP','TenKhoa_HP',
        'MaGV','HoDemGV','TenGV','MaLopHP','LopHP','CauHoi','GiaTri','EssayText',
        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid'
    ])
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= DATABASE LOAD - SINGLE CONNECTION FAST =================
def insert_table(cursor, table, columns, data, batch_size=None):
    """Insert nhanh - 1 connection, không threading"""
    if not data: return 0
    if batch_size is None: batch_size = BATCH_SIZE
    
    ph = ', '.join(['?'] * len(columns))
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({ph})"
    
    # Tắt constraint tạm cho bảng này
    try:
        cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
        cursor.connection.commit()
    except: pass
    
    inserted = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            cursor.executemany(sql, batch)
            cursor.connection.commit()
            inserted += len(batch)
        except Exception as e:
            # Fallback: insert từng dòng cho batch lỗi
            for d in batch:
                try:
                    cursor.execute(sql, d)
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
            print(f"    ⚠️ Batch {i//batch_size} partial: {inserted}")
    
    # Bật lại constraint
    try:
        cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
        cursor.connection.commit()
    except: pass
    
    return inserted

def load_dimensions(cursor, df):
    """Load dimensions - tuần tự nhanh"""
    print("\n--- DIMENSIONS ---")
    t0 = time.time()
    mhk, nh, hk = derive_ma_hoc_ky()
    total = 0
    
    tasks = []
    
    # DIM_LOP_SINH_VIEN
    df_lop = df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop').fillna('')
    tasks.append(('DIM_LOP_SINH_VIEN', ['MaLop','Lop','MaChuyenNganh'],
                  [(str(r['MaLop'])[:20], str(r['Lop'])[:50], str(r['MaChuyenNganh'])[:20]) for _, r in df_lop.iterrows()]))
    
    # DIM_SINH_VIEN
    df_sv = df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV').fillna('')
    data_sv = []
    for _, r in df_sv.iterrows():
        try: ns = pd.to_datetime(r['NgaySinh'], format='%d/%m/%Y').strftime('%Y-%m-%d')
        except: ns = None
        data_sv.append((str(r['MaSV'])[:20], str(r['HoDem'])[:100], str(r['Ten'])[:50], ns, str(r['MaLop'])[:20]))
    tasks.append(('DIM_SINH_VIEN', ['MaSV','HoDem','Ten','NgaySinh','MaLop'], data_sv))
    
    # DIM_GIANG_VIEN
    df_gv = df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV').fillna('')
    tasks.append(('DIM_GIANG_VIEN', ['MaGV','HoDemGV','TenGV'],
                  [(str(r['MaGV'])[:20], str(r['HoDemGV'])[:100], str(r['TenGV'])[:50]) for _, r in df_gv.iterrows()]))
    
    # DIM_HOC_PHAN
    df_hp = df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}).drop_duplicates('MaHP').fillna('')
    tasks.append(('DIM_HOC_PHAN', ['MaHP','TenHP','MaKhoa'],
                  [(str(r['MaHP'])[:20], str(r['TenHP'])[:200], str(r['MaKhoa'])[:20]) for _, r in df_hp.iterrows()]))
    
    # DIM_HOC_KY
    tasks.append(('DIM_HOC_KY', ['MaHocKy','NamHoc','HocKy'], [(mhk, nh, hk)]))
    
    # DIM_LOP_HOC_PHAN
    df_lhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    tasks.append(('DIM_LOP_HOC_PHAN', ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'],
                  [(str(r['MaLopHP'])[:50], str(r['LopHP'])[:100], str(r['MaHP'])[:20], str(r['MaGV'])[:20], mhk) for _, r in df_lhp.iterrows()]))
    
    # Chạy tuần tự từng bảng
    for table, cols, data in tasks:
        t1 = time.time()
        c = insert_table(cursor, table, cols, data)
        total += c
        print(f"  {table}: {c:,} rows ({time.time()-t1:.1f}s)")
    
    print(f"  📊 Total: {total:,} ({time.time()-t0:.1f}s)")
    return total

def load_facts(cursor, df):
    """Load FACT tables"""
    print("\n--- FACTS ---")
    t0 = time.time()
    
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    # FACT_GOP_Y_TU_LUAN
    t1 = time.time()
    if not df_essay.empty:
        data_gy = []
        for _, r in df_essay.iterrows():
            data_gy.append((
                str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                str(r['EssayText']) if pd.notna(r['EssayText']) else '',
                str(r['Sentiment'])[:20] if pd.notna(r['Sentiment']) else 'NEUTRAL',
                int(r['Is_Valid']) if pd.notna(r['Is_Valid']) else 0,
                int(r['Tag_HocPhan']) if pd.notna(r['Tag_HocPhan']) else 0,
                int(r['Tag_DayHoc']) if pd.notna(r['Tag_DayHoc']) else 0,
                int(r['Tag_KiemTra']) if pd.notna(r['Tag_KiemTra']) else 0,
                int(r['Tag_Khac']) if pd.notna(r['Tag_Khac']) else 0
            ))
        c = insert_table(cursor, 'FACT_GOP_Y_TU_LUAN',
                        ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                         'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac'], data_gy)
        print(f"  FACT_GOP_Y: {c:,} rows ({time.time()-t1:.1f}s)")
    
    # FACT_KET_QUA_DANH_GIA
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
        c = insert_table(cursor, 'FACT_KET_QUA_DANH_GIA', ['SubmissionID','MaCauHoi','Diem'], data_kq)
        print(f"  FACT_KET_QUA: {c:,} rows ({time.time()-t1:.1f}s)")
    
    print(f"  📊 Facts done ({time.time()-t0:.1f}s)")

def load_to_database(df):
    """Load toàn bộ vào database - 1 connection, tuần tự"""
    print("\n💾 LOAD TO DATABASE")
    t0 = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Tắt tất cả constraint 1 lần
        print("  -> Disabling constraints...")
        for table in ['DIM_SINH_VIEN','DIM_LOP_SINH_VIEN','DIM_LOP_HOC_PHAN',
                       'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try:
                cursor.execute(f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL")
            except: pass
        conn.commit()
        
        load_dimensions(cursor, df)
        load_facts(cursor, df)
        
        # Bật lại constraint
        print("\n  -> Enabling constraints...")
        for table in ['DIM_SINH_VIEN','DIM_LOP_SINH_VIEN','DIM_LOP_HOC_PHAN',
                       'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try:
                cursor.execute(f"ALTER TABLE {table} CHECK CONSTRAINT ALL")
            except: pass
        conn.commit()
        
    finally:
        conn.close()
    
    print(f"  ✅ Total load: {time.time()-t0:.1f}s")

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
    
    df.to_parquet(f"/tmp/{FILE_NAME}.parquet", index=False)
    
    load_to_database(df)
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
