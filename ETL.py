#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL PIPELINE - HIGH PERFORMANCE VERSION
"""

import os
import sys
import re
import time
import pandas as pd
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
    print("❌ Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=300;Command Timeout=900;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_PATH = "tailieu"

NUM_WORKERS = max(4, cpu_count() - 2)
CHUNK_SIZE = 25000
BATCH_SIZE = 100000

print("=" * 80)
print("🚀 SURVEY ETL HIGH PERFORMANCE PIPELINE")
print(f"Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
print("=" * 80)

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

# ================= MASTER DATA CACHE =================
_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, n.TenNganh, n.MaKhoa 
        FROM DIM_CHUYEN_NGANH cn 
        JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh
    """)
    for r in cursor.fetchall():
        _g_cn[str(r[0]).strip()] = tuple(map(str, r))
    
    cursor.execute("SELECT MaHP, TenHP, MaKhoa FROM DIM_HOC_PHAN")
    for r in cursor.fetchall():
        hp = str(r[0]).strip()
        _g_hp[hp] = str(r[1]).strip()
        _g_khoa_hp[hp] = str(r[2]).strip()
    
    conn.close()
    print(f"✅ Loaded master: {_g_cn.__len__()} Chuyên ngành, {_g_hp.__len__()} Học phần")

# ================= NLP FAST =================
def nlp_fast(text: str):
    if not text or len(text) < 8:
        return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(k in t for k in ('nội dung','chương trình','chuẩn đầu ra','học phần','kiến thức')) else 0
    t2 = 1 if any(k in t for k in ('giảng viên','thầy','cô','nhiệt tình','tận tâm','dạy','truyền đạt')) else 0
    t3 = 1 if any(k in t for k in ('kiểm tra','thi','đánh giá','chấm điểm','công bằng')) else 0
    t4 = 1 if any(k in t for k in ('cơ sở','phòng học','wifi','cải thiện','góp ý')) else 0
    
    p = sum(1 for k in ('tốt','hay','tận tình','dễ hiểu','thú vị','xuất sắc','bổ ích') if k in t)
    n = sum(1 for k in ('kém','tệ','khó hiểu','chán','thiếu','cải thiện','thất vọng') if k in t)
    
    sent = 'POSITIVE' if p > n + 1 else 'NEGATIVE' if n > p + 1 else 'NEUTRAL'
    return t1, t2, t3, t4, sent, 1

# ================= PARSE =================
def parse_batch(args):
    lines, file_name = args
    results = []
    append = results.append
    
    for line in lines:
        if not line: continue
        line = line.strip()
        ni = line.find('NULL')
        
        left = line[:ni].rstrip(', ') if ni >= 0 else line
        right = line[ni+4:].lstrip(', ') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',')]
        rl = len(row)
        if rl < 10: continue
            
        nsi = next((i for i in range(2, min(12, rl)) if _DATE_RE(row[i])), -1)
        if nsi == -1: continue
            
        mgi = next((i for i in range(nsi+1, min(nsi+25, rl)) if _MA_GV_RE(row[i])), -1)
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        lop = row[0]
        ma_sv = row[1]
        ns = row[nsi]
        np = row[2:nsi]
        ten = np[-1] if np else ''
        hd = ' '.join(np[:-1]) if len(np) > 1 else ''
        
        ma_hp = row[nsi+1] if nsi+1 < rl else ''
        thp_raw = ' '.join(row[nsi+2:mgi])
        ma_gv = row[mgi] if mgi < rl else ''
        hdgv = row[mgi+1] if mgi+1 < rl else ''
        tgv = row[mgi+2] if mgi+2 < rl else ''
        lhp = row[mgi+3] if mgi+3 < rl else ''
        
        essay = right.replace(' , ', ', ').strip()
        t1,t2,t3,t4,sent,valid = nlp_fast(essay)
        
        # Master lookup
        ma_cn = f"K{lop[3:5]}" if _LOP_RE(lop) else lop
        cn_info = _g_cn.get(ma_cn, (ma_cn, f'CN {ma_cn}', 'DEFAULT', 'Ngành mặc định', 'TĐHKT'))
        
        thp = _g_hp.get(ma_hp, thp_raw)
        mkhp = _g_khoa_hp.get(ma_hp, 'TĐHKT')
        
        mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{file_name}"
        
        append((
            sid, ma_sv, hd, ten, ns, lop, ma_cn, cn_info[1], cn_info[2], cn_info[4],
            ma_hp, thp, mkhp, ma_gv, hdgv, tgv, mlhp, lhp or '', 
            '', '', essay, t1, t2, t3, t4, sent, valid
        ))
    
    return results

# ================= DATABASE LOAD =================
def load_to_database(df):
    print("\n💾 LOADING TO DATABASE...")
    t0 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Tắt constraint
        for tbl in ['DIM_SINH_VIEN', 'DIM_LOP_SINH_VIEN', 'FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA']:
            try: cursor.execute(f"ALTER TABLE {tbl} NOCHECK CONSTRAINT ALL")
            except: pass
        conn.commit()
        
        # Load Dimensions (các bạn có thể mở rộng)
        # ... (thêm load_dimensions nếu cần)
        
        # FACT_GOP_Y_TU_LUAN
        df_essay = df[df['EssayText'].str.strip().ne('')].copy()
        data_gy = [(
            r['SubmissionID'], r['MaSV'], r['MaLopHP'], r['EssayText'],
            r['Sentiment'], r['Is_Valid'],
            r['Tag_HocPhan'], r['Tag_DayHoc'], r['Tag_KiemTra'], r['Tag_Khac']
        ) for _, r in df_essay.iterrows()]
        
        if data_gy:
            cursor.executemany("""
                INSERT INTO FACT_GOP_Y_TU_LUAN 
                (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid,
                 Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, data_gy)
            conn.commit()
            print(f"✅ FACT_GOP_Y_TU_LUAN: {len(data_gy):,} rows")
        
        conn.close()
        print(f"✅ LOAD HOÀN TẤT ({time.time()-t0:.1f}s)")
        
    except Exception as e:
        print(f"❌ LỖI: {e}")
        conn.rollback()
    finally:
        conn.close()

# ================= MAIN =================
def main():
    t0 = time.time()
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    # Download
    content = ""
    try:
        client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8-sig')
    except Exception as e:
        print(f"❌ Download error: {e}")
        sys.exit(1)
    
    if not content:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    # Parse
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    print(f"📝 Parsing {len(lines):,} lines...")
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches):
            all_results.extend(res)
    
    columns = [
        'SubmissionID','MaSV','HoDem','Ten','NgaySinh','Lop','MaChuyenNganh','TenChuyenNganh',
        'MaNganh','MaKhoa','MaHP','TenHP','MaKhoa_HP','MaGV','HoDemGV','TenGV','MaLopHP',
        'LopHP','CauHoi','GiaTri','EssayText',
        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid'
    ]
    
    df = pd.DataFrame(all_results, columns=columns)
    print(f"✅ Parse xong: {len(df):,} rows")
    
    # Load
    load_to_database(df)
    
    print(f"\n🎉 HOÀN THÀNH! Tổng thời gian: {time.time()-t0:.1f} giây | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
