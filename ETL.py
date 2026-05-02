#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - FINAL STABLE VERSION
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

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"

NUM_WORKERS = 4
CHUNK_SIZE = 20000

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

# Master cache
_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        
        # Load Chuyên ngành (theo schema bạn cung cấp)
        cursor.execute("""
            SELECT MaChuyenNganh, TenChuyenNganh, MaNganh 
            FROM DIM_CHUYEN_NGANH
        """)
        for r in cursor.fetchall():
            key = str(r[0]).strip()
            _g_cn[key] = (key, str(r[1]).strip(), str(r[2]).strip())
        
        # Load Học phần
        cursor.execute("SELECT MaHP, TenHP, MaKhoa FROM DIM_HOC_PHAN")
        for r in cursor.fetchall():
            hp = str(r[0]).strip()
            _g_hp[hp] = str(r[1]).strip()
            _g_khoa_hp[hp] = str(r[2]).strip()
        
        conn.close()
        print(f"✅ Master loaded: {len(_g_cn)} Chuyên ngành, {len(_g_hp)} Học phần")
    except Exception as e:
        print(f"⚠️ Load master error: {e}")
        print("→ Sẽ dùng giá trị mặc định")

def nlp_fast(text):
    if not text or len(text) < 5:
        return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(k in t for k in ('nội dung','chương trình','học phần','chuẩn')) else 0
    t2 = 1 if any(k in t for k in ('giảng viên','thầy','cô','nhiệt tình','tận tâm')) else 0
    t3 = 1 if any(k in t for k in ('kiểm tra','thi','đánh giá','chấm')) else 0
    t4 = 1 if any(k in t for k in ('cơ sở','phòng','cải thiện','góp ý')) else 0
    sent = 'POSITIVE' if t.count('tốt') + t.count('hay') > 1 else 'NEGATIVE' if any(x in t for x in ('kém','tệ','khó')) else 'NEUTRAL'
    return t1, t2, t3, t4, sent, 1

def parse_batch(args):
    lines, file_name = args
    results = []
    
    for line in lines:
        if not line: continue
        line = line.strip()
        ni = line.find('NULL')
        left = line[:ni].rstrip(', ') if ni >= 0 else line
        right = line[ni+4:].lstrip(', ') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',') if x.strip()]
        if len(row) < 8: continue
            
        nsi = next((i for i in range(2, min(12, len(row))) if _DATE_RE(row[i])), -1)
        if nsi == -1: continue
            
        mgi = next((i for i in range(nsi+1, min(nsi+25, len(row))) if _MA_GV_RE(row[i])), -1)
        if mgi == -1: mgi = min(len(row)-1, nsi+6)
        
        ma_sv = row[1] if len(row) > 1 else ''
        lop = row[0]
        ma_hp = row[nsi+1] if nsi+1 < len(row) else ''
        ma_gv = row[mgi] if mgi < len(row) else ''
        lhp = row[mgi+3] if mgi+3 < len(row) else f"{ma_hp}_{ma_gv}"
        
        essay = right.replace(' , ', ', ').strip()
        t1,t2,t3,t4,sent,valid = nlp_fast(essay)
        
        sid = f"{ma_sv}_{lhp}_{ma_gv}_{file_name}"
        
        results.append({
            'SubmissionID': sid,
            'MaSV': ma_sv,
            'Lop': lop,
            'MaHP': ma_hp,
            'MaGV': ma_gv,
            'MaLopHP': lhp,
            'EssayText': essay,
            'Tag_HocPhan': t1,
            'Tag_DayHoc': t2,
            'Tag_KiemTra': t3,
            'Tag_Khac': t4,
            'Sentiment': sent,
            'Is_Valid': valid
        })
    
    return results

def main():
    t0 = time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    # Download file
    content = ""
    try:
        client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
        content = client.download_blob().readall().decode('utf-8-sig')
        print(f"Downloaded: {len(content):,} characters")
    except Exception as e:
        print(f"Download error: {e}")
        sys.exit(1)
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"Total lines: {len(lines):,}")
    
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches):
            all_results.extend(res)
    
    df = pd.DataFrame(all_results)
    print(f"✅ Final DataFrame: {len(df):,} rows")
    
    if not df.empty:
        print("\nSample data:")
        print(df.head(3))
    
    print(f"\nTotal time: {time.time()-t0:.1f}s")
    # TODO: Gọi load_to_database(df) khi bạn muốn load

if __name__ == "__main__":
    main()
