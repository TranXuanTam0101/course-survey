#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - DEBUG & FIXED VERSION
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
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"

NUM_WORKERS = 4
CHUNK_SIZE = 15000

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp
    # ... giữ nguyên hàm load master
    print(f"✅ Master loaded: CN={len(_g_cn)}, HP={len(_g_hp)}")

def nlp_fast(text):
    if not text or len(text) < 5:
        return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(x in t for x in ('nội dung','chương trình','học phần')) else 0
    t2 = 1 if any(x in t for x in ('giảng viên','thầy','cô','nhiệt tình')) else 0
    t3 = 1 if any(x in t for x in ('kiểm tra','thi','đánh giá')) else 0
    t4 = 1 if any(x in t for x in ('cơ sở','cải thiện','góp ý')) else 0
    sent = 'NEUTRAL'
    return t1, t2, t3, t4, sent, 1

# ================= PARSE WITH DEBUG =================
def parse_batch(args):
    lines, file_name = args
    results = []
    debug_count = 0
    
    for line in lines:
        if not line: continue
        line = line.strip()
        debug_count += 1
        if debug_count <= 5:  # In 5 dòng đầu để debug
            print(f"DEBUG LINE: {line[:150]}...")
        
        ni = line.find('NULL')
        left = line[:ni].rstrip(', ') if ni >= 0 else line
        right = line[ni+4:].lstrip(', ') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',') if x.strip()]
        rl = len(row)
        
        if rl < 8:
            continue
            
        # Tìm ngày sinh
        nsi = next((i for i in range(2, min(12, rl)) if _DATE_RE(row[i])), -1)
        if nsi == -1:
            continue
            
        # Tìm MaGV
        mgi = next((i for i in range(nsi+1, min(nsi+25, rl)) if _MA_GV_RE(row[i])), -1)
        if mgi == -1:
            mgi = min(rl-1, nsi+6)
        
        essay = right.replace(' , ', ', ').strip()
        t1,t2,t3,t4,sent,valid = nlp_fast(essay)
        
        ma_sv = row[1] if rl > 1 else ''
        lop = row[0] if rl > 0 else ''
        ma_gv = row[mgi] if mgi < rl else ''
        ma_hp = row[nsi+1] if nsi+1 < rl else ''
        lhp = row[mgi+3] if mgi+3 < rl else ''
        
        sid = f"{ma_sv}_{lhp or 'NA'}_{ma_gv}_{file_name}"
        
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
    
    print(f"Batch processed: {len(results)} rows")
    return results

# ================= MAIN =================
def main():
    t0 = time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    content = ""
    try:
        client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
        if client.exists():
            content = client.download_blob().readall().decode('utf-8-sig')
            print(f"Downloaded: {len(content):,} characters")
    except Exception as e:
        print(f"Download error: {e}")
    
    if not content:
        print("❌ No content!")
        sys.exit(1)
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"Total lines: {len(lines):,}")
    
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            if (i + 1) % 10 == 0:
                print(f"Processed {i+1}/{len(batches)} batches | Total rows: {len(all_results):,}")
    
    df = pd.DataFrame(all_results)
    print(f"Final DataFrame: {len(df):,} rows")
    
    if df.empty:
        print("❌ DataFrame rỗng! Kiểm tra logic parse.")
        print("5 dòng đầu file:", lines[:5])
    else:
        print(df.head(3))
        # load_to_database(df)   # Bật khi parse ổn
    
    print(f"Total time: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
