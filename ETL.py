#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - ULTRA FAST VERSION
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
    f"Connection Timeout=600;Command Timeout=1800;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"

NUM_WORKERS = 4
CHUNK_SIZE = 25000

print("="*90)
print("🚀 SURVEY ETL - ULTRA FAST PIPELINE")
print(f"Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,}")
print("="*90)

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_LOP_RE = re.compile(r'^\d{2}K\d{2}$').match

_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.execute("SELECT MaChuyenNganh, TenChuyenNganh, MaNganh FROM DIM_CHUYEN_NGANH")
        for r in cursor.fetchall():
            key = str(r[0]).strip()
            _g_cn[key] = tuple(str(x).strip() for x in r)
        
        cursor.execute("SELECT MaHP, TenHP, MaKhoa FROM DIM_HOC_PHAN")
        for r in cursor.fetchall():
            hp = str(r[0]).strip()
            _g_hp[hp] = str(r[1]).strip()
            _g_khoa_hp[hp] = str(r[2]).strip()
        
        conn.close()
        print(f"✅ Master loaded: {len(_g_cn)} CN | {len(_g_hp)} HP")
    except Exception as e:
        print(f"⚠️ Master load error: {e}")

def nlp_fast(text):
    if not text or len(text) < 5:
        return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(k in t for k in ('nội dung','chương trình','học phần','chuẩn')) else 0
    t2 = 1 if any(k in t for k in ('giảng viên','thầy','cô','nhiệt tình','tận tâm')) else 0
    t3 = 1 if any(k in t for k in ('kiểm tra','thi','đánh giá','chấm')) else 0
    t4 = 1 if any(k in t for k in ('cơ sở','cải thiện','góp ý')) else 0
    sent = 'POSITIVE' if 'tốt' in t or 'hay' in t else 'NEGATIVE' if any(x in t for x in ('kém','tệ','không')) else 'NEUTRAL'
    return t1,t2,t3,t4,sent,1

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

def load_to_database(df):
    """LOAD NHANH NHẤT CÓ THỂ"""
    if df.empty:
        print("❌ No data to load!")
        return
    
    print("\n💾 LOADING TO DATABASE...")
    t0 = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    try:
        # Tắt constraint
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        conn.commit()
        
        df_essay = df[df['EssayText'].str.strip() != ''].copy()
        
        load_df = pd.DataFrame({
            'SubmissionID': df_essay['SubmissionID'].astype(str).str[:150],
            'MaSV': df_essay['MaSV'].astype(str).str[:20],
            'MaLopHP': df_essay['MaLopHP'].astype(str).str[:50],
            'NoiDungGopY': df_essay['EssayText'].astype(str).str[:4000],
            'Sentiment': df_essay['Sentiment'].astype(str),
            'Is_Valid': df_essay['Is_Valid'].astype(int),
            'Tag_HocPhan': df_essay['Tag_HocPhan'].astype(int),
            'Tag_DayHoc': df_essay['Tag_DayHoc'].astype(int),
            'Tag_KiemTra': df_essay['Tag_KiemTra'].astype(int),
            'Tag_Khac': df_essay['Tag_Khac'].astype(int)
        })
        
        print(f" → Inserting {len(load_df):,} rows...")
        
        # Load nhanh
        load_df.to_sql(
            name='FACT_GOP_Y_TU_LUAN',
            con=conn,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=15000
        )
        
        # Bật lại constraint
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        conn.commit()
        
        print(f"✅ LOAD HOÀN TẤT: {len(load_df):,} rows")
        
    except Exception as e:
        print(f"❌ Load error: {e}")
        conn.rollback()
    finally:
        conn.close()
    
    print(f"⏱️ Load time: {time.time()-t0:.1f}s")

def main():
    t0 = time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master_from_db()
    
    # Download
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    print(f"Downloaded: {len(content):,} characters")
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"Total lines: {len(lines):,}")
    
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches):
            all_results.extend(res)
    
    df = pd.DataFrame(all_results)
    print(f"✅ Parsed: {len(df):,} rows")
    
    if not df.empty:
        load_to_database(df)
    
    print(f"\n🎉 HOÀN THÀNH! Total time: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
