#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - FIXED LOAD
- Dùng executemany thay vì to_sql
- Load siêu nhanh vào FACT_GOP_Y_TU_LUAN
"""

import os
import sys
import re
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
BATCH_SIZE = 50000

print("="*90)
print("🚀 SURVEY ETL - FIXED LOAD")
print(f"Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
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
    """LOAD DÙNG executemany - siêu nhanh"""
    if df.empty:
        print("❌ No data to load!")
        return
    
    print("\n💾 LOADING TO DATABASE...")
    t0 = time.time()
    
    # Lọc dòng có EssayText
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'].astype(str).str.strip() != '')].copy()
    
    if df_essay.empty:
        print("❌ No essay data!")
        return
    
    print(f" → Preparing {len(df_essay):,} rows...")
    
    # Chuẩn bị data dạng list of tuples
    data = []
    for _, r in df_essay.iterrows():
        data.append((
            str(r['SubmissionID'])[:150],
            str(r['MaSV'])[:20],
            str(r['MaLopHP'])[:50],
            str(r['EssayText'])[:4000] if pd.notna(r['EssayText']) else '',
            str(r['Sentiment'])[:20] if pd.notna(r['Sentiment']) else 'NEUTRAL',
            int(r['Is_Valid']) if pd.notna(r['Is_Valid']) else 0,
            int(r['Tag_HocPhan']) if pd.notna(r['Tag_HocPhan']) else 0,
            int(r['Tag_DayHoc']) if pd.notna(r['Tag_DayHoc']) else 0,
            int(r['Tag_KiemTra']) if pd.notna(r['Tag_KiemTra']) else 0,
            int(r['Tag_Khac']) if pd.notna(r['Tag_Khac']) else 0
        ))
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # Tắt constraint
        try:
            cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
            conn.commit()
        except: pass
        
        # Insert
        sql = """INSERT INTO FACT_GOP_Y_TU_LUAN 
                 (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid,
                  Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        
        inserted = 0
        total_batches = (len(data) + BATCH_SIZE - 1) // BATCH_SIZE
        
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            try:
                cursor.executemany(sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f" → Batch {batch_num}/{total_batches}: {len(batch):,} rows")
            except Exception as e:
                print(f" ⚠️ Batch {batch_num} error: {str(e)[:100]}")
                # Fallback: insert từng dòng
                for d in batch:
                    try:
                        cursor.execute(sql, d)
                        conn.commit()
                        inserted += 1
                    except:
                        pass
        
        # Bật lại constraint
        try:
            cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
            conn.commit()
        except: pass
        
        print(f" ✅ LOAD HOÀN TẤT: {inserted:,} rows")
        
    except Exception as e:
        print(f" ❌ Load error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
    
    print(f" ⏱️ Load time: {time.time()-t0:.1f}s")

def main():
    t0 = time.time()
    
    # Kết nối Azure
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Load master từ DB
    load_master_from_db()
    
    # Download file
    print(f"\n📥 Downloading {SURVEY_FILE}...")
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    print(f"   Downloaded: {len(content):,} characters")
    
    # Parse
    print(f"\n📝 Parsing...")
    t1 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"   Total lines: {len(lines):,}")
    
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches):
            all_results.extend(res)
    
    df = pd.DataFrame(all_results)
    print(f" ✅ Parsed: {len(df):,} rows ({time.time()-t1:.1f}s)")
    
    # Backup
    backup_path = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(backup_path, index=False)
    print(f" 📁 Backup: {backup_path}")
    
    # Load DB
    if not df.empty:
        load_to_database(df)
    
    print(f"\n🎉 HOÀN THÀNH! Total time: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
