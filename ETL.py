#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

if not SEMESTER or not SURVEY_FILE:
    print("❌ Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=60;Command Timeout=300;"
)

# ================= PATTERNS & NLP =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

def nlp_fast(text):
    if not text or len(text) < 5: return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(k in t for k in ('nội dung','chương trình','học phần')) else 0
    t2 = 1 if any(k in t for k in ('giảng viên','thầy','cô','nhiệt tình')) else 0
    t3 = 1 if any(k in t for k in ('kiểm tra','thi','đánh giá')) else 0
    t4 = 1 if any(k in t for k in ('cơ sở','wifi','phòng học')) else 0
    s = 'POSITIVE' if any(k in t for k in ('tốt','hay','hài lòng')) else 'NEGATIVE' if any(k in t for k in ('tệ','kém','không')) else 'NEUTRAL'
    return t1, t2, t3, t4, s, 1

# ================= PARSE LOGIC =================
def parse_batch(args):
    lines, file_name = args
    res = []
    for line in lines:
        if not line: continue
        ni = line.find('NULL')
        left = line[:ni].rstrip(', ') if ni >= 0 else line
        right = line[ni+4:].lstrip(', ') if ni >= 0 else ''
        row = [x.strip() for x in left.split(',')]
        if len(row) < 10: continue
        
        nsi = next((i for i in range(2, 12) if i < len(row) and _DATE_RE(row[i])), -1)
        if nsi == -1: continue
        mgi = next((i for i in range(nsi+1, min(nsi+25, len(row))) if _MA_GV_RE(row[i])), nsi+8)
        
        ma_sv, ma_hp, ma_gv = row[1], row[nsi+1], row[mgi] if mgi < len(row) else ''
        lhp = row[mgi+3] if mgi+3 < len(row) else f"{ma_hp}_{ma_gv}"
        ch = row[mgi+4] if mgi+4 < len(row) else ''
        gt = row[mgi+5] if mgi+5 < len(row) else ''
        
        t1,t2,t3,t4,sent,valid = nlp_fast(right)
        res.append([f"{ma_sv}_{lhp}_{ma_gv}_{file_name}"[:150], ma_sv, lhp, right[:4000], sent, valid, t1, t2, t3, t4, ch, gt])
    return res

# ================= DATABASE LOAD (STABLE BATCHING) =================
def load_to_db(df):
    print("\n💾 Loading to Database (Mini-Batch Mode)...")
    t0 = time.time()
    SUB_BATCH_SIZE = 5000  # Kích thước an toàn cho Azure SQL

    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            
            # 1. FACT_GOP_Y_TU_LUAN
            df_gy = df[df['EssayText'] != ''].copy()
            if not df_gy.empty:
                print(f"   -> Inserting FACT_GOP_Y ({len(df_gy):,} rows)...")
                cols_gy = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
                data_gy = df_gy.iloc[:, 0:10].values.tolist()
                sql_gy = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({','.join(cols_gy)}) VALUES ({','.join(['?']*10)})"
                
                for i in range(0, len(data_gy), SUB_BATCH_SIZE):
                    batch = data_gy[i : i + SUB_BATCH_SIZE]
                    cursor.executemany(sql_gy, batch)
                    conn.commit() # Commit ngay để tránh tràn Transaction Log
            
            # 2. FACT_KET_QUA_DANH_GIA (Vectorized Processing)
            print("   -> Preparing FACT_KET_QUA...")
            df_kq = df[(df['CH'] != '') & (df['GT'] != '')].copy()
            df_kq['MaCauHoi'] = pd.to_numeric(df_kq['CH'], errors='coerce')
            df_kq['Diem'] = pd.to_numeric(df_kq['GT'], errors='coerce')
            df_kq = df_kq[df_kq['MaCauHoi'].between(1, 12)][['SubmissionID','MaCauHoi','Diem']]
            
            df_ess = df[df['EssayText'] != ''].drop_duplicates('SubmissionID').copy()
            s_map = {'POSITIVE': 5, 'NEUTRAL': 3, 'NEGATIVE': 2}
            df_ess['Diem'] = df_ess['Sentiment'].map(s_map).fillna(3)
            
            ess_rows = []
            for mc in [13, 14, 15, 16]:
                tmp = df_ess[['SubmissionID', 'Diem']].copy()
                tmp['MaCauHoi'] = mc
                ess_rows.append(tmp)
            
            final_kq = pd.concat([df_kq] + ess_rows).dropna()
            if not final_kq.empty:
                print(f"   -> Inserting FACT_KET_QUA ({len(final_kq):,} rows)...")
                data_kq = final_kq.values.tolist()
                sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
                for i in range(0, len(data_kq), SUB_BATCH_SIZE):
                    batch = data_kq[i : i + SUB_BATCH_SIZE]
                    cursor.executemany(sql_kq, batch)
                    conn.commit()

    except pyodbc.Error as e:
        print(f"❌ SQL Error: {e}")
        sys.exit(1)
    
    print(f"   ⏱️ SQL Load Time: {time.time()-t0:.1f}s")

# ================= MAIN =================
def main():
    start_time = time.time()
    
    print(f"📥 Downloading {SURVEY_FILE} from Blob...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    num_lines = len(lines)
    chunk_size = max(1, num_lines // cpu_count())
    batches = [(lines[i:i+chunk_size], FILE_NAME) for i in range(0, num_lines, chunk_size)]
    
    print(f"📝 Parsing {num_lines:,} lines with {cpu_count()} cores...")
    with Pool(cpu_count()) as pool:
        all_res = []
        for res in pool.imap_unordered(parse_batch, batches):
            all_res.extend(res)
    
    df = pd.DataFrame(all_res, columns=['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','CH','GT'])
    
    load_to_db(df)
    print(f"\n🎉 HOÀN THÀNH! Tổng thời gian: {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    main()
