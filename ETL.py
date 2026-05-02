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
    print("❌ Missing environment variables SEMESTER or SURVEY_FILE")
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
    f"Connection Timeout=300;Command Timeout=600;"
)

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 100000 
BATCH_SIZE = 50000 

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

# ================= MASTER DATA =================
_g_cn, _g_hp = {}, {}

def load_master_from_db():
    global _g_cn, _g_hp
    print("📚 Loading Master Data...")
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            # Sửa lỗi Ambiguous bằng cách chỉ định rõ alias cn.MaNganh
            cursor.execute("""
                SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, n.TenNganh, n.MaKhoa, k.TenKhoa 
                FROM DIM_CHUYEN_NGANH cn 
                JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh 
                JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa
            """)
            for r in cursor.fetchall():
                _g_cn[str(r[0]).strip()] = tuple(str(x).strip() for x in r)
            
            cursor.execute("SELECT MaHP, TenHP FROM DIM_HOC_PHAN")
            for r in cursor.fetchall():
                _g_hp[str(r[0]).strip()] = str(r[1]).strip()
        print(f"   -> Loaded {len(_g_cn)} CN, {len(_g_hp)} HP")
    except Exception as e: 
        print(f"   ⚠️ Master Load Error: {e}")

# ================= PARSE LOGIC =================
def nlp_fast(text):
    if not text or len(text) < 5: return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    t1 = 1 if any(k in t for k in ('nội dung','chương trình','học phần')) else 0
    t2 = 1 if any(k in t for k in ('giảng viên','thầy','cô','nhiệt tình')) else 0
    t3 = 1 if any(k in t for k in ('kiểm tra','thi','đánh giá')) else 0
    t4 = 1 if any(k in t for k in ('cơ sở','wifi','phòng học')) else 0
    s = 'POSITIVE' if 'tốt' in t or 'hay' in t else 'NEGATIVE' if 'tệ' in t or 'không' in t else 'NEUTRAL'
    return t1, t2, t3, t4, s, 1

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
        
        res.append([
            f"{ma_sv}_{lhp}_{ma_gv}_{file_name}"[:150], ma_sv, lhp, 
            right[:4000], sent, valid, t1, t2, t3, t4, ch, gt
        ])
    return res

# ================= DB LOAD =================
def load_to_db(df):
    print("\n💾 Loading to Database (fast_executemany)...")
    t0 = time.time()
    
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        
        # 1. FACT_GOP_Y (Gán cột chính xác)
        print("   -> FACT_GOP_Y...")
        df_gy = df[df['EssayText'] != ''].copy()
        if not df_gy.empty:
            cols_to_db = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
            # Lấy đúng 10 cột đầu từ df_gy (đã được parse theo thứ tự tương ứng)
            data_gy = df_gy.iloc[:, 0:10].values.tolist()
            
            sql_gy = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({','.join(cols_to_db)}) VALUES ({','.join(['?']*10)})"
            cursor.executemany(sql_gy, data_gy)
            conn.commit()
            print(f"      ✅ Inserted {len(data_gy):,} rows")

        # 2. FACT_KET_QUA (Xử lý Vectorized)
        print("   -> FACT_KET_QUA...")
        # Trắc nghiệm (Câu 1-12)
        df_kq = df[(df['CH'] != '') & (df['GT'] != '')].copy()
        df_kq['MaCauHoi'] = pd.to_numeric(df_kq['CH'], errors='coerce')
        df_kq['Diem'] = pd.to_numeric(df_kq['GT'], errors='coerce')
        df_kq = df_kq[df_kq['MaCauHoi'].between(1, 12)][['SubmissionID','MaCauHoi','Diem']]
        
        # Tự luận (Câu 13-16)
        df_ess = df[df['EssayText'] != ''].drop_duplicates('SubmissionID').copy()
        s_map = {'POSITIVE': 5, 'NEUTRAL': 3, 'NEGATIVE': 2}
        df_ess['Diem'] = df_ess['Sentiment'].map(s_map)
        
        ess_list = []
        for mc in [13, 14, 15, 16]:
            tmp = df_ess[['SubmissionID', 'Diem']].copy()
            tmp['MaCauHoi'] = mc
            ess_list.append(tmp)
        
        final_kq = pd.concat([df_kq] + ess_list).dropna()
        if not final_kq.empty:
            data_kq = final_kq.values.tolist()
            sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
            cursor.executemany(sql_kq, data_kq)
            conn.commit()
            print(f"      ✅ Inserted {len(data_kq):,} rows")

    print(f"   ⏱️ SQL Load Time: {time.time()-t0:.1f}s")

def main():
    start = time.time()
    load_master_from_db()
    
    print(f"📥 Downloading {SURVEY_FILE}...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    print(f"📝 Parsing {len(lines):,} lines...")
    with Pool(NUM_WORKERS) as pool:
        all_res = []
        for res in pool.imap_unordered(parse_batch, batches):
            all_res.extend(res)
    
    df = pd.DataFrame(all_res, columns=['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','CH','GT'])
    
    load_to_db(df)
    print(f"\n🎉 ALL DONE! Total Time: {time.time()-start:.1f}s")

if __name__ == "__main__":
    main()
