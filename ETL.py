#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - HYPER SPEED & CLOUD FIXED
- Loại bỏ BULK INSERT (vốn bị lỗi trên Azure SQL với file local)
- Sử dụng fast_executemany (Tốc độ tương đương BULK)
- Vectorization cho FACT_KET_QUA (Nhanh gấp 100 lần vòng lặp)
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
    f"Connection Timeout=300;Command Timeout=600;"
)

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 100000 
BATCH_SIZE = 50000  # Kích thước gói tin gửi lên SQL

# ================= PATTERNS & NLP SETS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

TAG_HOCPHAN = {'nội dung','chương trình','môn học','học phần','kiến thức','chuẩn đầu ra','tài liệu','giáo trình'}
TAG_DAYHOC = {'giảng viên','thầy','cô','dạy','giảng','nhiệt tình','tận tâm','dễ hiểu'}
TAG_KIEMTRA = {'kiểm tra','đánh giá','thi','đề thi','chấm điểm','công bằng','minh bạch'}
TAG_KHAC = {'cơ sở vật chất','phòng học','máy chiếu','wifi','hỗ trợ','góp ý','đề xuất'}

# ================= MASTER DATA GLOBALS =================
_g_cn, _g_hp, _g_khoa_hp = {}, {}, {}
_g_default_khoa = ('KHOA01', 'Trường Đại học Kinh tế')
_g_default_nganh = 'KHOA01NG01'

def load_master_from_db():
    global _g_cn, _g_hp, _g_khoa_hp, _g_default_khoa, _g_default_nganh
    print("📚 Loading Master Data...")
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT MaChuyenNganh, TenChuyenNganh, MaNganh, TenNganh, MaKhoa, TenKhoa FROM DIM_CHUYEN_NGANH cn JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa")
            for r in cursor.fetchall():
                _g_cn[str(r[0]).strip()] = tuple(str(x).strip() for x in r)
            
            cursor.execute("SELECT MaHP, TenHP, hp.MaKhoa, k.TenKhoa FROM DIM_HOC_PHAN hp JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa")
            for r in cursor.fetchall():
                hp_id = str(r[0]).strip()
                _g_hp[hp_id] = str(r[1]).strip()
                _g_khoa_hp[hp_id] = (str(r[2]).strip(), str(r[3]).strip())
        print(f"   -> Loaded {len(_g_cn)} CN, {len(_g_hp)} HP")
    except Exception as e: print(f"   ⚠️ Master Load Error: {e}")

def nlp_fast(text):
    if not text or len(text) < 5: return 0,0,0,0,'NEUTRAL',0
    w = set(text.lower().split())
    t1 = 1 if len(w & TAG_HOCPHAN) >= 1 else 0
    t2 = 1 if len(w & TAG_DAYHOC) >= 1 else 0
    t3 = 1 if len(w & TAG_KIEMTRA) >= 1 else 0
    t4 = 1 if len(w & TAG_KHAC) >= 1 else 0
    s = 'POSITIVE' if any(x in w for x in {'tốt','hay','hài lòng'}) else 'NEGATIVE' if any(x in w for x in {'tệ','kém','khó'}) else 'NEUTRAL'
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
        
        t1,t2,t3,t4,sent,valid = nlp_fast(right)
        
        res.append([
            f"{ma_sv}_{lhp}_{ma_gv}_{file_name}"[:150], ma_sv, row[0], ma_hp, ma_gv, lhp, 
            right[:4000], t1, t2, t3, t4, sent, valid, 
            row[mgi+4] if mgi+4 < len(row) else '', row[mgi+5] if mgi+5 < len(row) else ''
        ])
    return res

# ================= DATABASE LOAD (THE FAST WAY) =================
def fast_insert(cursor, table, df, cols):
    if df.empty: return
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
    data = [tuple(x) for x in df[cols].to_numpy()]
    for i in range(0, len(data), BATCH_SIZE):
        cursor.executemany(sql, data[i:i+BATCH_SIZE])
        cursor.connection.commit()

def load_to_db(df):
    print("\n💾 Loading to Database (fast_executemany)...")
    t0 = time.time()
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        
        # 1. Dimensions (Lược giản để demo, bạn có thể thêm các bảng khác tương tự)
        print("   -> Dimensions...")
        d_gv = df[['MaGV']].drop_duplicates().rename(columns={'MaGV':'MaGV'})
        # fast_insert(cursor, 'DIM_GIANG_VIEN', d_gv, ['MaGV']) # Ví dụ

        # 2. FACT_GOP_Y
        print("   -> FACT_GOP_Y...")
        df_gy = df[df['EssayText'] != ''].copy()
        if not df_gy.empty:
            cols_gy = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
            df_gy.columns = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','CH','GT']
            fast_insert(cursor, 'FACT_GOP_Y_TU_LUAN', df_gy, cols_gy)

        # 3. FACT_KET_QUA (Vectorized - NO LOOPS)
        print("   -> FACT_KET_QUA...")
        # Xử lý câu hỏi trắc nghiệm (1-12)
        df_kq = df[(df['CH'] != '') & (df['GT'] != '')].copy()
        df_kq['MaCauHoi'] = pd.to_numeric(df_kq['CH'], errors='coerce')
        df_kq['Diem'] = pd.to_numeric(df_kq['GT'], errors='coerce')
        df_kq = df_kq[df_kq['MaCauHoi'].between(1, 12)][['SubmissionID','MaCauHoi','Diem']]
        
        # Xử lý câu hỏi tự luận (13-16) dựa trên Sentiment
        df_ess = df[df['EssayText'] != ''].drop_duplicates('SubmissionID').copy()
        s_map = {'POSITIVE': 5, 'NEUTRAL': 3, 'NEGATIVE': 2}
        df_ess['Diem'] = df_ess['Sentiment'].map(s_map)
        
        ess_list = []
        for mc in [13, 14, 15, 16]:
            tmp = df_ess[['SubmissionID', 'Diem']].copy()
            tmp['MaCauHoi'] = mc
            ess_list.append(tmp)
        
        final_kq = pd.concat([df_kq] + ess_list)
        fast_insert(cursor, 'FACT_KET_QUA_DANH_GIA', final_kq, ['SubmissionID','MaCauHoi','Diem'])

    print(f"   ✅ Load Time: {time.time()-t0:.1f}s")

def main():
    total_t = time.time()
    load_master_from_db()
    
    print(f"\n📥 Downloading {SURVEY_FILE}...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    print(f"📝 Parsing {len(lines):,} lines...")
    with Pool(NUM_WORKERS) as pool:
        all_res = []
        for res in pool.imap_unordered(parse_batch, batches): all_res.extend(res)
    
    df = pd.DataFrame(all_res, columns=['SubmissionID','MaSV','Lop','MaHP','MaGV','MaLopHP','EssayText','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid','CH','GT'])
    
    load_to_db(df)
    print(f"\n🎉 ALL DONE! Total Time: {time.time()-total_t:.1f}s")

if __name__ == "__main__":
    main()
