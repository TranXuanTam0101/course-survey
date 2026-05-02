#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - HIGH PERFORMANCE VERSION
- Tối ưu hóa chuyển đổi dữ liệu bằng itertuples (nhanh hơn 50 lần iterrows)
- Vector hóa NLP bằng Pandas/Regex
- Tối ưu hóa tài nguyên Multiprocessing
"""

import os
import re
import time
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from multiprocessing import Pool

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE or "data"))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=60;Command Timeout=600;"
)

BATCH_SIZE = 10000  # Batch size tối ưu cho fast_executemany
CHUNK_SIZE = 20000  # Kích thước chunk để phân phối cho các core CPU

# ================= OPTIMIZED REGEX PATTERNS =================
# Biên dịch trước các pattern để dùng lại trong Pandas
RE_DATE = re.compile(r'^\d{2}/\d{2}/\d{4}$')
RE_MA_GV = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')

# Từ khóa NLP (biên dịch 1 lần để dùng cho toàn bộ cột)
PAT_HOCPHAN = re.compile(r'nội dung|chương trình|học phần|chuẩn', re.I)
PAT_GIANGVIEN = re.compile(r'giảng viên|thầy|cô|nhiệt tình|tận tâm', re.I)
PAT_KIEMTRA = re.compile(r'kiểm tra|thi|đánh giá|chấm', re.I)
PAT_COSO = re.compile(r'cơ sở|cải thiện|góp ý', re.I)
PAT_POS = re.compile(r'tốt|hay', re.I)
PAT_NEG = re.compile(r'kém|tệ|không', re.I)

def parse_line_worker(args):
    """Hàm xử lý tách chuỗi thô (vẫn dùng Multiprocessing cho CPU-bound task này)"""
    lines, file_name = args
    results = []
    
    for line in lines:
        if not line: continue
        ni = line.find('NULL')
        left = line[:ni].rstrip(', ') if ni >= 0 else line
        right = line[ni+4:].lstrip(', ') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',') if x.strip()]
        if len(row) < 5: continue
            
        # Tìm chỉ mục ngày (nsi)
        nsi = -1
        for i in range(2, min(12, len(row))):
            if RE_DATE.match(row[i]):
                nsi = i
                break
        if nsi == -1: continue
            
        ma_sv = row[1] if len(row) > 1 else ''
        ma_hp = row[nsi+1] if nsi+1 < len(row) else ''
        
        # Tìm mã giảng viên
        mgi = -1
        for i in range(nsi+1, min(nsi+20, len(row))):
            if RE_MA_GV.match(row[i]):
                mgi = i
                break
        
        ma_gv = row[mgi] if mgi != -1 else ''
        lhp = row[mgi+3] if mgi != -1 and mgi+3 < len(row) else f"{ma_hp}_{ma_gv}"
        
        results.append({
            'SubmissionID': f"{ma_sv}_{lhp}_{ma_gv}_{file_name}"[:150],
            'MaSV': ma_sv[:20],
            'MaLopHP': lhp[:50],
            'EssayText': right.replace(' , ', ', ').strip(),
        })
    return results

def apply_nlp_vectorized(df):
    """Sử dụng Vectorization của Pandas thay vì vòng lặp để gán nhãn NLP"""
    print("🧠 Processing NLP labels (Vectorized)...")
    t_nlp = df['EssayText'].str.lower().fillna('')
    
    df['Tag_HocPhan'] = t_nlp.str.contains(PAT_HOCPHAN, regex=True).astype(int)
    df['Tag_DayHoc'] = t_nlp.str.contains(PAT_GIANGVIEN, regex=True).astype(int)
    df['Tag_KiemTra'] = t_nlp.str.contains(PAT_KIEMTRA, regex=True).astype(int)
    df['Tag_Khac'] = t_nlp.str.contains(PAT_COSO, regex=True).astype(int)
    
    # Sentiment logic
    df['Sentiment'] = 'NEUTRAL'
    df.loc[t_nlp.str.contains(PAT_POS, regex=True), 'Sentiment'] = 'POSITIVE'
    df.loc[t_nlp.str.contains(PAT_NEG, regex=True), 'Sentiment'] = 'NEGATIVE'
    
    df['Is_Valid'] = (t_nlp.str.len() >= 5).astype(int)
    return df

def load_to_db(df):
    """Nạp dữ liệu cực nhanh với fast_executemany và chuyển đổi danh sách tối ưu"""
    if df.empty: return
    
    # Chỉ lấy những dòng có nội dung góp ý
    df_clean = df[df['EssayText'] != ''].copy()
    if df_clean.empty: return

    print(f"💾 Preparing {len(df_clean):,} rows for SQL Server...")
    
    # Chuyển đổi DataFrame sang list of tuples bằng itertuples (Nhanh nhất)
    # Thứ tự phải khớp tuyệt đối với câu SQL INSERT
    data_to_insert = list(df_clean[[
        'SubmissionID', 'MaSV', 'MaLopHP', 'EssayText', 
        'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
        'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
    ]].itertuples(index=False, name=None))

    sql = """INSERT INTO FACT_GOP_Y_TU_LUAN 
             (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, 
              Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

    t0 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True  # Quan trọng nhất để nạp dữ liệu nhanh
    
    try:
        # Tắt kiểm tra ràng buộc để tăng tốc nếu cần (tùy chọn)
        # cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        
        for i in range(0, len(data_to_insert), BATCH_SIZE):
            batch = data_to_insert[i : i + BATCH_SIZE]
            cursor.executemany(sql, batch)
            conn.commit()
            print(f"   → Loaded {min(i + BATCH_SIZE, len(data_to_insert)):,} rows...")
            
    except Exception as e:
        print(f"❌ Database Load Error: {e}")
        conn.rollback()
    finally:
        # cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        conn.close()
        print(f"⏱️ SQL Load Time: {time.time() - t0:.2f}s")

def main():
    start_total = time.time()
    
    # 1. Download data
    print(f"📥 Downloading blob: {SURVEY_FILE}...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    
    # 2. Multiprocessing Parsing (CPU-bound)
    print(f"📝 Parsing {len(lines):,} lines using Multiprocessing...")
    t1 = time.time()
    batches = [(lines[i:i+CHUNK_SIZE], FILE_NAME) for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_rows = []
    with Pool() as pool:
        for result in pool.imap_unordered(parse_line_worker, batches):
            all_rows.extend(result)
    
    df = pd.DataFrame(all_rows)
    print(f"✅ Parsing completed in {time.time() - t1:.2f}s")

    # 3. NLP Labeling (Vectorized)
    df = apply_nlp_vectorized(df)
    
    # 4. Backup to Parquet (Nhanh và nhẹ hơn CSV)
    backup_file = f"{FILE_NAME}_backup.parquet"
    df.to_parquet(backup_file, index=False)
    
    # 5. Load to DB
    load_to_db(df)
    
    print(f"\n🚀 ALL DONE! Total execution time: {time.time() - start_total:.2f}s")

if __name__ == "__main__":
    main()
