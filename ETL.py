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

# ================= DATABASE LOAD =================
def load_to_db(df):
    print("\n💾 Loading to Database (Deduplicated Mode)...")
    t0 = time.time()
    SUB_BATCH_SIZE = 5000 

    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            
            # --- OPTIONAL: XÓA DỮ LIỆU CŨ CỦA FILE NÀY NẾU MUỐN CHÈN ĐÈ ---
            # cursor.execute("DELETE FROM FACT_GOP_Y_TU_LUAN WHERE SubmissionID LIKE ?", (f"%_{FILE_NAME}",))
            # cursor.execute("DELETE FROM FACT_KET_QUA_DANH_GIA WHERE SubmissionID LIKE ?", (f"%_{FILE_NAME}",))
            # conn.commit()

            # 1. FACT_GOP_Y_TU_LUAN
            df_gy = df[df['EssayText'] != ''].drop_duplicates(subset=['SubmissionID']).copy()
            if not df_gy.empty:
                print(f"   -> Inserting FACT_GOP_Y ({len(df_gy):,} unique rows)...")
                cols_gy = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
                data_gy = df_gy.iloc[:, 0:10].values.tolist()
                
                # Sử dụng cú pháp INSERT ... WHERE NOT EXISTS để tránh lỗi PK nếu dữ liệu đã có trong DB
                sql_gy = f"""
                    INSERT INTO FACT_GOP_Y_TU_LUAN ({','.join(cols_gy)})
                    SELECT {','.join(['?']*10)}
                    WHERE NOT EXISTS (SELECT 1 FROM FACT_GOP_Y_TU_LUAN WHERE SubmissionID = ?)
                """
                # Chuẩn bị dữ liệu: mỗi row cần thêm SubmissionID ở cuối để phục vụ mệnh đề WHERE
                data_gy_final = [row + [row[0]] for row in data_gy]
                
                for i in range(0, len(data_gy_final), SUB_BATCH_SIZE):
                    cursor.executemany(sql_gy, data_gy_final[i : i + SUB_BATCH_SIZE])
                    conn.commit()
            
            # 2. FACT_KET_QUA_DANH_GIA
            print("   -> Preparing FACT_KET_QUA...")
            df_kq_raw = df[(df['CH'] != '') & (df['GT'] != '')].copy()
            df_kq_raw['MaCauHoi'] = pd.to_numeric(df_kq_raw['CH'], errors='coerce')
            df_kq_raw['Diem'] = pd.to_numeric(df_kq_raw['GT'], errors='coerce')
            df_kq = df_kq_raw[df_kq_raw['MaCauHoi'].between(1, 12)][['SubmissionID','MaCauHoi','Diem']]
            
            df_ess = df[df['EssayText'] != ''].drop_duplicates('SubmissionID').copy()
            s_map = {'POSITIVE': 5, 'NEUTRAL': 3, 'NEGATIVE': 2}
            df_ess['Diem'] = df_ess['Sentiment'].map(s_map).fillna(3)
            
            ess_list = []
            for mc in [13, 14, 15, 16]:
                tmp = df_ess[['SubmissionID', 'Diem']].copy()
                tmp['MaCauHoi'] = mc
                ess_list.append(tmp)
            
            final_kq = pd.concat([df_kq] + ess_list).dropna().drop_duplicates(subset=['SubmissionID', 'MaCauHoi'])
            
            if not final_kq.empty:
                print(f"   -> Inserting FACT_KET_QUA ({len(final_kq):,} rows)...")
                data_kq = final_kq.values.tolist()
                sql_kq = """
                    INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem)
                    SELECT ?, ?, ?
                    WHERE NOT EXISTS (SELECT 1 FROM FACT_KET_QUA_DANH_GIA WHERE SubmissionID = ? AND MaCauHoi = ?)
                """
                # Data: SubID, MaCH, Diem, SubID, MaCH
                data_kq_final = [ [r[0], r[1], r[2], r[0], r[1]] for r in data_kq ]
                
                for i in range(0, len(data_kq_final), SUB_BATCH_SIZE):
                    cursor.executemany(sql_kq, data_kq_final[i : i + SUB_BATCH_SIZE])
                    conn.commit()

    except pyodbc.Error as e:
        print(f"❌ SQL Error: {e}")
        sys.exit(1)
    print(f"   ⏱️ SQL Load Time: {time.time()-t0:.1f}s")

# ================= MAIN =================
def main():
    start_time = time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    num_lines = len(lines)
    chunk_size = max(1, num_lines // cpu_count())
    batches = [(lines[i:i+chunk_size], FILE_NAME) for i in range(0, num_lines, chunk_size)]
    
    print(f"📝 Parsing {num_lines:,} lines...")
    with Pool(cpu_count()) as pool:
        all_res = []
        for res in pool.imap_unordered(parse_batch, batches):
            all_res.extend(res)
    
    df = pd.DataFrame(all_res, columns=['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','CH','GT'])
    
    load_to_db(df)
    print(f"\n🎉 THÀNH CÔNG! Tổng thời gian: {time.time()-start_time:.1f}s")

if __name__ == "__main__":
    main()
