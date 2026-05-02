#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import re
import time
import pandas as pd
import numpy as np
import pyodbc
from azure.storage.blob import BlobServiceClient
from multiprocessing import Pool, cpu_count

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

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
    f"Connection Timeout=60;"
)

# ================= TỐI ƯU HÓA XỬ LÝ CHUỖI =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

def nlp_fast(text):
    if not text or len(text) < 5: return 0,0,0,0,'NEUTRAL',0
    t = text.lower()
    return (1 if 'nội dung' in t or 'chương trình' in t else 0,
            1 if 'giảng viên' in t or 'thầy' in t or 'cô' in t else 0,
            1 if 'kiểm tra' in t or 'thi' in t else 0,
            1 if 'cơ sở' in t or 'wifi' in t else 0,
            'POSITIVE' if 'tốt' in t or 'hay' in t else 'NEGATIVE' if 'tệ' in t in t else 'NEUTRAL',
            1)

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
        
        ma_sv, lhp = row[1], (row[mgi+3] if mgi+3 < len(row) else f"{row[nsi+1]}_{row[mgi]}")
        t1, t2, t3, t4, sent, val = nlp_fast(right)
        
        res.append([f"{ma_sv}_{lhp}_{file_name}"[:150], ma_sv, lhp, right[:4000], sent, val, t1, t2, t3, t4, 
                    row[mgi+4] if mgi+4 < len(row) else '', row[mgi+5] if mgi+5 < len(row) else ''])
    return res

# ================= CHÈN THẲNG VÀO DATABASE (MAX SPEED) =================
def load_to_db(df):
    print(f"\n🚀 Đang đẩy {len(df):,} dòng vào SQL Server...")
    t0 = time.time()
    
    # 1. Xử lý dữ liệu trong RAM trước khi đẩy
    df_gy = df[df['EssayText'] != ''].drop_duplicates('SubmissionID').iloc[:, 0:10]
    
    # Xử lý FACT_KET_QUA bằng Vectorization
    df_kq_raw = df[(df['CH'] != '') & (df['GT'] != '')].copy()
    df_kq_raw['MaCauHoi'] = pd.to_numeric(df_kq_raw['CH'], errors='coerce')
    df_kq_raw['Diem'] = pd.to_numeric(df_kq_raw['GT'], errors='coerce')
    df_kq = df_kq_raw[df_kq_raw['MaCauHoi'].between(1, 12)][['SubmissionID','MaCauHoi','Diem']]
    
    # Sentiment Mapping cho câu 13-16
    df_ess = df[df['EssayText'] != ''].drop_duplicates('SubmissionID')
    s_map = {'POSITIVE': 5, 'NEUTRAL': 3, 'NEGATIVE': 2}
    df_ess_score = df_ess[['SubmissionID', 'Sentiment']].copy()
    df_ess_score['Diem'] = df_ess_score['Sentiment'].map(s_map).fillna(3)
    
    kq_final = pd.concat([df_kq] + [df_ess_score[['SubmissionID', 'Diem']].assign(MaCauHoi=m) for m in [13,14,15,16]])
    kq_final = kq_final.dropna().drop_duplicates(['SubmissionID', 'MaCauHoi'])

    # 2. Thực hiện chèn thẳng (Transaction duy nhất)
    with pyodbc.connect(CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True  # Kích hoạt chế độ chèn siêu tốc
        
        # Xóa dữ liệu cũ để tránh lỗi Primary Key (Idempotent)
        print("🧹 Đang dọn dẹp dữ liệu cũ của file này...")
        cursor.execute("DELETE FROM FACT_GOP_Y_TU_LUAN WHERE SubmissionID LIKE ?", (f"%_{FILE_NAME}",))
        cursor.execute("DELETE FROM FACT_KET_QUA_DANH_GIA WHERE SubmissionID LIKE ?", (f"%_{FILE_NAME}",))
        
        # Chèn FACT_GOP_Y
        print(f"📥 Đang chèn {len(df_gy):,} dòng vào FACT_GOP_Y...")
        sql_gy = "INSERT INTO FACT_GOP_Y_TU_LUAN VALUES (?,?,?,?,?,?,?,?,?,?)"
        cursor.executemany(sql_gy, df_gy.values.tolist())
        
        # Chèn FACT_KET_QUA
        print(f"📥 Đang chèn {len(kq_final):,} dòng vào FACT_KET_QUA...")
        sql_kq = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?,?,?)"
        cursor.executemany(sql_kq, kq_final.values.tolist())
        
        conn.commit()

    print(f"✅ Hoàn tất nạp DB trong {time.time()-t0:.1f} giây.")

def main():
    start = time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    cpus = cpu_count()
    chunk = len(lines) // cpus
    batches = [(lines[i:i+chunk], FILE_NAME) for i in range(0, len(lines), chunk)]
    
    with Pool(cpus) as pool:
        all_res = []
        for res in pool.imap_unordered(parse_batch, batches): all_res.extend(res)
    
    df = pd.DataFrame(all_res, columns=['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','CH','GT'])
    
    load_to_db(df)
    print(f"⏱️ Tổng thời gian thực thi: {time.time()-start:.1f}s")

if __name__ == "__main__":
    main()
