#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BƯỚC 2: LOAD CSV → DATABASE - 1 VÒNG LẶP
"""
import os, sys, time, pyodbc, pandas as pd, io
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

SERVER = "course-survey.database.windows.net"
DB = "course-survey-db"
UID = "sqladmin"

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};"
    f"DATABASE={DB};UID={UID};PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=600;Command Timeout=1800;"
)

CONTAINER_NAME = SEMESTER
PROCESSED_PATH = "processed-data"
BATCH = 50000

def safe_str(val, max_len=None):
    if pd.isna(val): return ''
    s = str(val).strip()
    return s[:max_len] if max_len else s

def load_existing(cur, table, col):
    cur.execute(f"SELECT {col} FROM {table}")
    return {str(r[0]).strip() for r in cur.fetchall()}

def fast_insert(cur, sql, data):
    """Insert nhanh, bỏ qua lỗi"""
    if not data: return 0
    cur.fast_executemany = True
    for i in range(0, len(data), BATCH):
        chunk = data[i:i+BATCH]
        try:
            cur.executemany(sql, chunk)
            cur.connection.commit()
        except:
            for row in chunk:
                try: cur.execute(sql, row); cur.connection.commit()
                except: pass
    return len(data)

def main():
    t0 = time.time()
    print("="*50, "\n📊 BƯỚC 2: LOAD CSV → DATABASE (1 LOOP)\n", "="*50)
    
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    nh = f"{nbd}-{nbd+1}"
    
    # Đọc CSV
    t1 = time.time()
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    client = container.get_blob_client(f"{PROCESSED_PATH}/{FILE_NAME}_parsed.csv")
    content = client.download_blob().readall().decode('utf-8-sig')
    df = pd.read_csv(io.StringIO(content), sep='|', dtype=str, keep_default_na=False).fillna('')
    print(f"  Read CSV: {len(df):,} rows ({time.time()-t1:.1f}s)")
    
    # Kết nối DB
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    
    # Tắt constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
               'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    
    cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nh}',{hk})")
    
    # Load existing IDs
    t1 = time.time()
    exist_lop = load_existing(cur, 'DIM_LOP_SINH_VIEN', 'MaLop')
    exist_sv  = load_existing(cur, 'DIM_SINH_VIEN', 'MaSV')
    exist_gv  = load_existing(cur, 'DIM_GIANG_VIEN', 'MaGV')
    exist_lhp = load_existing(cur, 'DIM_LOP_HOC_PHAN', 'MaLopHP')
    exist_gy  = load_existing(cur, 'FACT_GOP_Y_TU_LUAN', 'SubmissionID')
    print(f"  Existing IDs: {time.time()-t1:.1f}s")
    
    # ===== 1 VÒNG LẶP DUY NHẤT =====
    t1 = time.time()
    seen_lop, seen_sv, seen_gv, seen_lhp, seen_gy = set(), set(), set(), set(), set()
    data_lop, data_sv, data_gv, data_lhp, data_gy, data_kq = [], [], [], [], [], []
    
    # Đổi DataFrame thành numpy array để lặp nhanh hơn
    arr = df[['SubmissionID','MaSV','MaLop','MaHP','MaGV','MaLopHP','CauHoi','GiaTri','EssayText',
               'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid']].values
    
    for row in arr:
        sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1v, t2v, t3v, t4v, sent, valid = row
        
        sv = safe_str(sv, 50)
        ml = safe_str(ml, 50)
        hp = safe_str(hp, 50)
        gv = safe_str(gv, 50)
        lhp = safe_str(lhp, 100)
        essay = safe_str(essay)
        sent = safe_str(sent, 20) or 'NEUTRAL'
        sid = safe_str(sid, 200)
        
        # DIM_LOP
        if ml and ml not in seen_lop and ml not in exist_lop:
            seen_lop.add(ml)
            data_lop.append((ml, ml, ml))
        
        # DIM_SV
        if sv and sv not in seen_sv and sv not in exist_sv:
            seen_sv.add(sv)
            data_sv.append((sv, '', '', None, ml))
        
        # DIM_GV
        if gv and gv not in seen_gv and gv not in exist_gv:
            seen_gv.add(gv)
            data_gv.append((gv, '', ''))
        
        # DIM_LHP
        if lhp and lhp not in seen_lhp and lhp not in exist_lhp:
            seen_lhp.add(lhp)
            data_lhp.append((lhp, lhp, hp, gv, mhk))
        
        # FACT_GY
        if essay and sid not in seen_gy and sid not in exist_gy:
            seen_gy.add(sid)
            data_gy.append((
                sid, sv, lhp, essay[:4000], sent[:20],
                int(float(valid)) if valid else 0,
                int(float(t1v)) if t1v else 0,
                int(float(t2v)) if t2v else 0,
                int(float(t3v)) if t3v else 0,
                int(float(t4v)) if t4v else 0,
            ))
            d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
            for mc in [13,14,15,16]:
                data_kq.append((sid, mc, d))
        
        # FACT_KQ - trắc nghiệm
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    data_kq.append((sid, mc, d))
            except: pass
    
    print(f"  Prepare: {time.time()-t1:.1f}s")
    print(f"  LOP={len(data_lop)} SV={len(data_sv)} GV={len(data_gv)} LHP={len(data_lhp)} GY={len(data_gy)} KQ={len(data_kq)}")
    
    # Insert tất cả
    t1 = time.time()
    fast_insert(cur, "INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data_lop)
    fast_insert(cur, "INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data_sv)
    fast_insert(cur, "INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data_gv)
    fast_insert(cur, "INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data_lhp)
    fast_insert(cur, "INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data_gy)
    fast_insert(cur, "INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", data_kq)
    print(f"  Insert: {time.time()-t1:.1f}s")
    
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    
    conn.close()
    print(f"\n🎉 Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
