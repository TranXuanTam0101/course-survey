#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BƯỚC 2: LOAD CSV → DATABASE - KHÔNG VÒNG LẶP
- Dùng pandas drop_duplicates, to_numpy
- executemany thẳng
"""
import os, time, pyodbc, pandas as pd, io
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

def main():
    t0 = time.time()
    print("="*50, "\n📊 BƯỚC 2: LOAD CSV → DATABASE (NO LOOP)\n", "="*50)
    
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    
    # Đọc CSV
    t1 = time.time()
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    client = container.get_blob_client(f"{PROCESSED_PATH}/{FILE_NAME}_parsed.csv")
    content = client.download_blob().readall().decode('utf-8-sig')
    df = pd.read_csv(io.StringIO(content), sep='|', dtype=str, keep_default_na=False).fillna('')
    df.columns = df.columns.str.strip()
    print(f"  Read CSV: {len(df):,} rows ({time.time()-t1:.1f}s)")
    
    # Kết nối DB
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    cur.fast_executemany = True
    
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
               'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nbd}-{nbd+1}',{hk})")
    
    t1 = time.time()
    
    # ===== DIM_LOP: drop_duplicates + to_numpy =====
    df_lop = df[['MaLop']].drop_duplicates()
    df_lop['MaLop'] = df_lop['MaLop'].str[:50]
    data = [(r[0], r[0], r[0]) for r in df_lop[df_lop['MaLop']!=''].to_numpy()]
    cur.executemany("INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data)
    print(f"  DIM_LOP: {len(data):,} ({time.time()-t1:.1f}s)")
    
    # ===== DIM_SV =====
    t1=time.time()
    df_sv = df[['MaSV','MaLop']].drop_duplicates('MaSV')
    df_sv['MaSV'] = df_sv['MaSV'].str[:50]
    df_sv['MaLop'] = df_sv['MaLop'].str[:50]
    data = [(r[0], '', '', None, r[1]) for r in df_sv[df_sv['MaSV']!=''].to_numpy()]
    cur.executemany("INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data)
    print(f"  DIM_SV: {len(data):,} ({time.time()-t1:.1f}s)")
    
    # ===== DIM_GV =====
    t1=time.time()
    df_gv = df[['MaGV']].drop_duplicates()
    df_gv['MaGV'] = df_gv['MaGV'].str[:50]
    data = [(r[0], '', '') for r in df_gv[df_gv['MaGV']!=''].to_numpy()]
    cur.executemany("INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data)
    print(f"  DIM_GV: {len(data):,} ({time.time()-t1:.1f}s)")
    
    # ===== DIM_LHP =====
    t1=time.time()
    df_lhp = df[['MaLopHP','MaHP','MaGV']].drop_duplicates('MaLopHP')
    data = [(r[0][:100], r[0][:100], r[1][:50], r[2][:50], mhk) for r in df_lhp[df_lhp['MaLopHP']!=''].to_numpy()]
    cur.executemany("INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data)
    print(f"  DIM_LHP: {len(data):,} ({time.time()-t1:.1f}s)")
    
    # ===== FACT_GY =====
    t1=time.time()
    df_gy = df[df['EssayText']!=''].drop_duplicates('SubmissionID')
    data = [(
        r[0][:200], r[1][:50], r[5][:100], r[8][:4000],
        r[13][:20] if r[13] else 'NEUTRAL',
        int(float(r[14])) if r[14] else 0,
        int(float(r[9])) if r[9] else 0,
        int(float(r[10])) if r[10] else 0,
        int(float(r[11])) if r[11] else 0,
        int(float(r[12])) if r[12] else 0,
    ) for r in df_gy[['SubmissionID','MaSV','MaLop','MaHP','MaGV','MaLopHP','CauHoi','GiaTri','EssayText','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid']].to_numpy()]
    cur.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  FACT_GY: {len(data):,} ({time.time()-t1:.1f}s)")
    
    # ===== FACT_KQ =====
    t1=time.time()
    # Trắc nghiệm
    df_kq1 = df[(df['CauHoi']!='') & (df['GiaTri']!='')][['SubmissionID','CauHoi','GiaTri']]
    data_kq = []
    for r in df_kq1.to_numpy():
        try:
            mc, d = int(float(r[1])), int(float(r[2]))
            if 1<=mc<=12 and 1<=d<=5: data_kq.append((r[0][:200], mc, d))
        except: pass
    # Tự luận
    for r in df_gy[['SubmissionID','Sentiment']].to_numpy():
        d = 5 if r[1]=='POSITIVE' else (2 if r[1]=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: data_kq.append((r[0][:200], mc, d))
    
    for i in range(0, len(data_kq), BATCH):
        cur.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", data_kq[i:i+BATCH])
    print(f"  FACT_KQ: {len(data_kq):,} ({time.time()-t1:.1f}s)")
    
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    
    conn.close()
    print(f"\n🎉 Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
