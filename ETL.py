#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BƯỚC 2: LOAD CSV TỪ AZURE BLOB VÀO DATABASE
- Đọc CSV từ Azure Blob
- Tạo các bảng DIM + FACT
- Dùng BULK INSERT từ Azure Blob (nhanh nhất)
"""
import os, sys, re, time, pyodbc
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

# Lấy storage info
parts = dict(p.split('=', 1) for p in CONNECTION_STRING.split(';') if '=' in p)
STORAGE_ACCOUNT = parts.get('AccountName', '')
STORAGE_KEY = parts.get('AccountKey', '')

def main():
    t0 = time.time()
    print("="*50, "\n📊 BƯỚC 2: LOAD CSV → DATABASE\n", "="*50)
    
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    nh = f"{nbd}-{nbd+1}"
    
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    
    # Tắt constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
               'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    
    # DIM_HOC_KY
    cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nh}',{hk})")
    
    csv_url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}/{PROCESSED_PATH}/{FILE_NAME}_parsed.csv"
    
    # BULK INSERT DIM_LOP_SINH_VIEN
    t1 = time.time()
    sql = f"""
        INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh)
        SELECT DISTINCT MaLop, MaLop, MaLop
        FROM OPENROWSET(
            BULK '{csv_url}',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            FIRSTROW = 2,
            FIELDTERMINATOR = '|',
            ROWTERMINATOR = '\\n'
        ) AS r
        WHERE MaLop != ''
        AND NOT EXISTS (SELECT 1 FROM DIM_LOP_SINH_VIEN WHERE MaLop = r.MaLop)
    """
    try:
        cur.execute(sql)
        print(f"  DIM_LOP: ✅ ({time.time()-t1:.1f}s)")
    except Exception as e:
        print(f"  DIM_LOP: ❌ {str(e)[:100]}")
    
    # Tương tự cho các bảng khác...
    # Nếu OPENROWSET không hoạt động → fallback đọc CSV bằng pandas rồi insert
    
    print(f"  ⚠️ OPENROWSET cần SAS token hoặc Managed Identity.")
    print(f"  → Fallback: Đọc CSV từ Blob bằng pandas → executemany")
    
    # Fallback: Đọc CSV từ Blob
    t1 = time.time()
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    client = container.get_blob_client(f"{PROCESSED_PATH}/{FILE_NAME}_parsed.csv")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    import pandas as pd, io
    df = pd.read_csv(io.StringIO(content), sep='|')
    print(f"  Read CSV: {len(df):,} rows ({time.time()-t1:.1f}s)")
    
    # Insert nhanh bằng executemany
    t1 = time.time()
    cur.fast_executemany = True
    
    # DIM_LOP
    lops = df[['MaLop']].dropna().drop_duplicates()
    data = [(r['MaLop'][:50], r['MaLop'][:50], r['MaLop'][:50]) for _,r in lops.iterrows() if str(r['MaLop']).strip()]
    if data: cur.executemany("INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data)
    print(f"  DIM_LOP: {len(data):,}")
    
    # DIM_SV
    svs = df[['MaSV','MaLop']].dropna().drop_duplicates('MaSV')
    data = [(r['MaSV'][:50], '', '', None, r['MaLop'][:50]) for _,r in svs.iterrows() if str(r['MaSV']).strip()]
    if data: cur.executemany("INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data)
    print(f"  DIM_SV: {len(data):,}")
    
    # DIM_GV
    gvs = df[['MaGV']].dropna().drop_duplicates()
    data = [(r['MaGV'][:50], '', '') for _,r in gvs.iterrows() if str(r['MaGV']).strip()]
    if data: cur.executemany("INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data)
    print(f"  DIM_GV: {len(data):,}")
    
    # DIM_LHP
    lhps = df[['MaLopHP','MaHP','MaGV']].dropna().drop_duplicates('MaLopHP')
    data = [(r['MaLopHP'][:100], r['MaLopHP'][:100], r['MaHP'][:50], r['MaGV'][:50], mhk) for _,r in lhps.iterrows() if str(r['MaLopHP']).strip()]
    if data: cur.executemany("INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data)
    print(f"  DIM_LHP: {len(data):,}")
    
    # FACT_GY
    gys = df[df['EssayText'].notna() & (df['EssayText']!='')].drop_duplicates('SubmissionID')
    data = [(r['SubmissionID'][:200], r['MaSV'][:50], r['MaLopHP'][:100], str(r['EssayText'])[:4000],
             r['Sentiment'][:20], int(r['Is_Valid']), int(r['Tag_HocPhan']), int(r['Tag_DayHoc']),
             int(r['Tag_KiemTra']), int(r['Tag_Khac'])) for _,r in gys.iterrows()]
    if data: cur.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data)
    print(f"  FACT_GY: {len(data):,}")
    
    # FACT_KQ
    kqs = []
    for _,r in df[df['CauHoi'].notna() & df['GiaTri'].notna()].iterrows():
        try:
            mc, d = int(float(r['CauHoi'])), int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: kqs.append((str(r['SubmissionID'])[:200], mc, d))
        except: pass
    for _,r in gys.iterrows():
        d = 5 if r['Sentiment']=='POSITIVE' else (2 if r['Sentiment']=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: kqs.append((str(r['SubmissionID'])[:200], mc, d))
    if kqs: cur.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", kqs)
    print(f"  FACT_KQ: {len(kqs):,}")
    
    print(f"  Insert: {time.time()-t1:.1f}s")
    
    # Bật constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    
    conn.close()
    print(f"\n🎉 Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
