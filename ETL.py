#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BƯỚC 2: LOAD CSV TỪ AZURE BLOB VÀO DATABASE (FIXED)
"""
import os, sys, re, time, pyodbc, pandas as pd, io
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

def safe_str(val, max_len=None):
    """Chuyển về string an toàn, cắt độ dài"""
    if pd.isna(val): return ''
    s = str(val).strip()
    if max_len: s = s[:max_len]
    return s

def main():
    t0 = time.time()
    print("="*50, "\n📊 BƯỚC 2: LOAD CSV → DATABASE\n", "="*50)
    
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    nh = f"{nbd}-{nbd+1}"
    
    # Đọc CSV từ Azure Blob
    t1 = time.time()
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    client = container.get_blob_client(f"{PROCESSED_PATH}/{FILE_NAME}_parsed.csv")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    # ✅ Đọc với dtype=str để tránh lỗi mixed types
    df = pd.read_csv(io.StringIO(content), sep='|', dtype=str, keep_default_na=False)
    df = df.fillna('')
    print(f"  Read CSV: {len(df):,} rows ({time.time()-t1:.1f}s)")
    print(f"  Columns: {list(df.columns)}")
    
    # Kết nối DB
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    cur.fast_executemany = True
    
    # Tắt constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
               'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    
    # DIM_HOC_KY
    cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nh}',{hk})")
    
    t1 = time.time()
    
    # DIM_LOP
    lops = set()
    data_lop = []
    for v in df['MaLop']:
        v = safe_str(v, 50)
        if v and v not in lops:
            lops.add(v)
            data_lop.append((v, v, v))
    if data_lop: cur.executemany("INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data_lop)
    print(f"  DIM_LOP: {len(data_lop):,}")
    
    # DIM_SV
    svs = {}
    for _, r in df.iterrows():
        sv = safe_str(r['MaSV'], 50)
        ml = safe_str(r['MaLop'], 50)
        if sv and sv not in svs:
            svs[sv] = ml
    data_sv = [(sv, '', '', None, ml) for sv, ml in svs.items()]
    if data_sv: cur.executemany("INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data_sv)
    print(f"  DIM_SV: {len(data_sv):,}")
    
    # DIM_GV
    gvs = set()
    data_gv = []
    for v in df['MaGV']:
        v = safe_str(v, 50)
        if v and v not in gvs:
            gvs.add(v)
            data_gv.append((v, '', ''))
    if data_gv: cur.executemany("INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data_gv)
    print(f"  DIM_GV: {len(data_gv):,}")
    
    # DIM_LHP
    lhps = {}
    for _, r in df.iterrows():
        lhp = safe_str(r['MaLopHP'], 100)
        hp = safe_str(r['MaHP'], 50)
        gv = safe_str(r['MaGV'], 50)
        if lhp and lhp not in lhps:
            lhps[lhp] = (hp, gv)
    data_lhp = [(lhp, lhp, hp, gv, mhk) for lhp, (hp, gv) in lhps.items()]
    if data_lhp: cur.executemany("INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data_lhp)
    print(f"  DIM_LHP: {len(data_lhp):,}")
    
    # FACT_GY
    seen_gy = set()
    data_gy = []
    for _, r in df.iterrows():
        essay = safe_str(r['EssayText'])
        if not essay: continue
        sid = safe_str(r['SubmissionID'], 200)
        if sid in seen_gy: continue
        seen_gy.add(sid)
        data_gy.append((
            sid,
            safe_str(r['MaSV'], 50),
            safe_str(r['MaLopHP'], 100),
            essay[:4000],
            safe_str(r['Sentiment'], 20) or 'NEUTRAL',
            int(float(r['Is_Valid'])) if r['Is_Valid'] else 0,
            int(float(r['Tag_HocPhan'])) if r['Tag_HocPhan'] else 0,
            int(float(r['Tag_DayHoc'])) if r['Tag_DayHoc'] else 0,
            int(float(r['Tag_KiemTra'])) if r['Tag_KiemTra'] else 0,
            int(float(r['Tag_Khac'])) if r['Tag_Khac'] else 0,
        ))
    if data_gy: cur.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data_gy)
    print(f"  FACT_GY: {len(data_gy):,}")
    
    # FACT_KQ
    data_kq = []
    for _, r in df.iterrows():
        sid = safe_str(r['SubmissionID'], 200)
        ch = r['CauHoi']
        gt = r['GiaTri']
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    data_kq.append((sid, mc, d))
            except: pass
    
    for _, r in df.iterrows():
        essay = safe_str(r['EssayText'])
        if not essay: continue
        sid = safe_str(r['SubmissionID'], 200)
        sent = safe_str(r['Sentiment'], 20)
        d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
        for mc in [13, 14, 15, 16]:
            data_kq.append((sid, mc, d))
    
    # Insert KQ theo batch
    BATCH = 50000
    for i in range(0, len(data_kq), BATCH):
        batch = data_kq[i:i+BATCH]
        cur.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", batch)
    print(f"  FACT_KQ: {len(data_kq):,}")
    
    print(f"  Insert: {time.time()-t1:.1f}s")
    
    # Bật constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    
    conn.close()
    print(f"\n🎉 Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
