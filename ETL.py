#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: BULK INSERT FROM AZURE BLOB - FIXED
- Tạo EXTERNAL DATA SOURCE trước
- Upload CSV lên Azure Blob -> BULK INSERT
"""

import os, sys, re, time, pandas as pd, numpy as np, pyodbc
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu SEMESTER hoặc SURVEY_FILE"); sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=600;Command Timeout=1800;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
PROCESSED_PATH = "processed-data"
NUM_WORKERS = cpu_count()
CHUNK = 100000

# Storage info
parts = dict(p.split('=', 1) for p in CONNECTION_STRING.split(';') if '=' in p)
STORAGE_ACCOUNT = parts.get('AccountName', '')
STORAGE_KEY = parts.get('AccountKey', '')

print("="*70)
print("📊 PIPELINE 2: BULK INSERT FROM AZURE BLOB")
print("="*70)

# ================= PATTERNS =================
_D = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_G = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

# ================= NLP =================
def nlp(t):
    if not t or len(t)<5: return 0,0,0,0,'NEUTRAL',0
    w=set(t.lower().split())
    t1=1 if len(w&{'nội dung','chương trình','học phần','kiến thức','chuẩn','tài liệu'})>=2 else 0
    t2=1 if len(w&{'giảng viên','thầy','cô','dạy','nhiệt tình','tận tâm','dễ hiểu'})>=2 else 0
    t3=1 if len(w&{'kiểm tra','đánh giá','thi','chấm','công bằng','khách quan'})>=2 else 0
    t4=1 if len(w&{'cơ sở','phòng học','máy chiếu','wifi','góp ý','cải thiện'})>=2 else 0
    p=len(w&{'tốt','hay','hài lòng','bổ ích','hiệu quả','tuyệt vời','nhiệt tình'})
    n=len(w&{'tệ','kém','chán','khó hiểu','nhàm chán','thiếu','thất vọng'})
    if 'không' in w: p=max(0,p-1); n+=1
    s='POSITIVE' if p>n else ('NEGATIVE' if n>p else 'NEUTRAL')
    return t1,t2,t3,t4,s,1 if len(t)>10 else 0

# ================= PARSE =================
def parse_batch(args):
    lines, fn = args
    res = []
    for line in lines:
        if not line: continue
        ni = line.find('NULL')
        left = line[:ni].rstrip(', \t') if ni>=0 else line
        right = line[ni+4:].lstrip(', \t') if ni>=0 else ''
        row = left.split(','); rl=len(row)
        if rl<10: continue
        nsi=-1
        for i in range(2,min(12,rl)):
            if _D(row[i].strip()): nsi=i; break
        if nsi==-1: continue
        mgi=-1
        for i in range(nsi+1,min(nsi+25,rl)):
            if _G(row[i].strip()): mgi=i; break
        if mgi==-1: mgi=min(rl-1,nsi+8)
        sv=row[1].strip(); hp=row[nsi+1].strip() if nsi+1<rl else ''
        gv=row[mgi].strip() if mgi<rl else ''
        lhp=row[mgi+3].strip() if mgi+3<rl else f"{hp}_{gv}"
        ch=row[mgi+4].strip() if mgi+4<rl else ''
        gt=row[mgi+5].strip() if mgi+5<rl else ''
        essay=right.replace(' , ',', ').strip()
        t1,t2,t3,t4,sent,valid=nlp(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        sid=f"{sv}_{lhp}_{gv}_{fn}"
        res.append([sid,sv,hp,gv,lhp,ch,gt,essay,t1,t2,t3,t4,sent,valid])
    return res

def parse_survey(content):
    print(f"  -> Parsing..."); t0=time.time()
    lines=[l.strip() for l in content.split('\n') if l.strip()]
    batches=[(lines[i:i+CHUNK],FILE_NAME) for i in range(0,len(lines),CHUNK)]
    all_res=[]
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch,batches): all_res.extend(res)
    df=pd.DataFrame(all_res,columns=['SubmissionID','MaSV','MaHP','MaGV','MaLopHP',
                                      'CauHoi','GiaTri','EssayText',
                                      'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac',
                                      'Sentiment','Is_Valid'])
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= SETUP EXTERNAL DATA SOURCE =================
def setup_external_data_source(cur):
    """Tạo MASTER KEY, CREDENTIAL và EXTERNAL DATA SOURCE"""
    safe_key = STORAGE_KEY.replace("'", "''")
    
    # 1. Tạo MASTER KEY nếu chưa có
    cur.execute("""
        IF NOT EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
        BEGIN
            CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'SurveyDB@2026!'
        END
    """)
    cur.connection.commit()
    
    # 2. Tạo CREDENTIAL
    cur.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.database_scoped_credentials WHERE name = 'AzureBlobCred')
        BEGIN
            CREATE DATABASE SCOPED CREDENTIAL AzureBlobCred
            WITH IDENTITY = 'SHARED ACCESS SIGNATURE',
            SECRET = '{safe_key}'
        END
    """)
    cur.connection.commit()
    
    # 3. Tạo EXTERNAL DATA SOURCE
    cur.execute(f"""
        IF NOT EXISTS (SELECT 1 FROM sys.external_data_sources WHERE name = 'MyAzureBlobStorage')
        BEGIN
            CREATE EXTERNAL DATA SOURCE MyAzureBlobStorage
            WITH (
                TYPE = BLOB_STORAGE,
                LOCATION = 'https://{STORAGE_ACCOUNT}.blob.core.windows.net/{CONTAINER_NAME}',
                CREDENTIAL = AzureBlobCred
            )
        END
    """)
    cur.connection.commit()
    print("  ✅ External Data Source ready")

# ================= UPLOAD + BULK INSERT =================
def upload_and_bulk_insert(blob_service, cur, table, df, columns, blob_name):
    """Upload CSV -> Azure Blob -> BULK INSERT"""
    if df.empty: return 0
    
    df_out = df[columns].copy()
    for c in df_out.columns:
        df_out[c] = df_out[c].astype(str).str.replace('|',' ').str.replace('\n',' ').str.replace('\r',' ').fillna('')
    
    # Upload
    csv_content = df_out.to_csv(index=False, header=False, sep='|', encoding='utf-8')
    blob_path = f"{PROCESSED_PATH}/{blob_name}"
    container_client = blob_service.get_container_client(CONTAINER_NAME)
    container_client.get_blob_client(blob_path).upload_blob(csv_content, overwrite=True)
    
    # ✅ BULK INSERT với full path: processed-data/lop_khaosat_252.csv
    sql = f"""
        BULK INSERT {table}
        FROM '{blob_path}'
        WITH (
            DATA_SOURCE = 'MyAzureBlobStorage',
            FORMAT = 'CSV',
            FIELDTERMINATOR = '|',
            ROWTERMINATOR = '\\n',
            BATCHSIZE = 100000,
            TABLOCK
        )
    """
    cur.execute(sql)
    cur.connection.commit()
    return len(df_out)

def load_all(df, blob_service):
    print("\n💾 BULK INSERT...")
    t0=time.time()
    
    fn=SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc=int(fn[:-1]); hk=int(fn[-1])
    nbd=2000+(yc-1); nkt=nbd+1
    mhk=f"HK{hk}_{nbd%100}{nkt%100}"; nh=f"{nbd}-{nkt}"
    
    de=df[(df['EssayText'].notna())&(df['EssayText']!='')].drop_duplicates('SubmissionID')
    
    kq=[]
    for _,r in df[(df['CauHoi']!='')&(df['GiaTri']!='')].iterrows():
        try:
            mc=int(float(r['CauHoi'])); d=int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: kq.append({'SubmissionID':str(r['SubmissionID'])[:200],'MaCauHoi':mc,'Diem':d})
        except: pass
    for _,r in de.iterrows():
        s=r['Sentiment']; d=5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: kq.append({'SubmissionID':str(r['SubmissionID'])[:200],'MaCauHoi':mc,'Diem':d})
    
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    
    # ✅ Tạo DATA SOURCE trước
    setup_external_data_source(cur)
    
    try:
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
            except: pass
        
        # 1. DIM_HOC_KY
        cur.execute("IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy=?) INSERT INTO DIM_HOC_KY(MaHocKy,NamHoc,HocKy) VALUES(?,?,?)",(mhk,mhk,nh,hk))
        
        # 2. DIM_LOP_SINH_VIEN
        t1=time.time()
        df_lop = df[['MaLopHP']].drop_duplicates().copy()
        df_lop['MaLop'] = df_lop['MaLopHP'].str[:50]
        df_lop['Lop'] = df_lop['MaLopHP'].str[:50]
        df_lop['MaChuyenNganh'] = df_lop['MaLopHP'].str[:50]
        c = upload_and_bulk_insert(blob_service, cur, 'DIM_LOP_SINH_VIEN', df_lop,
                                   ['MaLop','Lop','MaChuyenNganh'], f"lop_{FILE_NAME}.csv")
        print(f"  DIM_LOP: {c:,} ({time.time()-t1:.1f}s)")
        
        # 3. DIM_SINH_VIEN
        t1=time.time()
        df_sv = df[['MaSV','MaLopHP']].drop_duplicates('MaSV').copy()
        df_sv['HoDem'] = ''; df_sv['Ten'] = ''; df_sv['NgaySinh'] = None
        df_sv['MaSV'] = df_sv['MaSV'].str[:50]; df_sv['MaLop'] = df_sv['MaLopHP'].str[:50]
        c = upload_and_bulk_insert(blob_service, cur, 'DIM_SINH_VIEN', df_sv,
                                   ['MaSV','HoDem','Ten','NgaySinh','MaLop'], f"sv_{FILE_NAME}.csv")
        print(f"  DIM_SV: {c:,} ({time.time()-t1:.1f}s)")
        
        # 4. DIM_GIANG_VIEN
        t1=time.time()
        df_gv = df[['MaGV']].drop_duplicates().copy()
        df_gv['HoDemGV'] = ''; df_gv['TenGV'] = ''; df_gv['MaGV'] = df_gv['MaGV'].str[:50]
        c = upload_and_bulk_insert(blob_service, cur, 'DIM_GIANG_VIEN', df_gv,
                                   ['MaGV','HoDemGV','TenGV'], f"gv_{FILE_NAME}.csv")
        print(f"  DIM_GV: {c:,} ({time.time()-t1:.1f}s)")
        
        # 5. DIM_LOP_HOC_PHAN
        t1=time.time()
        df_lhp = df[['MaLopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').copy()
        df_lhp['LopHP'] = df_lhp['MaLopHP'].str[:100]
        df_lhp['MaLopHP'] = df_lhp['MaLopHP'].str[:100]
        df_lhp['MaHP'] = df_lhp['MaHP'].str[:50]; df_lhp['MaGV'] = df_lhp['MaGV'].str[:50]
        df_lhp['MaHocKy'] = mhk
        c = upload_and_bulk_insert(blob_service, cur, 'DIM_LOP_HOC_PHAN', df_lhp,
                                   ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], f"lhp_{FILE_NAME}.csv")
        print(f"  DIM_LHP: {c:,} ({time.time()-t1:.1f}s)")
        
        # 6. FACT_GOP_Y
        t1=time.time()
        if not de.empty:
            df_gy = de[['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid',
                         'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']].copy()
            df_gy.columns = ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                              'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
            df_gy['SubmissionID'] = df_gy['SubmissionID'].str[:200]
            df_gy['MaSV'] = df_gy['MaSV'].str[:50]; df_gy['MaLopHP'] = df_gy['MaLopHP'].str[:100]
            df_gy['NoiDungGopY'] = df_gy['NoiDungGopY'].str[:4000]
            df_gy['Sentiment'] = df_gy['Sentiment'].str[:20]
            c = upload_and_bulk_insert(blob_service, cur, 'FACT_GOP_Y_TU_LUAN', df_gy,
                                       ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                                        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac'], f"gy_{FILE_NAME}.csv")
            print(f"  FACT_GY: {c:,} ({time.time()-t1:.1f}s)")
        
        # 7. FACT_KET_QUA
        t1=time.time()
        if kq:
            df_kq = pd.DataFrame(kq)
            c = upload_and_bulk_insert(blob_service, cur, 'FACT_KET_QUA_DANH_GIA', df_kq,
                                       ['SubmissionID','MaCauHoi','Diem'], f"kq_{FILE_NAME}.csv")
            print(f"  FACT_KQ: {c:,} ({time.time()-t1:.1f}s)")
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
            except: pass
        conn.close()
    
    print(f"  ⏱️ Total: {time.time()-t0:.1f}s")

def main():
    t0=time.time()
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob_service.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    df = parse_survey(content)
    if df.empty: print("❌"); return
    load_all(df, blob_service)
    print(f"\n🎉 Done: {time.time()-t0:.1f}s | {len(df):,} rows")

if __name__=="__main__":
    main()
