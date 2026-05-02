#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - ULTRA FAST
- Parse + Insert thẳng, không validation
- Tắt constraint, index
- 1 transaction duy nhất
"""

import os, sys, re, time, pandas as pd, numpy as np, pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from multiprocessing import Pool, cpu_count

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

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
NUM_WORKERS = cpu_count()
CHUNK = 100000
BATCH = 100000

print("="*70)
print("📊 PIPELINE 2: ULTRA FAST")
print(f"   Workers: {NUM_WORKERS} | Chunk: {CHUNK:,} | Batch: {BATCH:,}")
print("="*70)

# ================= PATTERNS =================
_D = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_G = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match
_L = re.compile(r'^\d{2}K\d{2}$').match

# ================= NLP (tối giản) =================
def nlp(t):
    if not t or len(t)<5: return 0,0,0,0,'NEUTRAL',0
    w = set(t.lower().split())
    t1=1 if len(w&{'nội dung','chương trình','học phần','kiến thức','chuẩn','tài liệu','giáo trình','thực hành','phù hợp','bổ ích'})>=2 else 0
    t2=1 if len(w&{'giảng viên','thầy','cô','dạy','giảng','nhiệt tình','tận tâm','dễ hiểu','sinh động','thú vị'})>=2 else 0
    t3=1 if len(w&{'kiểm tra','đánh giá','thi','đề thi','chấm','công bằng','khách quan'})>=2 else 0
    t4=1 if len(w&{'cơ sở','phòng học','máy chiếu','wifi','hỗ trợ','góp ý','cải thiện'})>=2 else 0
    p=len(w&{'tốt','hay','hài lòng','thích','bổ ích','hiệu quả','chất lượng','tuyệt vời','nhiệt tình','dễ hiểu','công bằng'})
    n=len(w&{'tệ','kém','chán','không tốt','khó hiểu','nhàm chán','thiếu','hạn chế','thất vọng'})
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
        
        # Find date
        nsi=-1
        for i in range(2,min(12,rl)):
            if _D(row[i].strip()): nsi=i; break
        if nsi==-1: continue
        
        # Find MaGV
        mgi=-1
        for i in range(nsi+1,min(nsi+25,rl)):
            if _G(row[i].strip()): mgi=i; break
        if mgi==-1: mgi=min(rl-1,nsi+8)
        
        lop=row[0].strip(); sv=row[1].strip(); ns=row[nsi].strip()
        np=[x.strip() for x in row[2:nsi]]
        ten=np[-1] if np else ''; hd=' '.join(np[:-1]) if len(np)>1 else ''
        hp=row[nsi+1].strip() if nsi+1<rl else ''
        gv=row[mgi].strip() if mgi<rl else ''
        lhp=row[mgi+3].strip() if mgi+3<rl else f"{hp}_{gv}"
        ch=row[mgi+4].strip() if mgi+4<rl else ''
        gt=row[mgi+5].strip() if mgi+5<rl else ''
        essay=right.replace(' , ',', ').strip()
        t1,t2,t3,t4,sent,valid=nlp(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        
        # MaLop
        ml=lop.strip()
        if ml.upper().startswith('CTS-'): ml=ml[4:]
        for sep in ['.','-','_']:
            if sep in ml: ml=ml.split(sep)[0]
        
        # MaChuyenNganh
        lu=lop.upper()
        if 'ACCA' in lu:
            m=re.search(r'K(\d{2})',lu); mcn=f"K{m.group(1)}-ACCA" if m else lop
        elif 'CTS' in lu: mcn='CTS'
        elif 'QT' in lu: mcn='QT'
        else:
            m=re.search(r'K(\d{2})',lu); mcn=f"K{m.group(1)}" if m else ml
        
        sid=f"{sv}_{lhp}_{gv}_{fn}"
        res.append([sid,sv,hd,ten,ns,ml,lop,mcn,hp,gv,lhp,ch,gt,essay,t1,t2,t3,t4,sent,valid])
    return res

def parse_survey(content):
    print(f"  -> Parsing..."); t0=time.time()
    lines=[l.strip() for l in content.split('\n') if l.strip()]
    print(f"  -> {len(lines):,} lines")
    batches=[(lines[i:i+CHUNK],FILE_NAME) for i in range(0,len(lines),CHUNK)]
    all_res=[]
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch,batches): all_res.extend(res)
    df=pd.DataFrame(all_res,columns=['SubmissionID','MaSV','HoDem','Ten','NgaySinh','MaLop','Lop','MaChuyenNganh',
                                      'MaHP','MaGV','MaLopHP','CauHoi','GiaTri','EssayText',
                                      'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid'])
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= LOAD (SIÊU TỐC) =================
def load_all(df):
    print("\n💾 LOAD TO DATABASE")
    t0=time.time()
    
    mhk,nh,hk=derive_ma_hoc_ky()
    
    # Chuẩn bị data
    # DIMs
    d_lop=df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop').fillna('')
    d_sv=df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV').fillna('')
    d_gv=df[['MaGV']].drop_duplicates('MaGV').fillna('')
    d_gv['HoDemGV']=''; d_gv['TenGV']=''
    d_lhp=df[['MaLopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    d_lhp['LopHP']=d_lhp['MaLopHP']; d_lhp['MaHocKy']=mhk
    
    # Facts
    de=df[(df['EssayText'].notna())&(df['EssayText']!='')].drop_duplicates('SubmissionID')
    d_gy=de[['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid',
             'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']].copy()
    d_gy.columns=['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                  'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
    
    d_kq=[]
    for _,r in df[(df['CauHoi']!='')&(df['GiaTri']!='')].iterrows():
        try:
            mc=int(float(r['CauHoi'])); d=int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: d_kq.append((str(r['SubmissionID'])[:150],mc,d))
        except: pass
    for _,r in de.iterrows():
        s=r['Sentiment']; d=5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: d_kq.append((str(r['SubmissionID'])[:150],mc,d))
    
    conn=pyodbc.connect(CONN_STR)
    cur=conn.cursor()
    cur.fast_executemany=True
    
    # Tắt constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
              'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL"); conn.commit()
        except: pass
    
    def ins(table,cols,data):
        if not data: return 0
        ph=','.join(['?']*len(cols))
        sql=f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph})"
        ins_count=0
        for i in range(0,len(data),BATCH):
            batch=data[i:i+BATCH]
            try:
                cur.executemany(sql,batch); conn.commit(); ins_count+=len(batch)
            except:
                for d in batch:
                    try: cur.execute(sql,d); conn.commit(); ins_count+=1
                    except: pass
        return ins_count
    
    # Insert
    t1=time.time()
    
    # DIM_HOC_KY
    ins('DIM_HOC_KY',['MaHocKy','NamHoc','HocKy'],[(mhk,nh,hk)])
    
    # DIM_LOP_SINH_VIEN
    data=[(str(r['MaLop'])[:20],str(r['Lop'])[:50],str(r['MaChuyenNganh'])[:20]) 
          for _,r in d_lop.iterrows() if str(r['MaLop']).strip()]
    print(f"  DIM_LOP: {ins('DIM_LOP_SINH_VIEN',['MaLop','Lop','MaChuyenNganh'],data):,} ({time.time()-t1:.1f}s)")
    t1=time.time()
    
    # DIM_SINH_VIEN
    data=[]
    for _,r in d_sv.iterrows():
        if not str(r['MaSV']).strip(): continue
        try: ns=pd.to_datetime(r['NgaySinh'],format='%d/%m/%Y').strftime('%Y-%m-%d')
        except: ns=None
        data.append((str(r['MaSV'])[:20],str(r['HoDem'])[:100],str(r['Ten'])[:50],ns,str(r['MaLop'])[:20]))
    print(f"  DIM_SV: {ins('DIM_SINH_VIEN',['MaSV','HoDem','Ten','NgaySinh','MaLop'],data):,} ({time.time()-t1:.1f}s)")
    t1=time.time()
    
    # DIM_GIANG_VIEN
    data=[(str(r['MaGV'])[:20],str(r['HoDemGV'])[:100],str(r['TenGV'])[:50]) 
          for _,r in d_gv.iterrows() if str(r['MaGV']).strip()]
    print(f"  DIM_GV: {ins('DIM_GIANG_VIEN',['MaGV','HoDemGV','TenGV'],data):,} ({time.time()-t1:.1f}s)")
    t1=time.time()
    
    # DIM_LOP_HOC_PHAN
    data=[(str(r['MaLopHP'])[:50],str(r['LopHP'])[:100],str(r['MaHP'])[:20],str(r['MaGV'])[:20],mhk) 
          for _,r in d_lhp.iterrows() if str(r['MaLopHP']).strip()]
    print(f"  DIM_LHP: {ins('DIM_LOP_HOC_PHAN',['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'],data):,} ({time.time()-t1:.1f}s)")
    t1=time.time()
    
    # FACT_GOP_Y
    data=[]
    for _,r in d_gy.iterrows():
        data.append((str(r['SubmissionID'])[:150],str(r['MaSV'])[:20],str(r['MaLopHP'])[:50],
                     str(r['NoiDungGopY'])[:4000],str(r['Sentiment'])[:20],int(r['Is_Valid']),
                     int(r['Tag_HocPhan']),int(r['Tag_DayHoc']),int(r['Tag_KiemTra']),int(r['Tag_Khac'])))
    print(f"  FACT_GY: {ins('FACT_GOP_Y_TU_LUAN',['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac'],data):,} ({time.time()-t1:.1f}s)")
    t1=time.time()
    
    # FACT_KET_QUA
    print(f"  FACT_KQ: {ins('FACT_KET_QUA_DANH_GIA',['SubmissionID','MaCauHoi','Diem'],d_kq):,} ({time.time()-t1:.1f}s)")
    
    # Bật constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
              'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL"); conn.commit()
        except: pass
    
    conn.close()
    print(f"  ✅ Total load: {time.time()-t0:.1f}s")

def derive_ma_hoc_ky():
    fn=SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc=int(fn[:-1]); hk=int(fn[-1])
    nbd=2000+(yc-1); nkt=nbd+1
    return f"HK{hk}_{nbd%100}{nkt%100}",f"{nbd}-{nkt}",hk

# ================= MAIN =================
def main():
    t0=time.time()
    
    blob=BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client=blob.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content=client.download_blob().readall().decode('utf-8-sig')
    print(f"📥 Downloaded: {len(content):,} chars")
    
    df=parse_survey(content)
    if df.empty: print("❌ No data!"); return
    
    df.to_parquet(f"/tmp/{FILE_NAME}.parquet",index=False)
    load_all(df)
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__=="__main__":
    main()
