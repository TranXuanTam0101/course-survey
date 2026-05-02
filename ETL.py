#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: PANDAS TO_SQL - FIXED
- chunksize=500 để tránh lỗi 2100 params
"""

import os, sys, re, time, pandas as pd, numpy as np
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient
from sqlalchemy import create_engine
import urllib.parse

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

pwd = urllib.parse.quote_plus(DB_PASSWORD)

ENGINE_STR = (
    f"mssql+pyodbc://sqladmin:{pwd}@course-survey.database.windows.net:1433/"
    f"course-survey-db?driver=ODBC+Driver+18+for+SQL+Server"
    f"&Encrypt=yes&TrustServerCertificate=no"
    f"&Connection+Timeout=600&Command+Timeout=1800"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
NUM_WORKERS = cpu_count()
CHUNK = 100000

# Chunksize an toàn: số cột × chunksize < 2100
CS_DIM = 500    # 3-5 cột × 500 = 1500-2500
CS_FACT = 200   # 10 cột × 200 = 2000

print("="*70)
print("📊 PIPELINE 2: PANDAS TO_SQL (FIXED)")
print(f"   Workers: {NUM_WORKERS} | Chunk DIM={CS_DIM} FACT={CS_FACT}")
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

# ================= LOAD =================
def load_all(df):
    print("\n💾 LOAD (to_sql)...")
    t0=time.time()
    
    fn=SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc=int(fn[:-1]); hk=int(fn[-1])
    nbd=2000+(yc-1); nkt=nbd+1
    mhk=f"HK{hk}_{nbd%100}{nkt%100}"; nh=f"{nbd}-{nkt}"
    
    engine = create_engine(ENGINE_STR, fast_executemany=True)
    
    # 1. DIM_HOC_KY
    t1=time.time()
    pd.DataFrame([{'MaHocKy':mhk,'NamHoc':nh,'HocKy':hk}]).to_sql('DIM_HOC_KY',engine,if_exists='append',index=False,method='multi')
    print(f"  DIM_HOC_KY ({time.time()-t1:.1f}s)")
    
    # 2. DIM_LOP_SINH_VIEN
    t1=time.time()
    df_lop=df[['MaLopHP']].drop_duplicates().copy()
    df_lop.columns=['MaLop']; df_lop['Lop']=df_lop['MaLop']; df_lop['MaChuyenNganh']=df_lop['MaLop']
    df_lop=df_lop[df_lop['MaLop']!='']
    df_lop.to_sql('DIM_LOP_SINH_VIEN',engine,if_exists='append',index=False,method='multi',chunksize=CS_DIM)
    print(f"  DIM_LOP: {len(df_lop):,} ({time.time()-t1:.1f}s)")
    
    # 3. DIM_SINH_VIEN
    t1=time.time()
    df_sv=df[['MaSV']].drop_duplicates().copy()
    df_sv['HoDem']=''; df_sv['Ten']=''; df_sv['NgaySinh']=None; df_sv['MaLop']=df_sv['MaSV']
    df_sv=df_sv[df_sv['MaSV']!='']
    df_sv.to_sql('DIM_SINH_VIEN',engine,if_exists='append',index=False,method='multi',chunksize=CS_DIM)
    print(f"  DIM_SV: {len(df_sv):,} ({time.time()-t1:.1f}s)")
    
    # 4. DIM_GIANG_VIEN
    t1=time.time()
    df_gv=df[['MaGV']].drop_duplicates().copy()
    df_gv['HoDemGV']=''; df_gv['TenGV']=''
    df_gv=df_gv[df_gv['MaGV']!='']
    df_gv.to_sql('DIM_GIANG_VIEN',engine,if_exists='append',index=False,method='multi',chunksize=CS_DIM)
    print(f"  DIM_GV: {len(df_gv):,} ({time.time()-t1:.1f}s)")
    
    # 5. DIM_LOP_HOC_PHAN
    t1=time.time()
    df_lhp=df[['MaLopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').copy()
    df_lhp['LopHP']=df_lhp['MaLopHP']; df_lhp['MaHocKy']=mhk
    df_lhp=df_lhp[df_lhp['MaLopHP']!='']
    df_lhp.to_sql('DIM_LOP_HOC_PHAN',engine,if_exists='append',index=False,method='multi',chunksize=CS_DIM)
    print(f"  DIM_LHP: {len(df_lhp):,} ({time.time()-t1:.1f}s)")
    
    # 6. FACT_GOP_Y_TU_LUAN
    t1=time.time()
    df_gy=df[(df['EssayText'].notna())&(df['EssayText']!='')].drop_duplicates('SubmissionID').copy()
    df_gy=df_gy[['SubmissionID','MaSV','MaLopHP','EssayText','Sentiment','Is_Valid',
                   'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']]
    df_gy.columns=['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid',
                    'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']
    df_gy['NoiDungGopY']=df_gy['NoiDungGopY'].str[:4000]
    df_gy.to_sql('FACT_GOP_Y_TU_LUAN',engine,if_exists='append',index=False,method='multi',chunksize=CS_FACT)
    print(f"  FACT_GY: {len(df_gy):,} ({time.time()-t1:.1f}s)")
    
    # 7. FACT_KET_QUA_DANH_GIA
    t1=time.time()
    rows=[]
    for _,r in df[(df['CauHoi']!='')&(df['GiaTri']!='')].iterrows():
        try:
            mc=int(float(r['CauHoi'])); d=int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: rows.append({'SubmissionID':str(r['SubmissionID'])[:150],'MaCauHoi':mc,'Diem':d})
        except: pass
    for _,r in df_gy.iterrows():
        s=r['Sentiment']; d=5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: rows.append({'SubmissionID':str(r['SubmissionID'])[:150],'MaCauHoi':mc,'Diem':d})
    if rows:
        df_kq=pd.DataFrame(rows)
        df_kq.to_sql('FACT_KET_QUA_DANH_GIA',engine,if_exists='append',index=False,method='multi',chunksize=CS_DIM)
    print(f"  FACT_KQ: {len(rows):,} ({time.time()-t1:.1f}s)")
    
    engine.dispose()
    print(f"  ⏱️ Total load: {time.time()-t0:.1f}s")

# ================= MAIN =================
def main():
    t0=time.time()
    blob=BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client=blob.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content=client.download_blob().readall().decode('utf-8-sig')
    df=parse_survey(content)
    if df.empty: print("❌ No data!"); return
    load_all(df)
    print(f"\n🎉 Total: {time.time()-t0:.1f}s | {len(df):,} rows")

if __name__=="__main__":
    main()
