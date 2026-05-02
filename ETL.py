#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: ULTRA FAST
- Load existing IDs 1 lần (1 query)
- Lọc trong Python (set lookup O(1))
- INSERT thẳng không WHERE
- 1 transaction, 1 commit
"""

import os, sys, re, time, pandas as pd, pyodbc
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient

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

print("="*70)
print("📊 PIPELINE 2: ULTRA FAST (Python filter)")
print(f"   Workers: {NUM_WORKERS}")
print("="*70)

# ================= PATTERNS =================
_D = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_G = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

# ================= NLP =================
def nlp(t):
    if not t or len(t)<5: return 0,0,0,0,'NEUTRAL',0
    w=set(t.lower().split())
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
    print(f"  -> {len(lines):,} lines")
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
    print("\n💾 LOAD (Python filter)...")
    t0=time.time()
    
    fn=SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc=int(fn[:-1]); hk=int(fn[-1])
    nbd=2000+(yc-1); nkt=nbd+1
    mhk=f"HK{hk}_{nbd%100}{nkt%100}"; nh=f"{nbd}-{nkt}"
    
    de=df[(df['EssayText'].notna())&(df['EssayText']!='')].drop_duplicates('SubmissionID')
    
    # Chuẩn bị FACT_KET_QUA
    kq=[]
    for _,r in df[(df['CauHoi']!='')&(df['GiaTri']!='')].iterrows():
        try:
            mc=int(float(r['CauHoi'])); d=int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: kq.append((str(r['SubmissionID'])[:150],mc,d))
        except: pass
    for _,r in de.iterrows():
        s=r['Sentiment']; d=5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: kq.append((str(r['SubmissionID'])[:150],mc,d))
    
    conn=pyodbc.connect(CONN_STR, autocommit=False)
    cur=conn.cursor()
    cur.fast_executemany=True
    
    # ===== BƯỚC 1: LOAD EXISTING IDs (5 query, <1s) =====
    print("  -> Loading existing IDs...")
    t1=time.time()
    
    cur.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {r[0] for r in cur.fetchall()}
    
    cur.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {r[0] for r in cur.fetchall()}
    
    cur.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_gv = {r[0] for r in cur.fetchall()}
    
    cur.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {r[0] for r in cur.fetchall()}
    
    cur.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", (mhk,))
    existing_hk = cur.fetchone() is not None
    
    print(f"  ✅ Loaded IDs ({time.time()-t1:.1f}s)")
    print(f"     Lop={len(existing_lop):,} SV={len(existing_sv):,} GV={len(existing_gv):,} LHP={len(existing_lhp):,} HK={existing_hk}")
    
    # ===== BƯỚC 2: LỌC TRONG PYTHON (set lookup O(1)) =====
    t1=time.time()
    
    # DIM_HOC_KY
    if not existing_hk:
        cur.execute("INSERT INTO DIM_HOC_KY(MaHocKy,NamHoc,HocKy) VALUES(?,?,?)",(mhk,nh,hk))
    
    # DIM_LOP_SINH_VIEN
    lops=df[['MaLopHP']].drop_duplicates().fillna('')
    new_lop=[(str(r['MaLopHP'])[:20],str(r['MaLopHP'])[:50],str(r['MaLopHP'])[:20])
             for _,r in lops.iterrows()
             if str(r['MaLopHP']).strip() and str(r['MaLopHP'])[:20] not in existing_lop]
    
    # DIM_SINH_VIEN
    svs=df[['MaSV']].drop_duplicates().fillna('')
    new_sv=[(str(r['MaSV'])[:20],'','',None,str(r['MaSV'])[:20])
            for _,r in svs.iterrows()
            if str(r['MaSV']).strip() and str(r['MaSV'])[:20] not in existing_sv]
    
    # DIM_GIANG_VIEN
    gvs=df[['MaGV']].drop_duplicates().fillna('')
    new_gv=[(str(r['MaGV'])[:20],'','')
            for _,r in gvs.iterrows()
            if str(r['MaGV']).strip() and str(r['MaGV'])[:20] not in existing_gv]
    
    # DIM_LOP_HOC_PHAN
    lhps=df[['MaLopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    new_lhp=[(str(r['MaLopHP'])[:50],str(r['MaLopHP'])[:100],str(r['MaHP'])[:20],str(r['MaGV'])[:20],mhk)
             for _,r in lhps.iterrows()
             if str(r['MaLopHP']).strip() and str(r['MaLopHP'])[:50] not in existing_lhp]
    
    print(f"  ✅ Filtered in Python ({time.time()-t1:.1f}s)")
    print(f"     New: Lop={len(new_lop):,} SV={len(new_sv):,} GV={len(new_gv):,} LHP={len(new_lhp):,}")
    
    try:
        # Tắt constraint
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
            except: pass
        
        # ===== BƯỚC 3: INSERT THẲNG (không WHERE) =====
        t1=time.time()
        
        if new_lop:
            cur.executemany("INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)",new_lop)
        print(f"  DIM_LOP: {len(new_lop):,} ({time.time()-t1:.1f}s)")
        
        t1=time.time()
        if new_sv:
            cur.executemany("INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)",new_sv)
        print(f"  DIM_SV: {len(new_sv):,} ({time.time()-t1:.1f}s)")
        
        t1=time.time()
        if new_gv:
            cur.executemany("INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)",new_gv)
        print(f"  DIM_GV: {len(new_gv):,} ({time.time()-t1:.1f}s)")
        
        t1=time.time()
        if new_lhp:
            cur.executemany("INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)",new_lhp)
        print(f"  DIM_LHP: {len(new_lhp):,} ({time.time()-t1:.1f}s)")
        
        # ===== FACT: INSERT THẲNG =====
        t1=time.time()
        if not de.empty:
            data=[(str(r['SubmissionID'])[:150],str(r['MaSV'])[:20],str(r['MaLopHP'])[:50],
                   str(r['EssayText'])[:4000],str(r['Sentiment'])[:20],int(r['Is_Valid']),
                   int(r['Tag_HocPhan']),int(r['Tag_DayHoc']),int(r['Tag_KiemTra']),int(r['Tag_Khac']))
                  for _,r in de.iterrows()]
            cur.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)",data)
        print(f"  FACT_GY: {len(data):,} ({time.time()-t1:.1f}s)")
        
        t1=time.time()
        if kq:
            cur.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)",kq)
        print(f"  FACT_KQ: {len(kq):,} ({time.time()-t1:.1f}s)")
        
        # COMMIT
        conn.commit()
        print(f"  ✅ COMMIT OK!")
        
    except Exception as e:
        conn.rollback()
        print(f"  ❌ {e}")
    finally:
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL"); conn.commit()
            except: pass
        conn.close()
    
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
