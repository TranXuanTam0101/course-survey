#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: ĐÚNG - CHỈ LOAD CÁC BẢNG CÒN LẠI
- DIM_HOC_KY, DIM_LOP_SINH_VIEN, DIM_SINH_VIEN, DIM_GIANG_VIEN, DIM_LOP_HOC_PHAN
- FACT_GOP_Y_TU_LUAN, FACT_KET_QUA_DANH_GIA
- KHÔNG load DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH, DIM_HOC_PHAN (Pipeline 1 đã làm)
- MaLop = Lop gốc (cột đầu tiên trong CSV), MaLopHP = LopHP
"""
import os, sys, re, time, pandas as pd, pyodbc
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;UID=sqladmin;PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=600;Command Timeout=1800;LongAsMax=yes;"
)
CONTAINER_NAME, RAWDATA_PATH = SEMESTER, "rawdata"
NUM_WORKERS, CHUNK, BATCH = cpu_count(), 100000, 50000

_D = re.compile(r'^\d{2}/\d{2}/\d{4}$').match
_G = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$').match

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
    return t1,t2,t3,t4,'POSITIVE' if p>n else ('NEGATIVE' if n>p else 'NEUTRAL'),1

def normalize_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.','-','_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def parse_batch(args):
    lines, fn = args
    res = []
    for line in lines:
        if not line: continue
        ni = line.find('NULL')
        left = line[:ni].rstrip(', \t') if ni>=0 else line
        right = line[ni+4:].lstrip(', \t') if ni>=0 else ''
        row = left.split(','); rl = len(row)
        if rl < 10: continue
        nsi = -1
        for i in range(2, min(12, rl)):
            if _D(row[i].strip()): nsi = i; break
        if nsi == -1: continue
        mgi = -1
        for i in range(nsi+1, min(nsi+25, rl)):
            if _G(row[i].strip()): mgi = i; break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        sv = row[1].strip()
        ml = normalize_lop(row[0].strip())  # MaLop = Lop gốc chuẩn hóa
        hp = row[nsi+1].strip() if nsi+1 < rl else ''
        gv = row[mgi].strip() if mgi < rl else ''
        lhp = row[mgi+3].strip() if mgi+3 < rl else f"{hp}_{gv}"
        ch = row[mgi+4].strip() if mgi+4 < rl else ''
        gt = row[mgi+5].strip() if mgi+5 < rl else ''
        essay = right.replace(' , ', ', ').strip()
        t1v,t2v,t3v,t4v,sent,valid = nlp(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        sid = f"{sv}_{lhp}_{gv}_{fn}"
        res.append((sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1v, t2v, t3v, t4v, sent, valid))
    return res

def main():
    t0 = time.time()
    print("="*60, "\n📊 PIPELINE 2: ĐÚNG\n", "="*60)
    
    # Download & Parse
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    t1 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK], FILE_NAME) for i in range(0, len(lines), CHUNK)]
    all_res = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches): all_res.extend(res)
    print(f"  Parse: {len(all_res):,} rows ({time.time()-t1:.1f}s)")
    
    # Chuẩn bị data
    t1 = time.time()
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    
    seen_lop, seen_sv, seen_gv, seen_lhp, seen_gy = set(), set(), set(), set(), set()
    data_lop, data_sv, data_gv, data_lhp, data_gy, data_kq = [], [], [], [], [], []
    
    for r in all_res:
        sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1v, t2v, t3v, t4v, sent, valid = r
        
        # DIM_LOP: MaLop = Lop gốc
        if ml and ml not in seen_lop:
            seen_lop.add(ml)
            data_lop.append((ml[:50], ml[:50], ml[:50]))
        
        # DIM_SV: MaLop = Lop gốc
        if sv and sv not in seen_sv:
            seen_sv.add(sv)
            data_sv.append((sv[:50], '', '', None, ml[:50]))
        
        # DIM_GV
        if gv and gv not in seen_gv:
            seen_gv.add(gv)
            data_gv.append((gv[:50], '', ''))
        
        # DIM_LHP: MaLopHP = LopHP (từ cột LopHP trong CSV)
        if lhp and lhp not in seen_lhp:
            seen_lhp.add(lhp)
            data_lhp.append((lhp[:100], lhp[:100], hp[:50], gv[:50], mhk))
        
        # FACT_GY
        if essay and len(essay.strip()) > 0 and sid not in seen_gy:
            seen_gy.add(sid)
            data_gy.append((sid[:200], sv[:50], lhp[:100], essay[:4000], sent[:20], valid, t1v, t2v, t3v, t4v))
            d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
            for mc in [13,14,15,16]:
                data_kq.append((sid[:200], mc, d))
        
        # FACT_KQ - trắc nghiệm
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    data_kq.append((sid[:200], mc, d))
            except: pass
    
    print(f"  Prepare: LOP={len(data_lop)} SV={len(data_sv)} GV={len(data_gv)} LHP={len(data_lhp)} GY={len(data_gy)} KQ={len(data_kq)} ({time.time()-t1:.1f}s)")
    
    # INSERT
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR, autocommit=False)
    cur = conn.cursor()
    cur.fast_executemany = True
    
    try:
        cur.execute("BEGIN TRANSACTION")
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
            except: pass
        
        cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nbd}-{nbd+1}',{hk})")
        
        if data_lop: cur.executemany("INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data_lop)
        if data_sv:  cur.executemany("INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data_sv)
        if data_gv:  cur.executemany("INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data_gv)
        if data_lhp: cur.executemany("INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data_lhp)
        if data_gy:  cur.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data_gy)
        if data_kq:  cur.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", data_kq)
        
        cur.execute("COMMIT")
        print(f"  ✅ COMMIT ({time.time()-t1:.1f}s)")
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"  ❌ {e}")
    finally:
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL"); conn.commit()
            except: pass
        conn.close()
    
    print(f"\n🎉 Total: {time.time()-t0:.1f}s | {len(all_res):,} rows")

if __name__ == "__main__":
    main()
