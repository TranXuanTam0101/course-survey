#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: PARSE & INSERT TRỰC TIẾP
- Parse → Insert từng batch vào DB ngay
- Không lưu file trung gian
"""
import os, sys, re, time, pyodbc
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
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=600;Command Timeout=1800;"
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
        ml = normalize_lop(row[0].strip())
        hp = row[nsi+1].strip() if nsi+1 < rl else ''
        gv = row[mgi].strip() if mgi < rl else ''
        lhp = row[mgi+3].strip() if mgi+3 < rl else f"{hp}_{gv}"
        ch = row[mgi+4].strip() if mgi+4 < rl else ''
        gt = row[mgi+5].strip() if mgi+5 < rl else ''
        essay = right.replace(' , ', ', ').strip()
        t1,t2,t3,t4,sent,valid = nlp(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        sid = f"{sv}_{lhp}_{gv}_{fn}"
        res.append((sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1, t2, t3, t4, sent, valid))
    return res

def fast_insert(cur, sql, data, batch=BATCH):
    """Insert nhanh, bỏ qua lỗi"""
    if not data: return 0
    done = 0
    for i in range(0, len(data), batch):
        chunk = data[i:i+batch]
        try:
            cur.executemany(sql, chunk)
            done += len(chunk)
        except:
            for row in chunk:
                try: cur.execute(sql, row); done += 1
                except: pass
    return done

def main():
    t0 = time.time()
    print("="*50, "\n📊 PIPELINE 2: DIRECT INSERT\n", "="*50)
    
    # Download
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    client = blob.get_container_client(CONTAINER_NAME).get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    # Parse
    t1 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK], FILE_NAME) for i in range(0, len(lines), CHUNK)]
    all_res = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches): all_res.extend(res)
    print(f"  Parse: {len(all_res):,} ({time.time()-t1:.1f}s)")
    
    # Chuẩn bị data (1 vòng lặp, set lookup)
    t1 = time.time()
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    
    seen = {k: set() for k in ['lop','sv','gv','lhp','gy']}
    data = {k: [] for k in ['lop','sv','gv','lhp','gy','kq']}
    
    for r in all_res:
        sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1v, t2v, t3v, t4v, sent, valid = r
        
        if ml and ml not in seen['lop']:
            seen['lop'].add(ml)
            data['lop'].append((ml, ml, ml))
        if sv and sv not in seen['sv']:
            seen['sv'].add(sv)
            data['sv'].append((sv, '', '', None, ml))
        if gv and gv not in seen['gv']:
            seen['gv'].add(gv)
            data['gv'].append((gv, '', ''))
        if lhp and lhp not in seen['lhp']:
            seen['lhp'].add(lhp)
            data['lhp'].append((lhp, lhp, hp, gv, mhk))
        if essay and sid not in seen['gy']:
            seen['gy'].add(sid)
            data['gy'].append((sid, sv, lhp, essay[:4000], sent, valid, t1v, t2v, t3v, t4v))
            d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
            for mc in [13,14,15,16]:
                data['kq'].append((sid, mc, d))
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    data['kq'].append((sid, mc, d))
            except: pass
    
    print(f"  Prepare: LOP={len(data['lop'])} SV={len(data['sv'])} GV={len(data['gv'])} LHP={len(data['lhp'])} GY={len(data['gy'])} KQ={len(data['kq'])} ({time.time()-t1:.1f}s)")
    
    # INSERT
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR, autocommit=True)
    cur = conn.cursor()
    cur.fast_executemany = True
    
    # Tắt constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
               'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    
    cur.execute("IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy=?) INSERT INTO DIM_HOC_KY VALUES(?,?,?)",
                (mhk, mhk, f"{nbd}-{nbd+1}", hk))
    
    # Insert từng bảng - mỗi bảng 1 câu SQL
    inserts = [
        ("DIM_LOP", "INSERT INTO DIM_LOP_SINH_VIEN(MaLop,Lop,MaChuyenNganh) VALUES(?,?,?)", data['lop']),
        ("DIM_SV", "INSERT INTO DIM_SINH_VIEN(MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES(?,?,?,?,?)", data['sv']),
        ("DIM_GV", "INSERT INTO DIM_GIANG_VIEN(MaGV,HoDemGV,TenGV) VALUES(?,?,?)", data['gv']),
        ("DIM_LHP", "INSERT INTO DIM_LOP_HOC_PHAN(MaLopHP,LopHP,MaHP,MaGV,MaHocKy) VALUES(?,?,?,?,?)", data['lhp']),
        ("FACT_GY", "INSERT INTO FACT_GOP_Y_TU_LUAN(SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES(?,?,?,?,?,?,?,?,?,?)", data['gy']),
        ("FACT_KQ", "INSERT INTO FACT_KET_QUA_DANH_GIA(SubmissionID,MaCauHoi,Diem) VALUES(?,?,?)", data['kq']),
    ]
    
    for name, sql, d in inserts:
        t2 = time.time()
        n = fast_insert(cur, sql, d)
        print(f"  {name}: {n:,} ({time.time()-t2:.1f}s)")
    
    # Bật constraint
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN',
               'FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    
    conn.close()
    print(f"  Insert total: {time.time()-t1:.1f}s")
    print(f"\n🎉 Total: {time.time()-t0:.1f}s | {len(all_res):,} rows")

if __name__ == "__main__":
    main()
