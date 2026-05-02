#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: BCP - NHANH NHẤT CÓ THỂ
- Parse → ghi CSV → gọi bcp để import
- bcp nhanh hơn BULK INSERT 10x
"""
import os, sys, re, time, subprocess, tempfile
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

SERVER = "course-survey.database.windows.net"
DB = "course-survey-db"
UID = "sqladmin"

CONTAINER_NAME, RAWDATA_PATH = SEMESTER, "rawdata"
NUM_WORKERS, CHUNK = cpu_count(), 100000

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

def bcp_import(table, csv_path, cols):
    """Gọi bcp để import CSV vào table"""
    cmd = [
        "bcp", f"[{DB}].dbo.[{table}]", "in", csv_path,
        "-S", SERVER, "-U", UID, "-P", DB_PASSWORD,
        "-c", "-t", "|", "-r", "0x0a",
        "-b", "100000", "-m", "0",
        "-F", "1"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    return result.returncode == 0

def main():
    t0 = time.time()
    print("="*50, "\n📊 PIPELINE 2: BCP\n", "="*50)
    
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
    
    # Chuẩn bị
    t1 = time.time()
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    
    seen = {}
    for key in ['lop','sv','gv','lhp']:
        seen[key] = set()
    
    tmpdir = tempfile.mkdtemp()
    
    # Ghi CSV cho từng bảng
    files = {}
    for name, cols in [('lop', ['MaLop','Lop','MaChuyenNganh']),
                        ('sv', ['MaSV','HoDem','Ten','NgaySinh','MaLop']),
                        ('gv', ['MaGV','HoDemGV','TenGV']),
                        ('lhp', ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'])]:
        files[name] = open(f"{tmpdir}/{name}.csv", 'w', encoding='utf-8')
    
    # GY và KQ ghi riêng
    gy_file = open(f"{tmpdir}/gy.csv", 'w', encoding='utf-8')
    kq_file = open(f"{tmpdir}/kq.csv", 'w', encoding='utf-8')
    
    seen_gy = set()
    
    for r in all_res:
        sid, sv, ml, hp, gv, lhp, ch, gt, essay, t1, t2, t3, t4, sent, valid = r
        
        if ml and ml not in seen['lop']:
            seen['lop'].add(ml)
            files['lop'].write(f"{ml}|{ml}|{ml}\n")
        if sv and sv not in seen['sv']:
            seen['sv'].add(sv)
            files['sv'].write(f"{sv}|||NULL|{ml}\n")
        if gv and gv not in seen['gv']:
            seen['gv'].add(gv)
            files['gv'].write(f"{gv}||\n")
        if lhp and lhp not in seen['lhp']:
            seen['lhp'].add(lhp)
            files['lhp'].write(f"{lhp}|{lhp}|{hp}|{gv}|{mhk}\n")
        if essay and sid not in seen_gy:
            seen_gy.add(sid)
            gy_file.write(f"{sid}|{sv}|{lhp}|{essay}|{sent}|{valid}|{t1}|{t2}|{t3}|{t4}\n")
            d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
            for mc in [13,14,15,16]:
                kq_file.write(f"{sid}|{mc}|{d}\n")
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    kq_file.write(f"{sid}|{mc}|{d}\n")
            except: pass
    
    for f in files.values(): f.close()
    gy_file.close()
    kq_file.close()
    print(f"  Write CSV: {time.time()-t1:.1f}s")
    
    # BCP import
    t1 = time.time()
    
    # Tắt constraint trước
    import pyodbc
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DB};"
        f"UID={UID};PWD={DB_PASSWORD};Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=600;", autocommit=True
    )
    cur = conn.cursor()
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
        except: pass
    cur.execute(f"IF NOT EXISTS(SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy='{mhk}') INSERT INTO DIM_HOC_KY VALUES('{mhk}','{nbd}-{nbd+1}',{hk})")
    conn.close()
    
    # Chạy bcp
    tables = [
        ('DIM_LOP_SINH_VIEN', 'lop'),
        ('DIM_SINH_VIEN', 'sv'),
        ('DIM_GIANG_VIEN', 'gv'),
        ('DIM_LOP_HOC_PHAN', 'lhp'),
        ('FACT_GOP_Y_TU_LUAN', 'gy'),
        ('FACT_KET_QUA_DANH_GIA', 'kq'),
    ]
    
    for table, name in tables:
        csv_path = f"{tmpdir}/{name}.csv"
        if os.path.getsize(csv_path) > 0:
            ok = bcp_import(table, csv_path, None)
            print(f"  {table}: {'✅' if ok else '❌'}")
    
    # Bật lại constraint
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DB};"
        f"UID={UID};PWD={DB_PASSWORD};Encrypt=yes;TrustServerCertificate=no;"
        f"Connection Timeout=600;", autocommit=True
    )
    cur = conn.cursor()
    for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN','DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
        try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL")
        except: pass
    conn.close()
    
    print(f"  BCP: {time.time()-t1:.1f}s")
    print(f"\n🎉 Total: {time.time()-t0:.1f}s | {len(all_res):,} rows")

if __name__ == "__main__":
    main()
