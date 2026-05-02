#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BƯỚC 1: PARSE & LƯU CSV
- Parse survey → NLP → lưu CSV lên Azure Blob
- Chạy 1 lần, lưu kết quả
"""
import os, sys, re, time, io
from multiprocessing import Pool, cpu_count
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
PROCESSED_PATH = "processed-data"
NUM_WORKERS = cpu_count()
CHUNK = 100000

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
        # Format CSV: sid|sv|ml|hp|gv|lhp|ch|gt|essay|t1|t2|t3|t4|sent|valid
        res.append(f"{sid}|{sv}|{ml}|{hp}|{gv}|{lhp}|{ch}|{gt}|{essay}|{t1}|{t2}|{t3}|{t4}|{sent}|{valid}")
    return res

def main():
    t0 = time.time()
    print("="*50, "\n📊 BƯỚC 1: PARSE & LƯU CSV\n", "="*50)
    
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    
    # Download
    client = container.get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    
    # Parse (trả về list of string CSV)
    t1 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK], FILE_NAME) for i in range(0, len(lines), CHUNK)]
    all_csv = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches): all_csv.extend(res)
    print(f"  Parse: {len(all_csv):,} rows ({time.time()-t1:.1f}s)")
    
    # Lưu CSV lên Azure Blob
    t1 = time.time()
    csv_content = "SubmissionID|MaSV|MaLop|MaHP|MaGV|MaLopHP|CauHoi|GiaTri|EssayText|Tag_HocPhan|Tag_DayHoc|Tag_KiemTra|Tag_Khac|Sentiment|Is_Valid\n"
    csv_content += "\n".join(all_csv)
    
    blob_path = f"{PROCESSED_PATH}/{FILE_NAME}_parsed.csv"
    container.get_blob_client(blob_path).upload_blob(csv_content, overwrite=True)
    
    size_mb = len(csv_content) / 1024 / 1024
    print(f"  Upload: {blob_path} ({size_mb:.0f}MB) ({time.time()-t1:.1f}s)")
    print(f"\n🎉 Done: {time.time()-t0:.1f}s")
    print(f"\n👉 Chạy bước 2: python load_to_db.py")

if __name__ == "__main__":
    main()
