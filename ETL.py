#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2A: PARSE & SAVE CSV TO BLOB
- Parse survey → tạo DataFrame cho từng bảng
- Lưu mỗi bảng thành 1 file CSV riêng lên Azure Blob
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
        essay = right.replace(' , ', ', ').strip().replace('|', ' ').replace('\n', ' ').replace('\r', ' ')
        t1,t2,t3,t4,sent,valid = nlp(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        sid = f"{sv}_{lhp}_{gv}_{fn}"
        # sid|sv|ml|hp|gv|lhp|ch|gt|essay|t1|t2|t3|t4|sent|valid
        res.append(f"{sid}|{sv}|{ml}|{hp}|{gv}|{lhp}|{ch}|{gt}|{essay}|{t1}|{t2}|{t3}|{t4}|{sent}|{valid}")
    return res

def upload_csv(container, blob_path, header, rows):
    """Upload CSV lên Azure Blob"""
    content = header + "\n" + "\n".join(rows)
    container.get_blob_client(blob_path).upload_blob(content, overwrite=True)
    return len(content) / 1024 / 1024

def main():
    t0 = time.time()
    print("="*60)
    print("📊 PIPELINE 2A: PARSE & SAVE CSV")
    print("="*60)
    
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    
    # Download survey
    t1 = time.time()
    client = container.get_blob_client(f"{RAWDATA_PATH}/{SURVEY_FILE}")
    content = client.download_blob().readall().decode('utf-8-sig')
    print(f"  Download: {time.time()-t1:.1f}s")
    
    # Parse
    t1 = time.time()
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [(lines[i:i+CHUNK], FILE_NAME) for i in range(0, len(lines), CHUNK)]
    all_rows = []
    with Pool(NUM_WORKERS) as pool:
        for res in pool.imap_unordered(parse_batch, batches): all_rows.extend(res)
    print(f"  Parse: {len(all_rows):,} rows ({time.time()-t1:.1f}s)")
    
    # Tạo dữ liệu cho từng bảng
    t1 = time.time()
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc, hk = int(fn[:-1]), int(fn[-1])
    nbd = 2000 + yc - 1
    mhk = f"HK{hk}_{nbd%100}{(nbd+1)%100}"
    
    seen_lop, seen_sv, seen_gv, seen_lhp, seen_gy = set(), set(), set(), set(), set()
    data_lop, data_sv, data_gv, data_lhp, data_gy, data_kq = [], [], [], [], [], []
    
    for row in all_rows:
        parts = row.split('|')
        sid, sv, ml, hp, gv, lhp, ch, gt, essay = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5], parts[6], parts[7], parts[8]
        t1v, t2v, t3v, t4v, sent, valid = parts[9], parts[10], parts[11], parts[12], parts[13], parts[14]
        
        if ml and ml not in seen_lop:
            seen_lop.add(ml)
            data_lop.append(f"{ml}|{ml}|{ml}")
        if sv and sv not in seen_sv:
            seen_sv.add(sv)
            data_sv.append(f"{sv}|||NULL|{ml}")
        if gv and gv not in seen_gv:
            seen_gv.add(gv)
            data_gv.append(f"{gv}||")
        if lhp and lhp not in seen_lhp:
            seen_lhp.add(lhp)
            data_lhp.append(f"{lhp}|{lhp}|{hp}|{gv}|{mhk}")
        if essay and sid not in seen_gy:
            seen_gy.add(sid)
            data_gy.append(f"{sid}|{sv}|{lhp}|{essay}|{sent}|{valid}|{t1v}|{t2v}|{t3v}|{t4v}")
            d = 5 if sent == 'POSITIVE' else (2 if sent == 'NEGATIVE' else 3)
            for mc in [13,14,15,16]:
                data_kq.append(f"{sid}|{mc}|{d}")
        if ch and gt:
            try:
                mc, d = int(float(ch)), int(float(gt))
                if 1 <= mc <= 12 and 1 <= d <= 5:
                    data_kq.append(f"{sid}|{mc}|{d}")
            except: pass
    
    print(f"  Prepare: LOP={len(data_lop)} SV={len(data_sv)} GV={len(data_gv)} LHP={len(data_lhp)} GY={len(data_gy)} KQ={len(data_kq)} ({time.time()-t1:.1f}s)")
    
    # Upload từng bảng lên Azure Blob
    t1 = time.time()
    tables = [
        ("DIM_HOC_KY", f"{mhk}|{nbd}-{nbd+1}|{hk}", f"MaHocKy|NamHoc|HocKy\n{mhk}|{nbd}-{nbd+1}|{hk}", ["done"]),
        ("DIM_LOP_SINH_VIEN", f"MaLop|Lop|MaChuyenNganh", f"MaLop|Lop|MaChuyenNganh", data_lop),
        ("DIM_SINH_VIEN", f"MaSV|HoDem|Ten|NgaySinh|MaLop", f"MaSV|HoDem|Ten|NgaySinh|MaLop", data_sv),
        ("DIM_GIANG_VIEN", f"MaGV|HoDemGV|TenGV", f"MaGV|HoDemGV|TenGV", data_gv),
        ("DIM_LOP_HOC_PHAN", f"MaLopHP|LopHP|MaHP|MaGV|MaHocKy", f"MaLopHP|LopHP|MaHP|MaGV|MaHocKy", data_lhp),
        ("FACT_GOP_Y_TU_LUAN", f"SubmissionID|MaSV|MaLopHP|NoiDungGopY|Sentiment|Is_Valid|Tag_HocPhan|Tag_DayHoc|Tag_KiemTra|Tag_Khac", f"SubmissionID|MaSV|MaLopHP|NoiDungGopY|Sentiment|Is_Valid|Tag_HocPhan|Tag_DayHoc|Tag_KiemTra|Tag_Khac", data_gy),
        ("FACT_KET_QUA_DANH_GIA", f"SubmissionID|MaCauHoi|Diem", f"SubmissionID|MaCauHoi|Diem", data_kq),
    ]
    
    for name, header, full_header, data in tables:
        if name == "DIM_HOC_KY":
            container.get_blob_client(f"{PROCESSED_PATH}/{FILE_NAME}/{name}.csv").upload_blob(full_header, overwrite=True)
        else:
            size = upload_csv(container, f"{PROCESSED_PATH}/{FILE_NAME}/{name}.csv", header, data)
            print(f"  ✅ {name}: {len(data):,} rows ({size:.1f}MB)")
    
    print(f"  Upload: {time.time()-t1:.1f}s")
    print(f"\n🎉 Total: {time.time()-t0:.1f}s | {len(all_rows):,} rows")
    print(f"\n👉 Chạy: python pipeline2b_load.py")

if __name__ == "__main__":
    main()
