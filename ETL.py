#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA
- Parse CSV khảo sát
- NLP: Tag + Sentiment
- Load DIM_SINH_VIEN, DIM_LOP_SINH_VIEN, DIM_GIANG_VIEN, DIM_HOC_PHAN, DIM_LOP_HOC_PHAN
- Load FACT_GOP_Y_TU_LUAN, FACT_KET_QUA_DANH_GIA
"""

import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from multiprocessing import Pool, cpu_count

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=300;"
    f"Command Timeout=600;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_CONTAINER = "tailieu"

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 50000
BATCH_SIZE = 100000

print("=" * 70)
print("📊 PIPELINE 2: SURVEY DATA")
print(f"   Semester: {SEMESTER} | File: {SURVEY_FILE}")
print(f"   Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,} | Batch: {BATCH_SIZE:,}")
print("=" * 70)

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_LOP_RE = re.compile(r'^\d{2}K\d{2}$')

# ================= NLP FAST =================
TAG_KW = {
    'Tag_HocPhan': ['nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức', 'chuẩn đầu ra', 'tài liệu', 'giáo trình', 'thực hành', 'lý thuyết', 'phù hợp', 'bổ ích', 'cần thiết', 'cập nhật', 'thực tế'],
    'Tag_DayHoc': ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng', 'truyền đạt', 'hướng dẫn', 'nhiệt tình', 'tận tâm', 'dễ hiểu', 'sinh động', 'thú vị', 'hấp dẫn', 'chuyên nghiệp'],
    'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'thi', 'đề thi', 'chấm điểm', 'công bằng', 'minh bạch', 'khách quan', 'nghiêm túc', 'chính xác'],
    'Tag_Khac': ['cơ sở vật chất', 'phòng học', 'máy chiếu', 'wifi', 'hỗ trợ', 'góp ý', 'đề xuất', 'cải thiện']
}

SENT_KW = {
    'POSITIVE': ['tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả', 'chất lượng', 'tuyệt vời', 'xuất sắc', 'nhiệt tình', 'dễ hiểu', 'công bằng'],
    'NEGATIVE': ['tệ', 'kém', 'chán', 'dở', 'không tốt', 'khó hiểu', 'nhàm chán', 'thiếu', 'hạn chế', 'thất vọng', 'cần cải thiện'],
    'NEUTRAL': ['không có góp ý', 'không ý kiến', 'không có', 'bình thường']
}

# ================= MASTER LOOKUP (loaded từ DB) =================
_g_cn = {}  # Dict lookup Chuyên ngành
_g_hp = {}  # Dict lookup Học phần
_g_khoa_hp = {}  # Dict Khoa quản lý HP

def load_master_from_db():
    """Load master data từ DIM tables đã có"""
    global _g_cn, _g_hp, _g_khoa_hp
    
    print("\n📚 Load master từ Database...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Load Chuyên ngành
    cursor.execute("""
        SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, 
               n.TenNganh, n.MaKhoa, k.TenKhoa
        FROM DIM_CHUYEN_NGANH cn
        JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh
        JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        _g_cn[str(row[0]).strip()] = {
            'MaChuyenNganh': str(row[0]).strip(),
            'TenChuyenNganh': str(row[1]).strip(),
            'MaNganh': str(row[2]).strip(),
            'TenNganh': str(row[3]).strip(),
            'MaKhoa': str(row[4]).strip(),
            'TenKhoa': str(row[5]).strip()
        }
    print(f"  -> Chuyên ngành: {len(_g_cn)} records")
    
    # Load Học phần + Khoa quản lý
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa
        FROM DIM_HOC_PHAN hp
        JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_hp[key] = str(row[1]).strip()
        _g_khoa_hp[key] = {'MaKhoa': str(row[2]).strip(), 'TenKhoa': str(row[3]).strip()}
    print(f"  -> Học phần: {len(_g_hp)} records")
    
    conn.close()

# ================= BLOB =================
def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except:
        return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    return f"HK{hoc_ky}_{nam_bat_dau%100}{nam_ket_thuc%100}", f"{nam_bat_dau}-{nam_ket_thuc}", hoc_ky

def normalize_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def lookup_cn(lop):
    lop_norm = normalize_lop(lop)
    
    if _LOP_RE.match(lop_norm):
        ma_cn = f"K{lop_norm[3:5]}"
        return _g_cn.get(ma_cn, {
            'MaChuyenNganh': ma_cn, 'TenChuyenNganh': f'CN {ma_cn}',
            'MaNganh': f'N{ma_cn}', 'TenNganh': f'Ngành {ma_cn}',
            'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'
        })
    else:
        return {
            'MaChuyenNganh': lop_norm or lop, 'TenChuyenNganh': lop,
            'MaNganh': lop_norm or lop, 'TenNganh': lop,
            'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'
        }

def lookup_hp(ma_hp):
    if not ma_hp: return '', 'TĐHKT', 'Trường ĐHKT'
    key = str(ma_hp).strip()
    ten_hp = _g_hp.get(key, '')
    khoa = _g_khoa_hp.get(key, {'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường ĐHKT'})
    return ten_hp, khoa['MaKhoa'], khoa['TenKhoa']

def nlp_fast(text):
    if not text or not isinstance(text, str) or len(text.strip()) < 5:
        return 0, 0, 0, 0, 'NEUTRAL', 0
    
    tl = text.lower()
    t1 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_HocPhan']) >= 2 else 0
    t2 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_DayHoc']) >= 2 else 0
    t3 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_KiemTra']) >= 2 else 0
    t4 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_Khac']) >= 2 else 0
    
    p = sum(tl.count(k) for k in SENT_KW['POSITIVE'])
    n = sum(tl.count(k) for k in SENT_KW['NEGATIVE'])
    e = sum(tl.count(k) for k in SENT_KW['NEUTRAL'])
    
    if 'không' in tl: p = max(0, p-1); n += 1
    
    if p > n and p > e: s = 'POSITIVE'
    elif n > p and n > e: s = 'NEGATIVE'
    else: s = 'NEUTRAL'
    
    v = 1 if len(tl) > 10 else 0
    return t1, t2, t3, t4, s, v

# ================= PARSE =================
COLUMNS = [
    'SubmissionID', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
    'MaLop', 'Lop', 'MaChuyenNganh', 'TenChuyenNganh',
    'MaNganh', 'TenNganh', 'MaKhoa_CN', 'TenKhoa_CN',
    'MaHP', 'TenHP', 'MaKhoa_HP', 'TenKhoa_HP',
    'MaGV', 'HoDemGV', 'TenGV', 'MaLopHP', 'LopHP',
    'CauHoi', 'GiaTri', 'EssayText',
    'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac',
    'Sentiment', 'Is_Valid'
]

def parse_batch(lines):
    results = []
    
    for line in lines:
        if not line: continue
        
        # Tìm NULL
        ni = line.upper().find('NULL')
        left = line[:ni].rstrip(', \t') if ni >= 0 else line
        right = line[ni+4:].lstrip(', \t') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',')]
        rl = len(row)
        if rl < 10: continue
        
        # Tìm ngày sinh
        nsi = -1
        for i in range(2, min(12, rl)):
            if _DATE_RE.match(row[i]): nsi = i; break
        if nsi == -1: continue
        
        # Tìm MaGV
        mgi = -1
        for i in range(nsi+1, min(nsi+25, rl)):
            if _MA_GV_RE.match(row[i]): mgi = i; break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        # Extract
        lop = row[0]; ma_sv = row[1]; ns = row[nsi]
        np = row[2:nsi]
        ten = np[-1] if np else ''
        hd = ' '.join(np[:-1]) if len(np) > 1 else ''
        
        ma_hp = row[nsi+1] if nsi+1 < rl else ''
        thp_raw = ' '.join(row[nsi+2:mgi])
        
        ma_gv = row[mgi] if mgi < rl else ''
        hdgv = row[mgi+1] if mgi+1 < rl else ''
        tgv = row[mgi+2] if mgi+2 < rl else ''
        lhp = row[mgi+3] if mgi+3 < rl else ''
        ch = row[mgi+4] if mgi+4 < rl else ''
        gt = row[mgi+5] if mgi+5 < rl else ''
        
        essay = right.replace(' , ', ', ').strip()
        
        # NLP
        t1, t2, t3, t4, sent, valid = nlp_fast(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        
        # Lookup
        cn = lookup_cn(lop)
        thp, mkhp, tkhp = lookup_hp(ma_hp)
        thp = thp or thp_raw
        
        ml = normalize_lop(lop)
        mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{FILE_NAME}"
        
        results.append([
            sid, ma_sv, hd, ten, ns,
            ml, lop, cn['MaChuyenNganh'], cn['TenChuyenNganh'],
            cn['MaNganh'], cn['TenNganh'], cn['MaKhoa'], cn['TenKhoa'],
            ma_hp, thp, mkhp, tkhp,
            ma_gv, hdgv, tgv, mlhp, lhp,
            ch, gt, essay,
            t1, t2, t3, t4, sent, valid
        ])
    
    return results

def parse_survey(content):
    print(f"  -> Parsing...")
    t0 = time.time()
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            print(f"    Batch {i+1}/{len(batches)}: {len(res):,} rows")
    
    df = pd.DataFrame(all_results, columns=COLUMNS)
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= LOAD DB =================
def load_dim(cursor, table, df, cols, id_col):
    if df.empty: return 0
    df = df.drop_duplicates(id_col).fillna('')
    
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {row[0] for row in cursor.fetchall()}
    new = df[~df[id_col].isin(existing)]
    if new.empty: return 0
    
    ph = ', '.join(['?']*len(cols))
    q = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    
    data = []
    for _, r in new.iterrows():
        td = []
        for c in cols:
            v = r[c]
            if c == 'NgaySinh':
                try:
                    dt = pd.to_datetime(v, format='%d/%m/%Y')
                    td.append(dt.strftime('%Y-%m-%d'))
                except: td.append(None)
            else:
                td.append(str(v)[:500] if v and pd.notna(v) else '')
        data.append(tuple(td))
    
    cursor.fast_executemany = True
    cursor.executemany(q, data)
    cursor.connection.commit()
    return len(new)

def load_all(cursor, df):
    print("\n--- DIMENSIONS ---")
    t = 0
    
    # DIM_LOP_SINH_VIEN
    t += load_dim(cursor, 'DIM_LOP_SINH_VIEN', df[['MaLop','Lop','MaChuyenNganh']],
                  ['MaLop','Lop','MaChuyenNganh'], 'MaLop')
    print(f"  DIM_LOP_SINH_VIEN: new records")
    
    # DIM_SINH_VIEN
    t += load_dim(cursor, 'DIM_SINH_VIEN', df[['MaSV','HoDem','Ten','NgaySinh','MaLop']],
                  ['MaSV','HoDem','Ten','NgaySinh','MaLop'], 'MaSV')
    print(f"  DIM_SINH_VIEN: new records")
    
    # DIM_GIANG_VIEN
    t += load_dim(cursor, 'DIM_GIANG_VIEN', df[['MaGV','HoDemGV','TenGV']],
                  ['MaGV','HoDemGV','TenGV'], 'MaGV')
    print(f"  DIM_GIANG_VIEN: new records")
    
    # DIM_HOC_PHAN
    t += load_dim(cursor, 'DIM_HOC_PHAN', df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}),
                  ['MaHP','TenHP','MaKhoa'], 'MaHP')
    print(f"  DIM_HOC_PHAN: new records")
    
    # DIM_HOC_KY
    mhk, nh, hk = derive_ma_hoc_ky()
    t += load_dim(cursor, 'DIM_HOC_KY', pd.DataFrame([{'MaHocKy':mhk,'NamHoc':nh,'HocKy':hk}]),
                  ['MaHocKy','NamHoc','HocKy'], 'MaHocKy')
    print(f"  DIM_HOC_KY: new records")
    
    # DIM_LOP_HOC_PHAN
    dlhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP')
    dlhp['MaHocKy'] = mhk
    t += load_dim(cursor, 'DIM_LOP_HOC_PHAN', dlhp,
                  ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], 'MaLopHP')
    print(f"  DIM_LOP_HOC_PHAN: new records")
    
    return t

def load_facts(cursor, df):
    print("\n--- FACTS ---")
    
    # FACT_GOP_Y
    de = df[(df['EssayText'].notna()) & (df['EssayText']!='')].drop_duplicates('SubmissionID')
    if not de.empty:
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        ex = {r[0] for r in cursor.fetchall()}
        dn = de[~de['SubmissionID'].isin(ex)]
        if not dn.empty:
            data = [(str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                     str(r['EssayText']), str(r['Sentiment'])[:20], int(r['Is_Valid']),
                     int(r['Tag_HocPhan']), int(r['Tag_DayHoc']), int(r['Tag_KiemTra']), int(r['Tag_Khac']))
                    for _, r in dn.iterrows()]
            for i in range(0, len(data), BATCH_SIZE):
                cursor.executemany("INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)", data[i:i+BATCH_SIZE])
                cursor.connection.commit()
            print(f"  FACT_GOP_Y: {len(data):,}")
        else:
            print(f"  FACT_GOP_Y: 0 new")
    
    # FACT_KET_QUA
    rows = []
    for _, r in df[(df['CauHoi']!='') & (df['GiaTri']!='')].iterrows():
        try:
            mc = int(float(r['CauHoi'])); d = int(float(r['GiaTri']))
            if 1<=mc<=12 and 1<=d<=5: rows.append((str(r['SubmissionID'])[:150], mc, d))
        except: pass
    
    for _, r in de.iterrows():
        s = r['Sentiment']; d = 5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if rows:
        for i in range(0, len(rows), BATCH_SIZE):
            cursor.executemany("INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)", rows[i:i+BATCH_SIZE])
            cursor.connection.commit()
        print(f"  FACT_KET_QUA: {len(rows):,}")

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Kết nối...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Load master từ DB
    load_master_from_db()
    
    # Download survey
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content: print("❌ No data!"); sys.exit(1)
    
    # Parse
    print(f"\n📝 PARSE + NLP")
    t1 = time.time()
    df = parse_survey(content)
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    if df.empty: print("❌ No data!"); sys.exit(1)
    
    # Backup
    df.to_parquet(f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet", index=False)
    
    # Load DB
    print(f"\n💾 LOAD DATABASE")
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    load_all(cursor, df)
    load_facts(cursor, df)
    conn.close()
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
