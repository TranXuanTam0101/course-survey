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
BATCH_SIZE = 50000

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

# ================= MASTER LOOKUP (từ DB) =================
_g_cn = {}      # Dict: MaChuyenNganh -> {MaChuyenNganh, TenChuyenNganh, MaNganh, TenNganh, MaKhoa, TenKhoa}
_g_hp = {}      # Dict: MaHP -> TenHP
_g_khoa_hp = {} # Dict: MaHP -> {MaKhoa, TenKhoa}
_g_valid_cn = set()  # Set MaChuyenNganh hợp lệ
_g_valid_hp = set()  # Set MaHP hợp lệ
_g_valid_gv = set()  # Set MaGV hợp lệ
_g_valid_sv = set()  # Set MaSV hợp lệ
_g_valid_lop = set() # Set MaLop hợp lệ
_g_valid_lhp = set() # Set MaLopHP hợp lệ

def load_master_from_db():
    """Load master data + existing IDs từ Database"""
    global _g_cn, _g_hp, _g_khoa_hp
    global _g_valid_cn, _g_valid_hp, _g_valid_gv, _g_valid_sv, _g_valid_lop, _g_valid_lhp
    
    print("\n📚 Load master từ Database...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Chuyên ngành
    cursor.execute("""
        SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, 
               n.TenNganh, n.MaKhoa, k.TenKhoa
        FROM DIM_CHUYEN_NGANH cn
        JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh
        JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_cn[key] = {
            'MaChuyenNganh': key,
            'TenChuyenNganh': str(row[1]).strip(),
            'MaNganh': str(row[2]).strip(),
            'TenNganh': str(row[3]).strip(),
            'MaKhoa': str(row[4]).strip(),
            'TenKhoa': str(row[5]).strip()
        }
        _g_valid_cn.add(key)
    print(f"  -> Chuyên ngành: {len(_g_cn)} records")
    
    # Học phần
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa
        FROM DIM_HOC_PHAN hp
        JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_hp[key] = str(row[1]).strip()
        _g_khoa_hp[key] = {'MaKhoa': str(row[2]).strip(), 'TenKhoa': str(row[3]).strip()}
        _g_valid_hp.add(key)
    print(f"  -> Học phần: {len(_g_hp)} records")
    
    # Existing IDs
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    _g_valid_gv.update(str(r[0]).strip() for r in cursor.fetchall())
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    _g_valid_sv.update(str(r[0]).strip() for r in cursor.fetchall())
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    _g_valid_lop.update(str(r[0]).strip() for r in cursor.fetchall())
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    _g_valid_lhp.update(str(r[0]).strip() for r in cursor.fetchall())
    
    print(f"  -> Existing: GV={len(_g_valid_gv)}, SV={len(_g_valid_sv)}, Lop={len(_g_valid_lop)}, LopHP={len(_g_valid_lhp)}")
    
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
    """Lookup Chuyên ngành - đảm bảo trả về MaChuyenNganh hợp lệ"""
    lop_norm = normalize_lop(lop)
    
    if _LOP_RE.match(lop_norm):
        ma_cn = f"K{lop_norm[3:5]}"
        if ma_cn in _g_cn:
            return _g_cn[ma_cn]
        else:
            # Nếu chưa có trong DB, thêm vào DIM_CHUYEN_NGANH sau
            return {
                'MaChuyenNganh': ma_cn,
                'TenChuyenNganh': f'Chuyên ngành {ma_cn}',
                'MaNganh': 'KHOA01NG01',  # Fallback
                'TenNganh': 'Ngành mặc định',
                'MaKhoa': 'KHOA01',       # Fallback
                'TenKhoa': 'Trường Đại học Kinh tế'
            }
    else:
        # Lớp đặc biệt
        return {
            'MaChuyenNganh': lop_norm or lop,
            'TenChuyenNganh': lop,
            'MaNganh': 'KHOA01NG01',
            'TenNganh': 'Ngành mặc định',
            'MaKhoa': 'KHOA01',
            'TenKhoa': 'Trường Đại học Kinh tế'
        }

def lookup_hp(ma_hp):
    """Lookup Học phần"""
    if not ma_hp:
        return '', 'KHOA01', 'Trường Đại học Kinh tế'
    key = str(ma_hp).strip()
    ten_hp = _g_hp.get(key, '')
    khoa = _g_khoa_hp.get(key, {'MaKhoa': 'KHOA01', 'TenKhoa': 'Trường Đại học Kinh tế'})
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
        
        ni = line.upper().find('NULL')
        left = line[:ni].rstrip(', \t') if ni >= 0 else line
        right = line[ni+4:].lstrip(', \t') if ni >= 0 else ''
        
        row = [x.strip() for x in left.split(',')]
        rl = len(row)
        if rl < 10: continue
        
        nsi = -1
        for i in range(2, min(12, rl)):
            if _DATE_RE.match(row[i]): nsi = i; break
        if nsi == -1: continue
        
        mgi = -1
        for i in range(nsi+1, min(nsi+25, rl)):
            if _MA_GV_RE.match(row[i]): mgi = i; break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
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
        
        t1, t2, t3, t4, sent, valid = nlp_fast(essay) if essay else (0,0,0,0,'NEUTRAL',0)
        
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
            if (i+1) % 5 == 0 or i == len(batches)-1:
                print(f"    Batch {i+1}/{len(batches)}: {len(res):,} rows")
    
    df = pd.DataFrame(all_results, columns=COLUMNS)
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= LOAD DB - ĐẢM BẢO FK =================
def ensure_chuyen_nganh_exists(cursor, df):
    """Đảm bảo tất cả MaChuyenNganh trong df tồn tại trong DIM_CHUYEN_NGANH"""
    cn_new = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing = {str(r[0]).strip() for r in cursor.fetchall()}
    
    to_insert = cn_new[~cn_new['MaChuyenNganh'].isin(existing)]
    
    if not to_insert.empty:
        print(f"  -> Thêm {len(to_insert)} Chuyên ngành mới...")
        data = []
        for _, r in to_insert.iterrows():
            data.append((
                str(r['MaChuyenNganh'])[:20],
                str(r['TenChuyenNganh'])[:200],
                str(r['MaNganh'])[:20] if pd.notna(r['MaNganh']) else 'KHOA01NG01'
            ))
        
        cursor.executemany(
            "INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)",
            data
        )
        cursor.connection.commit()
        
        # Update cache
        global _g_valid_cn, _g_cn
        for _, r in to_insert.iterrows():
            key = str(r['MaChuyenNganh']).strip()
            _g_valid_cn.add(key)

def ensure_nganh_exists(cursor, df):
    """Đảm bảo tất cả MaNganh tồn tại"""
    nganh_new = df[['MaNganh', 'TenNganh', 'MaKhoa_CN']].drop_duplicates('MaNganh')
    nganh_new.columns = ['MaNganh', 'TenNganh', 'MaKhoa']
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing = {str(r[0]).strip() for r in cursor.fetchall()}
    
    to_insert = nganh_new[~nganh_new['MaNganh'].isin(existing)]
    
    if not to_insert.empty:
        print(f"  -> Thêm {len(to_insert)} Ngành mới...")
        data = []
        for _, r in to_insert.iterrows():
            data.append((
                str(r['MaNganh'])[:20],
                str(r['TenNganh'])[:200],
                str(r['MaKhoa'])[:20]
            ))
        
        cursor.executemany(
            "INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)",
            data
        )
        cursor.connection.commit()

def load_dim_safe(cursor, table, df, cols, id_col):
    """Load dimension an toàn - skip FK errors"""
    if df.empty: return 0
    
    df = df.drop_duplicates(id_col).fillna('')
    
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {str(r[0]).strip() for r in cursor.fetchall()}
    
    new = df[~df[id_col].astype(str).str.strip().isin(existing)]
    if new.empty: return 0
    
    print(f"    -> {table}: {len(new)} new records")
    
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
    
    ph = ', '.join(['?']*len(cols))
    q = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    
    cursor.fast_executemany = True
    inserted = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        try:
            cursor.executemany(q, batch)
            cursor.connection.commit()
            inserted += len(batch)
        except pyodbc.IntegrityError as e:
            # Thử insert từng dòng
            for d in batch:
                try:
                    cursor.execute(q, d)
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
    
    return inserted

def load_all(cursor, df):
    print("\n--- DIMENSIONS ---")
    t = 0
    
    # Đảm bảo FK tồn tại trước
    ensure_nganh_exists(cursor, df)
    ensure_chuyen_nganh_exists(cursor, df)
    
    # DIM_LOP_SINH_VIEN
    t += load_dim_safe(cursor, 'DIM_LOP_SINH_VIEN', df[['MaLop','Lop','MaChuyenNganh']],
                       ['MaLop','Lop','MaChuyenNganh'], 'MaLop')
    
    # DIM_SINH_VIEN
    t += load_dim_safe(cursor, 'DIM_SINH_VIEN', df[['MaSV','HoDem','Ten','NgaySinh','MaLop']],
                       ['MaSV','HoDem','Ten','NgaySinh','MaLop'], 'MaSV')
    
    # DIM_GIANG_VIEN
    t += load_dim_safe(cursor, 'DIM_GIANG_VIEN', df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV'),
                       ['MaGV','HoDemGV','TenGV'], 'MaGV')
    
    # DIM_HOC_PHAN
    t += load_dim_safe(cursor, 'DIM_HOC_PHAN', 
                       df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}).drop_duplicates('MaHP'),
                       ['MaHP','TenHP','MaKhoa'], 'MaHP')
    
    # DIM_HOC_KY
    mhk, nh, hk = derive_ma_hoc_ky()
    t += load_dim_safe(cursor, 'DIM_HOC_KY',
                       pd.DataFrame([{'MaHocKy':mhk,'NamHoc':nh,'HocKy':hk}]),
                       ['MaHocKy','NamHoc','HocKy'], 'MaHocKy')
    
    # DIM_LOP_HOC_PHAN
    dlhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP')
    dlhp['MaHocKy'] = mhk
    t += load_dim_safe(cursor, 'DIM_LOP_HOC_PHAN', dlhp,
                       ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], 'MaLopHP')
    
    print(f"  📊 Total new: {t}")
    return t

def load_facts(cursor, df):
    print("\n--- FACTS ---")
    
    # FACT_GOP_Y
    de = df[(df['EssayText'].notna()) & (df['EssayText']!='')].drop_duplicates('SubmissionID')
    if not de.empty:
        cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
        ex = {str(r[0]).strip() for r in cursor.fetchall()}
        dn = de[~de['SubmissionID'].astype(str).str.strip().isin(ex)]
        if not dn.empty:
            print(f"  -> FACT_GOP_Y: {len(dn):,} new")
            data = [(str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                     str(r['EssayText']), str(r['Sentiment'])[:20], int(r['Is_Valid']),
                     int(r['Tag_HocPhan']), int(r['Tag_DayHoc']), int(r['Tag_KiemTra']), int(r['Tag_Khac']))
                    for _, r in dn.iterrows()]
            for i in range(0, len(data), BATCH_SIZE):
                try:
                    cursor.executemany(
                        "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        data[i:i+BATCH_SIZE]
                    )
                    cursor.connection.commit()
                except:
                    # Insert từng dòng
                    for d in data[i:i+BATCH_SIZE]:
                        try:
                            cursor.execute(
                                "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                d
                            )
                            cursor.connection.commit()
                        except: pass
    
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
        print(f"  -> FACT_KET_QUA: {len(rows):,} rows")
        for i in range(0, len(rows), BATCH_SIZE):
            try:
                cursor.executemany(
                    "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)",
                    rows[i:i+BATCH_SIZE]
                )
                cursor.connection.commit()
            except:
                for d in rows[i:i+BATCH_SIZE]:
                    try:
                        cursor.execute(
                            "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)",
                            d
                        )
                        cursor.connection.commit()
                    except: pass

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
    backup_path = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(backup_path, index=False)
    print(f"  📁 Backup: {backup_path}")
    
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
