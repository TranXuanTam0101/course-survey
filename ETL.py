#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA
- Parse CSV khảo sát với NULL làm mốc
- NLP: Tag classification + Sentiment analysis
- Load DIM_SINH_VIEN, DIM_LOP_SINH_VIEN, DIM_GIANG_VIEN, DIM_HOC_PHAN, DIM_LOP_HOC_PHAN
- Load FACT_GOP_Y_TU_LUAN, FACT_KET_QUA_DANH_GIA
- Tương thích với Pipeline 1 (Master Data)
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
    'Tag_HocPhan': [
        'nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức',
        'chuẩn đầu ra', 'tài liệu', 'giáo trình', 'thực hành', 'lý thuyết',
        'phù hợp', 'bổ ích', 'cần thiết', 'cập nhật', 'thực tế'
    ],
    'Tag_DayHoc': [
        'giảng viên', 'thầy', 'cô', 'dạy', 'giảng', 'truyền đạt',
        'hướng dẫn', 'nhiệt tình', 'tận tâm', 'dễ hiểu', 'sinh động',
        'thú vị', 'hấp dẫn', 'chuyên nghiệp'
    ],
    'Tag_KiemTra': [
        'kiểm tra', 'đánh giá', 'thi', 'đề thi', 'chấm điểm',
        'công bằng', 'minh bạch', 'khách quan', 'nghiêm túc', 'chính xác'
    ],
    'Tag_Khac': [
        'cơ sở vật chất', 'phòng học', 'máy chiếu', 'wifi', 'hỗ trợ',
        'góp ý', 'đề xuất', 'cải thiện'
    ]
}

SENT_KW = {
    'POSITIVE': [
        'tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả',
        'chất lượng', 'tuyệt vời', 'xuất sắc', 'nhiệt tình', 'dễ hiểu', 'công bằng'
    ],
    'NEGATIVE': [
        'tệ', 'kém', 'chán', 'dở', 'không tốt', 'khó hiểu',
        'nhàm chán', 'thiếu', 'hạn chế', 'thất vọng', 'cần cải thiện'
    ],
    'NEUTRAL': [
        'không có góp ý', 'không ý kiến', 'không có', 'bình thường'
    ]
}

# ================= MASTER DATA CACHE (từ DB) =================
_g_cn = {}          # MaChuyenNganh -> {MaChuyenNganh, TenChuyenNganh, MaNganh, TenNganh, MaKhoa, TenKhoa}
_g_hp = {}          # MaHP -> TenHP
_g_khoa_hp = {}     # MaHP -> {MaKhoa, TenKhoa}
_g_valid_cn = set() # MaChuyenNganh hợp lệ
_g_valid_hp = set() # MaHP hợp lệ
_g_valid_khoa = {}  # TenKhoa -> MaKhoa

def load_master_from_db():
    """Load master data từ Database (đã được Pipeline 1 tạo)"""
    global _g_cn, _g_hp, _g_khoa_hp, _g_valid_cn, _g_valid_hp, _g_valid_khoa
    
    print("\n📚 Load master từ Database...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Load Khoa
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    for row in cursor.fetchall():
        _g_valid_khoa[str(row[1]).strip()] = str(row[0]).strip()
    print(f"  -> Khoa: {len(_g_valid_khoa)} records")
    
    # Load Chuyên ngành + Ngành + Khoa
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
    
    # Load Học phần + Khoa quản lý
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa
        FROM DIM_HOC_PHAN hp
        JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_hp[key] = str(row[1]).strip()
        _g_khoa_hp[key] = {
            'MaKhoa': str(row[2]).strip(),
            'TenKhoa': str(row[3]).strip()
        }
        _g_valid_hp.add(key)
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
    """Tạo MaHocKy từ SURVEY_FILE"""
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    nam_hoc = f"{nam_bat_dau}-{nam_ket_thuc}"
    year_part = f"{nam_bat_dau % 100}{nam_ket_thuc % 100}"
    ma_hoc_ky = f"HK{hoc_ky}_{year_part}"
    return ma_hoc_ky, nam_hoc, hoc_ky

def normalize_lop(lop):
    """Chuẩn hóa mã lớp"""
    if not isinstance(lop, str):
        return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'):
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip()

def lookup_cn(lop):
    """
    Lookup Chuyên ngành từ mã lớp
    - Nếu Lop khớp pattern XXKXX: lấy MaChuyenNganh = K + XX
    - Nếu không khớp: lấy MaChuyenNganh = Lop
    """
    lop_norm = normalize_lop(lop)
    
    if _LOP_RE.match(lop_norm):
        ma_cn = f"K{lop_norm[3:5]}"
        if ma_cn in _g_cn:
            return _g_cn[ma_cn]
        else:
            # Fallback: tạo thông tin từ mã
            return {
                'MaChuyenNganh': ma_cn,
                'TenChuyenNganh': f'Chuyên ngành {ma_cn}',
                'MaNganh': list(_g_cn.values())[0]['MaNganh'] if _g_cn else 'KHOA01NG01',
                'TenNganh': 'Ngành mặc định',
                'MaKhoa': list(_g_valid_khoa.values())[0] if _g_valid_khoa else 'KHOA01',
                'TenKhoa': 'Trường Đại học Kinh tế'
            }
    else:
        # Lớp đặc biệt (CTS-, ...)
        return {
            'MaChuyenNganh': lop_norm if lop_norm else lop,
            'TenChuyenNganh': lop,
            'MaNganh': list(_g_cn.values())[0]['MaNganh'] if _g_cn else 'KHOA01NG01',
            'TenNganh': 'Ngành mặc định',
            'MaKhoa': list(_g_valid_khoa.values())[0] if _g_valid_khoa else 'KHOA01',
            'TenKhoa': 'Trường Đại học Kinh tế'
        }

def lookup_hp(ma_hp):
    """
    Lookup Học phần từ MaHP
    - Lấy TenHP từ DIM_HOC_PHAN
    - Lấy MaKhoa, TenKhoa quản lý học phần
    """
    if not ma_hp:
        default_khoa = list(_g_valid_khoa.items())[0] if _g_valid_khoa else ('KHOA01', 'Trường Đại học Kinh tế')
        return '', default_khoa[0], default_khoa[1]
    
    key = str(ma_hp).strip()
    ten_hp = _g_hp.get(key, '')
    khoa_info = _g_khoa_hp.get(key, None)
    
    if khoa_info:
        return ten_hp, khoa_info['MaKhoa'], khoa_info['TenKhoa']
    else:
        default_khoa = list(_g_valid_khoa.items())[0] if _g_valid_khoa else ('KHOA01', 'Trường Đại học Kinh tế')
        return ten_hp, default_khoa[0], default_khoa[1]

def nlp_fast(text):
    """NLP siêu nhanh cho EssayText"""
    if not text or not isinstance(text, str) or len(text.strip()) < 5:
        return 0, 0, 0, 0, 'NEUTRAL', 0
    
    tl = text.lower()
    
    # Tags (>= 2 từ khóa -> 1)
    t1 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_HocPhan']) >= 2 else 0
    t2 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_DayHoc']) >= 2 else 0
    t3 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_KiemTra']) >= 2 else 0
    t4 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_Khac']) >= 2 else 0
    
    # Sentiment
    p = sum(tl.count(k) for k in SENT_KW['POSITIVE'])
    n = sum(tl.count(k) for k in SENT_KW['NEGATIVE'])
    e = sum(tl.count(k) for k in SENT_KW['NEUTRAL'])
    
    # Xử lý negation
    if 'không' in tl:
        p = max(0, p - 1)
        n += 1
    
    # Xác định sentiment
    if p > n and p > e:
        s = 'POSITIVE'
    elif n > p and n > e:
        s = 'NEGATIVE'
    else:
        s = 'NEUTRAL'
    
    # Is_Valid: text > 10 ký tự
    v = 1 if len(tl) > 10 else 0
    
    return t1, t2, t3, t4, s, v


# ================= PARSE FUNCTIONS =================
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
    """Parse một batch lines"""
    results = []
    
    for line in lines:
        if not line:
            continue
        
        # Tìm NULL làm mốc
        ni = line.upper().find('NULL')
        if ni >= 0:
            left = line[:ni].rstrip(', \t')
            right = line[ni + 4:].lstrip(', \t')
        else:
            left = line
            right = ''
        
        # Split phần trái
        row = [x.strip() for x in left.split(',')]
        rl = len(row)
        
        if rl < 10:
            continue
        
        # Tìm ngày sinh
        nsi = -1
        for i in range(2, min(12, rl)):
            if _DATE_RE.match(row[i]):
                nsi = i
                break
        if nsi == -1:
            continue
        
        # Tìm MaGV
        mgi = -1
        for i in range(nsi + 1, min(nsi + 25, rl)):
            if _MA_GV_RE.match(row[i]):
                mgi = i
                break
        if mgi == -1:
            mgi = min(rl - 1, nsi + 8)
        
        # Trích xuất các trường
        lop = row[0]
        ma_sv = row[1]
        ns = row[nsi]
        
        # Họ tên SV
        np = row[2:nsi]
        ten = np[-1] if np else ''
        hd = ' '.join(np[:-1]) if len(np) > 1 else ''
        
        # Các trường còn lại
        ma_hp = row[nsi + 1] if nsi + 1 < rl else ''
        thp_raw = ' '.join(row[nsi + 2:mgi])
        
        ma_gv = row[mgi] if mgi < rl else ''
        hdgv = row[mgi + 1] if mgi + 1 < rl else ''
        tgv = row[mgi + 2] if mgi + 2 < rl else ''
        lhp = row[mgi + 3] if mgi + 3 < rl else ''
        ch = row[mgi + 4] if mgi + 4 < rl else ''
        gt = row[mgi + 5] if mgi + 5 < rl else ''
        
        # Essay text
        essay = right.replace(' , ', ', ').strip()
        
        # NLP
        t1, t2, t3, t4, sent, valid = nlp_fast(essay) if essay else (0, 0, 0, 0, 'NEUTRAL', 0)
        
        # Lookup Chuyên ngành (từ Lop)
        cn = lookup_cn(lop)
        
        # Lookup Học phần (từ MaHP)
        thp, mkhp, tkhp = lookup_hp(ma_hp)
        thp = thp if thp else thp_raw
        
        # Tạo mã
        ml = normalize_lop(lop)
        mlhp = lhp if lhp else f"{ma_hp}_{ma_gv}"
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
    """Parse survey với multiprocessing"""
    print(f"  -> Parsing với {NUM_WORKERS} workers...")
    t0 = time.time()
    
    lines = [l.strip() for l in content.split('\n') if l.strip()]
    print(f"  -> {len(lines):,} dòng")
    
    batches = [lines[i:i + CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    print(f"  -> {len(batches)} batches")
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            if (i + 1) % 5 == 0 or i == len(batches) - 1:
                print(f"    Batch {i + 1}/{len(batches)}: {len(res):,} rows (total: {len(all_results):,})")
    
    df = pd.DataFrame(all_results, columns=COLUMNS)
    print(f"  ✅ Parsed {len(df):,} rows ({time.time() - t0:.1f}s)")
    
    # Thống kê nhanh
    essay_count = (df['EssayText'] != '').sum()
    print(f"  📊 Essay: {essay_count:,} | Trắc nghiệm: {len(df) - essay_count:,}")
    
    return df


# ================= DATABASE LOAD FUNCTIONS =================
def load_dim_safe(cursor, table, df, cols, id_col):
    """
    Load dimension - INSERT nếu chưa tồn tại, bỏ qua nếu đã có
    """
    if df.empty:
        return 0
    
    df = df.drop_duplicates(id_col).fillna('')
    
    # Lấy IDs hiện có
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # Lọc dòng mới
    df['_id_str'] = df[id_col].astype(str).str.strip()
    new = df[~df['_id_str'].isin(existing)]
    
    if new.empty:
        return 0
    
    print(f"    -> {table}: {len(new)} new records")
    
    # Chuẩn bị data
    data = []
    for _, r in new.iterrows():
        td = []
        for c in cols:
            v = r[c]
            if c == 'NgaySinh':
                try:
                    dt = pd.to_datetime(v, format='%d/%m/%Y')
                    td.append(dt.strftime('%Y-%m-%d'))
                except:
                    td.append(None)
            elif c in ['HocKy', 'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac', 'Is_Valid']:
                try:
                    td.append(int(float(v)))
                except:
                    td.append(0)
            else:
                td.append(str(v)[:500] if v and pd.notna(v) else '')
        data.append(tuple(td))
    
    # INSERT
    ph = ', '.join(['?'] * len(cols))
    q = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph})"
    
    inserted = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        try:
            cursor.executemany(q, batch)
            cursor.connection.commit()
            inserted += len(batch)
        except Exception as e:
            # Fallback: insert từng dòng
            for d in batch:
                try:
                    cursor.execute(q, d)
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
    
    print(f"    -> Inserted: {inserted}")
    return inserted


def load_all_dimensions(cursor, df):
    """Load tất cả Dimension tables"""
    print("\n--- DIMENSIONS ---")
    total = 0
    
    # DIM_LOP_SINH_VIEN
    total += load_dim_safe(
        cursor, 'DIM_LOP_SINH_VIEN',
        df[['MaLop', 'Lop', 'MaChuyenNganh']].drop_duplicates('MaLop'),
        ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop'
    )
    
    # DIM_SINH_VIEN
    total += load_dim_safe(
        cursor, 'DIM_SINH_VIEN',
        df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']].drop_duplicates('MaSV'),
        ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV'
    )
    
    # DIM_GIANG_VIEN
    total += load_dim_safe(
        cursor, 'DIM_GIANG_VIEN',
        df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV'),
        ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV'
    )
    
    # DIM_HOC_PHAN (chỉ thêm HP mới chưa có trong Master)
    total += load_dim_safe(
        cursor, 'DIM_HOC_PHAN',
        df[['MaHP', 'TenHP', 'MaKhoa_HP']].rename(columns={'MaKhoa_HP': 'MaKhoa'}).drop_duplicates('MaHP'),
        ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP'
    )
    
    # DIM_HOC_KY
    mhk, nh, hk = derive_ma_hoc_ky()
    total += load_dim_safe(
        cursor, 'DIM_HOC_KY',
        pd.DataFrame([{'MaHocKy': mhk, 'NamHoc': nh, 'HocKy': hk}]),
        ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy'
    )
    
    # DIM_LOP_HOC_PHAN
    dlhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates('MaLopHP')
    dlhp['MaHocKy'] = mhk
    total += load_dim_safe(
        cursor, 'DIM_LOP_HOC_PHAN',
        dlhp,
        ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP'
    )
    
    print(f"  📊 Total new dimension records: {total}")
    return total


def load_fact_gop_y(cursor, df):
    """Load FACT_GOP_Y_TU_LUAN"""
    print("\n--- FACT_GOP_Y_TU_LUAN ---")
    
    # Lọc dòng có EssayText
    de = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    if de.empty:
        print("  ✅ No essay data")
        return 0
    
    # Lấy SubmissionID hiện có
    cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
    existing = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # Lọc dòng mới
    dn = de[~de['SubmissionID'].astype(str).str.strip().isin(existing)]
    
    if dn.empty:
        print("  ✅ 0 new rows")
        return 0
    
    print(f"  -> Inserting {len(dn):,} new rows...")
    
    data = []
    for _, r in dn.iterrows():
        data.append((
            str(r['SubmissionID'])[:150],
            str(r['MaSV'])[:20],
            str(r['MaLopHP'])[:50],
            str(r['EssayText']) if pd.notna(r['EssayText']) else '',
            str(r['Sentiment'])[:20] if pd.notna(r['Sentiment']) else 'NEUTRAL',
            int(r['Is_Valid']) if pd.notna(r['Is_Valid']) else 0,
            int(r['Tag_HocPhan']) if pd.notna(r['Tag_HocPhan']) else 0,
            int(r['Tag_DayHoc']) if pd.notna(r['Tag_DayHoc']) else 0,
            int(r['Tag_KiemTra']) if pd.notna(r['Tag_KiemTra']) else 0,
            int(r['Tag_Khac']) if pd.notna(r['Tag_Khac']) else 0
        ))
    
    inserted = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i + BATCH_SIZE]
        try:
            cursor.executemany("""
                INSERT INTO FACT_GOP_Y_TU_LUAN 
                (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid,
                 Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, batch)
            cursor.connection.commit()
            inserted += len(batch)
        except Exception as e:
            # Fallback từng dòng
            for d in batch:
                try:
                    cursor.execute("""
                        INSERT INTO FACT_GOP_Y_TU_LUAN 
                        (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid,
                         Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, d)
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
    
    print(f"  ✅ FACT_GOP_Y: {inserted:,} rows")
    return inserted


def load_fact_ket_qua(cursor, df):
    """Load FACT_KET_QUA_DANH_GIA"""
    print("\n--- FACT_KET_QUA_DANH_GIA ---")
    
    rows = []
    
    # 1. Câu trắc nghiệm (1-12)
    df_tn = df[(df['CauHoi'] != '') & (df['GiaTri'] != '')]
    for _, r in df_tn.iterrows():
        try:
            mc = int(float(r['CauHoi']))
            d = int(float(r['GiaTri']))
            if 1 <= mc <= 12 and 1 <= d <= 5:
                rows.append((str(r['SubmissionID'])[:150], mc, d))
        except:
            pass
    
    # 2. Câu tự luận (13-16) - lấy điểm từ sentiment
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    for _, r in df_essay.iterrows():
        s = r['Sentiment']
        if s == 'POSITIVE':
            d = 5
        elif s == 'NEGATIVE':
            d = 2
        else:
            d = 3
        
        for mc in [13, 14, 15, 16]:
            rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if not rows:
        print("  ✅ No data")
        return 0
    
    print(f"  -> {len(rows):,} rows")
    
    inserted = 0
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        try:
            cursor.executemany(
                "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)",
                batch
            )
            cursor.connection.commit()
            inserted += len(batch)
        except:
            # Fallback từng dòng (bỏ qua duplicate)
            for d in batch:
                try:
                    cursor.execute(
                        "IF NOT EXISTS (SELECT 1 FROM FACT_KET_QUA_DANH_GIA WHERE SubmissionID=? AND MaCauHoi=?) "
                        "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)",
                        (d[0], d[1], d[0], d[1], d[2])
                    )
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
    
    print(f"  ✅ FACT_KET_QUA: {inserted:,} rows")
    return inserted


def load_to_database(df):
    """Load toàn bộ dữ liệu vào database"""
    print("\n💾 LOAD TO DATABASE")
    start = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        dim_count = load_all_dimensions(cursor, df)
        fact1_count = load_fact_gop_y(cursor, df)
        fact2_count = load_fact_ket_qua(cursor, df)
        
        print(f"\n  ✅ Load hoàn tất: {time.time() - start:.1f}s")
        print(f"  📊 Dimensions: {dim_count} new")
        print(f"  📊 FACT_GOP_Y: {fact1_count:,} rows")
        print(f"  📊 FACT_KET_QUA: {fact2_count:,} rows")
        
    except Exception as e:
        print(f"\n  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()


# ================= MAIN =================
def main():
    total_start = time.time()
    
    # 1. Kết nối & Load Master từ DB
    print("\n📥 1. CONNECT & LOAD MASTER")
    start = time.time()
    
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Load master data từ DB (đã được Pipeline 1 tạo)
    load_master_from_db()
    
    # Download survey
    survey_content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    print(f"  ✅ Extract: {time.time() - start:.1f}s")
    
    if not survey_content:
        print("❌ Không thể đọc file survey!")
        sys.exit(1)
    
    # 2. Parse + NLP
    print("\n📝 2. PARSE + NLP")
    start = time.time()
    df = parse_survey(survey_content)
    print(f"  ✅ Parse + NLP: {time.time() - start:.1f}s")
    
    if df.empty:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    # 3. Backup
    print("\n💾 3. BACKUP")
    backup_path = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(backup_path, index=False, compression='snappy')
    print(f"  ✅ Backup: {backup_path}")
    
    # 4. Load Database
    print("\n💾 4. LOAD TO DATABASE")
    load_to_database(df)
    
    # Tổng kết
    total = time.time() - total_start
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH!")
    print(f"⏱️  Tổng thời gian: {total:.1f}s")
    print(f"📊 Tổng số dòng: {len(df):,}")
    print(f"📁 Backup: {backup_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
