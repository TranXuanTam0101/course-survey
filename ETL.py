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
        'chuẩn đầu ra', 'mục tiêu', 'đề cương', 'tài liệu', 'giáo trình',
        'bài tập', 'thực hành', 'lý thuyết', 'cấu trúc', 'phân bố',
        'phù hợp', 'rõ ràng', 'đầy đủ', 'hợp lý', 'bổ ích', 'cần thiết',
        'quan trọng', 'chi tiết', 'cụ thể', 'cập nhật', 'mới', 'thực tế'
    ],
    'Tag_DayHoc': [
        'giảng viên', 'thầy', 'cô', 'dạy', 'giảng dạy', 'truyền đạt',
        'hướng dẫn', 'giải thích', 'phương pháp', 'cách dạy',
        'nhiệt tình', 'tận tâm', 'tâm huyết', 'nhiệt huyết', 'truyền cảm hứng',
        'dễ hiểu', 'sinh động', 'linh hoạt', 'thu hút', 'tương tác',
        'sôi nổi', 'thú vị', 'hấp dẫn', 'chuyên nghiệp', 'kinh nghiệm'
    ],
    'Tag_KiemTra': [
        'kiểm tra', 'đánh giá', 'thi', 'bài kiểm tra', 'đề thi',
        'chấm điểm', 'cho điểm', 'điểm số', 'kết quả',
        'công bằng', 'minh bạch', 'khách quan', 'nghiêm túc', 'chính xác',
        'đánh giá đúng', 'phản ánh đúng', 'công tâm', 'thực lực'
    ],
    'Tag_Khac': [
        'cơ sở vật chất', 'phòng học', 'máy chiếu', 'điều hòa',
        'bàn ghế', 'thư viện', 'wifi', 'internet', 'hỗ trợ',
        'tư vấn', 'đăng ký', 'lịch học', 'thời khóa biểu',
        'góp ý', 'đề xuất', 'kiến nghị', 'mong muốn', 'cải thiện',
        'nâng cao', 'bổ sung', 'điều chỉnh', 'thay đổi'
    ]
}

SENT_KW = {
    'POSITIVE': [
        'tuyệt vời', 'xuất sắc', 'rất tốt', 'rất hay', 'hoàn hảo',
        'tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả',
        'ấn tượng', 'chất lượng', 'chuyên nghiệp', 'hữu ích',
        'phù hợp', 'hợp lý', 'rõ ràng', 'dễ hiểu', 'nhiệt tình',
        'tận tâm', 'sinh động', 'thú vị', 'hấp dẫn', 'công bằng',
        'ok', 'ổn', 'được', 'cảm ơn', 'cố gắng', 'nỗ lực'
    ],
    'NEGATIVE': [
        'rất tệ', 'rất kém', 'rất chán', 'rất dở', 'thất vọng',
        'tệ', 'kém', 'chán', 'dở', 'không tốt', 'không hay',
        'không phù hợp', 'không hợp lý', 'không rõ ràng',
        'khó hiểu', 'nhàm chán', 'thiếu', 'chưa tốt', 'hạn chế',
        'bất cập', 'không công bằng', 'thiên vị', 'qua loa',
        'cần cải thiện', 'nên cải thiện', 'mong cải thiện'
    ],
    'NEUTRAL': [
        'không có góp ý', 'không ý kiến', 'không có ý kiến',
        'không góp ý', 'không có gì', 'không biết',
        'không', 'ko', 'không có', 'bình thường', 'tạm được'
    ]
}

# ================= MASTER LOOKUP (từ DB - đã load bởi Pipeline 1) =================
_g_cn = {}          # MaChuyenNganh -> {MaChuyenNganh, TenChuyenNganh, MaNganh, TenNganh, MaKhoa, TenKhoa}
_g_hp = {}          # MaHP -> TenHP
_g_khoa_hp = {}     # MaHP -> {MaKhoa, TenKhoa}
_g_valid_cn = set() # Set MaChuyenNganh hợp lệ
_g_valid_hp = set() # Set MaHP hợp lệ
_g_valid_gv = set() # Set MaGV hợp lệ
_g_valid_sv = set() # Set MaSV hợp lệ
_g_valid_lop = set()# Set MaLop hợp lệ
_g_valid_lhp = set()# Set MaLopHP hợp lệ

def load_master_from_db():
    """Load master data + existing IDs từ Database (đã được Pipeline 1 chuẩn bị)"""
    global _g_cn, _g_hp, _g_khoa_hp
    global _g_valid_cn, _g_valid_hp, _g_valid_gv, _g_valid_sv, _g_valid_lop, _g_valid_lhp
    
    print("\n📚 Load master từ Database (Pipeline 1)...")
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # Load Chuyên ngành (từ DIM_CHUYEN_NGANH + DIM_NGANH + DIM_KHOA)
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
    
    # Load Học phần (từ DIM_HOC_PHAN + DIM_KHOA)
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa, k.TenKhoa
        FROM DIM_HOC_PHAN hp
        JOIN DIM_KHOA k ON hp.MaKhoa = k.MaKhoa
    """)
    for row in cursor.fetchall():
        key = str(row[0]).strip()
        _g_hp[key] = str(row[1]).strip() if row[1] else ''
        _g_khoa_hp[key] = {
            'MaKhoa': str(row[2]).strip() if row[2] else 'KHOA01',
            'TenKhoa': str(row[3]).strip() if row[3] else 'Trường Đại học Kinh tế'
        }
        _g_valid_hp.add(key)
    print(f"  -> Học phần: {len(_g_hp)} records")
    
    # Load Existing IDs
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    _g_valid_gv.update(str(r[0]).strip() for r in cursor.fetchall() if r[0])
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    _g_valid_sv.update(str(r[0]).strip() for r in cursor.fetchall() if r[0])
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    _g_valid_lop.update(str(r[0]).strip() for r in cursor.fetchall() if r[0])
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    _g_valid_lhp.update(str(r[0]).strip() for r in cursor.fetchall() if r[0])
    
    print(f"  -> Existing IDs: GV={len(_g_valid_gv)}, SV={len(_g_valid_sv)}, "
          f"Lop={len(_g_valid_lop)}, LopHP={len(_g_valid_lhp)}")
    
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
    return f"HK{hoc_ky}_{nam_bat_dau%100}{nam_ket_thuc%100}", f"{nam_bat_dau}-{nam_ket_thuc}", hoc_ky

def normalize_lop(lop):
    """Chuẩn hóa mã lớp"""
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

def lookup_cn(lop):
    """Lookup Chuyên ngành từ mã lớp - dùng dict từ Pipeline 1"""
    lop_norm = normalize_lop(lop)
    
    # Trường hợp 1: Lớp khớp pattern XXKXX
    if _LOP_RE.match(lop_norm):
        ma_cn = f"K{lop_norm[3:5]}"
        if ma_cn in _g_cn:
            return _g_cn[ma_cn]
        else:
            # Fallback nếu chưa có trong DB
            return {
                'MaChuyenNganh': ma_cn,
                'TenChuyenNganh': f'Chuyên ngành {ma_cn}',
                'MaNganh': 'KHOA01NG01',
                'TenNganh': 'Ngành mặc định',
                'MaKhoa': 'KHOA01',
                'TenKhoa': 'Trường Đại học Kinh tế'
            }
    
    # Trường hợp 2: Lớp đặc biệt (CTS, QT...)
    # Tìm trong dict
    lop_upper = lop_norm.upper()
    for key, val in _g_cn.items():
        if lop_upper in val.get('TenChuyenNganh', '').upper() or \
           lop_upper in val.get('MaChuyenNganh', '').upper():
            return val
    
    # Fallback
    return {
        'MaChuyenNganh': lop_norm if lop_norm else lop,
        'TenChuyenNganh': lop,
        'MaNganh': 'KHOA01NG01',
        'TenNganh': 'Ngành mặc định',
        'MaKhoa': 'KHOA01',
        'TenKhoa': 'Trường Đại học Kinh tế'
    }

def lookup_hp(ma_hp):
    """Lookup Học phần từ dict (Pipeline 1)"""
    if not ma_hp:
        return '', 'KHOA01', 'Trường Đại học Kinh tế'
    
    key = str(ma_hp).strip()
    ten_hp = _g_hp.get(key, '')
    khoa = _g_khoa_hp.get(key, {'MaKhoa': 'KHOA01', 'TenKhoa': 'Trường Đại học Kinh tế'})
    return ten_hp, khoa['MaKhoa'], khoa['TenKhoa']

def nlp_fast(text):
    """NLP siêu nhanh cho EssayText"""
    if not text or not isinstance(text, str) or len(text.strip()) < 5:
        return 0, 0, 0, 0, 'NEUTRAL', 0
    
    tl = text.lower()
    
    # Tags
    t1 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_HocPhan']) >= 2 else 0
    t2 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_DayHoc']) >= 2 else 0
    t3 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_KiemTra']) >= 2 else 0
    t4 = 1 if sum(tl.count(k) for k in TAG_KW['Tag_Khac']) >= 2 else 0
    
    # Sentiment
    p = sum(tl.count(k) for k in SENT_KW['POSITIVE'])
    n = sum(tl.count(k) for k in SENT_KW['NEGATIVE'])
    e = sum(tl.count(k) for k in SENT_KW['NEUTRAL'])
    
    # Xử lý negation
    if 'không' in tl or 'chẳng' in tl:
        p = max(0, p - 1)
        n += 1
    
    if p > n and p > e:
        s = 'POSITIVE'
    elif n > p and n > e:
        s = 'NEGATIVE'
    else:
        s = 'NEUTRAL'
    
    # Is_Valid
    v = 1 if len(tl) > 10 else 0
    if v == 1:
        invalid_patterns = [
            r'^(không|ko|k|không có|không có gì|\.|\,|\s)+$',
            r'^[\s\.\,\;\:\!\?\-]+$',
        ]
        if any(re.match(p, tl) for p in invalid_patterns):
            v = 0
    
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
    """Parse một batch lines"""
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
        
        # Lookup từ Pipeline 1
        cn = lookup_cn(lop)
        thp, mkhp, tkhp = lookup_hp(ma_hp)
        thp = thp if thp else thp_raw
        
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
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    print(f"  -> {len(batches)} batches")
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            if (i+1) % 5 == 0 or i == len(batches)-1:
                print(f"    Batch {i+1}/{len(batches)}: {len(res):,} rows (total: {len(all_results):,})")
    
    df = pd.DataFrame(all_results, columns=COLUMNS)
    print(f"  ✅ Parsed {len(df):,} rows ({time.time()-t0:.1f}s)")
    
    # Thống kê
    print(f"\n  📊 Thống kê:")
    print(f"     Essay: {(df['EssayText']!='').sum():,}")
    print(f"     Trắc nghiệm: {((df['CauHoi']!='') & (df['GiaTri']!='')).sum():,}")
    print(f"     Sentiment: POS={ (df['Sentiment']=='POSITIVE').sum():,}, "
          f"NEG={(df['Sentiment']=='NEGATIVE').sum():,}, "
          f"NEU={(df['Sentiment']=='NEUTRAL').sum():,}")
    
    return df

# ================= LOAD DB =================
def ensure_missing_fks(cursor, df):
    """Đảm bảo các FK tồn tại - thêm vào DB nếu thiếu"""
    print("\n  -> Kiểm tra FK...")
    
    # Chuyên ngành thiếu
    cn_new = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    cn_missing = cn_new[~cn_new['MaChuyenNganh'].isin(_g_valid_cn)]
    
    if not cn_missing.empty:
        print(f"     Thêm {len(cn_missing)} Chuyên ngành mới...")
        for _, r in cn_missing.iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = ?)
                    INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh)
                    VALUES (?, ?, ?)
                """, (r['MaChuyenNganh'], r['MaChuyenNganh'], r['TenChuyenNganh'], r['MaNganh']))
                cursor.connection.commit()
                _g_valid_cn.add(str(r['MaChuyenNganh']).strip())
            except:
                pass
    
    # Học phần thiếu
    hp_new = df[['MaHP', 'TenHP', 'MaKhoa_HP']].drop_duplicates('MaHP')
    hp_new.columns = ['MaHP', 'TenHP', 'MaKhoa']
    hp_missing = hp_new[~hp_new['MaHP'].isin(_g_valid_hp)]
    
    if not hp_missing.empty:
        print(f"     Thêm {len(hp_missing)} Học phần mới...")
        for _, r in hp_missing.iterrows():
            try:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DIM_HOC_PHAN WHERE MaHP = ?)
                    INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa)
                    VALUES (?, ?, ?)
                """, (r['MaHP'], r['MaHP'], r['TenHP'], r['MaKhoa']))
                cursor.connection.commit()
                _g_valid_hp.add(str(r['MaHP']).strip())
            except:
                pass

def load_dim_safe(cursor, table, df, cols, id_col, valid_set=None):
    """Load dimension an toàn"""
    if df.empty: return 0
    
    df = df.drop_duplicates(id_col).fillna('')
    
    # Lấy existing IDs
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {str(r[0]).strip() for r in cursor.fetchall()}
    
    new = df[~df[id_col].astype(str).str.strip().isin(existing)]
    if new.empty: return 0
    
    print(f"    -> {table}: {len(new)} new")
    
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
            elif c in ['HocKy', 'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac', 'Is_Valid']:
                try: td.append(int(float(v)) if pd.notna(v) and v != '' else 0)
                except: td.append(0)
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
        except:
            for d in batch:
                try:
                    cursor.execute(q, d)
                    cursor.connection.commit()
                    inserted += 1
                except: pass
    
    if valid_set is not None:
        valid_set.update(new[id_col].astype(str).str.strip().tolist())
    
    return inserted

def load_all_dimensions(cursor, df):
    """Load tất cả Dimensions"""
    print("\n--- DIMENSIONS ---")
    t = 0
    
    # Đảm bảo FK
    ensure_missing_fks(cursor, df)
    
    # DIM_LOP_SINH_VIEN
    t += load_dim_safe(cursor, 'DIM_LOP_SINH_VIEN', 
                       df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop'),
                       ['MaLop','Lop','MaChuyenNganh'], 'MaLop', _g_valid_lop)
    
    # DIM_SINH_VIEN
    t += load_dim_safe(cursor, 'DIM_SINH_VIEN',
                       df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV'),
                       ['MaSV','HoDem','Ten','NgaySinh','MaLop'], 'MaSV', _g_valid_sv)
    
    # DIM_GIANG_VIEN
    t += load_dim_safe(cursor, 'DIM_GIANG_VIEN',
                       df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV'),
                       ['MaGV','HoDemGV','TenGV'], 'MaGV', _g_valid_gv)
    
    # DIM_HOC_PHAN
    t += load_dim_safe(cursor, 'DIM_HOC_PHAN',
                       df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}).drop_duplicates('MaHP'),
                       ['MaHP','TenHP','MaKhoa'], 'MaHP', _g_valid_hp)
    
    # DIM_HOC_KY
    mhk, nh, hk = derive_ma_hoc_ky()
    t += load_dim_safe(cursor, 'DIM_HOC_KY',
                       pd.DataFrame([{'MaHocKy':mhk,'NamHoc':nh,'HocKy':hk}]),
                       ['MaHocKy','NamHoc','HocKy'], 'MaHocKy')
    
    # DIM_LOP_HOC_PHAN
    dlhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP')
    dlhp['MaHocKy'] = mhk
    t += load_dim_safe(cursor, 'DIM_LOP_HOC_PHAN', dlhp,
                       ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], 'MaLopHP', _g_valid_lhp)
    
    print(f"  📊 Total new: {t}")
    return t

def load_facts(cursor, df):
    """Load FACT tables"""
    print("\n--- FACTS ---")
    
    # FACT_GOP_Y_TU_LUAN
    de = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
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
                batch = data[i:i+BATCH_SIZE]
                try:
                    cursor.executemany(
                        "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        batch
                    )
                    cursor.connection.commit()
                except:
                    for d in batch:
                        try:
                            cursor.execute(
                                "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)",
                                d
                            )
                            cursor.connection.commit()
                        except: pass
            
            print(f"  ✅ FACT_GOP_Y: {len(data):,} inserted")
        else:
            print(f"  ✅ FACT_GOP_Y: 0 new")
    
    # FACT_KET_QUA_DANH_GIA
    rows = []
    
    # Trắc nghiệm (1-12)
    df_tn = df[(df['CauHoi'] != '') & (df['GiaTri'] != '')]
    for _, r in df_tn.iterrows():
        try:
            mc = int(float(r['CauHoi']))
            d = int(float(r['GiaTri']))
            if 1 <= mc <= 12 and 1 <= d <= 5:
                rows.append((str(r['SubmissionID'])[:150], mc, d))
        except: pass
    
    # Tự luận (13-16) - điểm từ sentiment
    for _, r in de.iterrows():
        s = r['Sentiment']
        if s == 'POSITIVE': d = 5 if r['Is_Valid'] else 4
        elif s == 'NEGATIVE': d = 2 if r['Is_Valid'] else 1
        else: d = 3
        for mc in [13, 14, 15, 16]:
            rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if rows:
        print(f"  -> FACT_KET_QUA: {len(rows):,} rows")
        for i in range(0, len(rows), BATCH_SIZE):
            batch = rows[i:i+BATCH_SIZE]
            try:
                cursor.executemany(
                    "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)",
                    batch
                )
                cursor.connection.commit()
            except:
                for d in batch:
                    try:
                        cursor.execute(
                            "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)",
                            d
                        )
                        cursor.connection.commit()
                    except: pass
        
        print(f"  ✅ FACT_KET_QUA: {len(rows):,} inserted")

# ================= MAIN =================
def main():
    t0 = time.time()
    
    # Kết nối
    print("\n📥 Kết nối Azure & Database...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Load master từ DB (Pipeline 1)
    load_master_from_db()
    
    # Download survey
    print(f"\n📥 Download survey: {SURVEY_FILE}...")
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content:
        print("❌ Không thể đọc file survey!")
        sys.exit(1)
    print(f"  ✅ Downloaded ({len(content):,} bytes)")
    
    # Parse
    print(f"\n📝 PARSE + NLP")
    t1 = time.time()
    df = parse_survey(content)
    print(f"  ✅ Parse: {time.time()-t1:.1f}s")
    
    if df.empty:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    # Backup
    backup_path = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(backup_path, index=False)
    print(f"\n📁 Backup: {backup_path}")
    
    # Load DB
    print(f"\n💾 LOAD DATABASE")
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        load_all_dimensions(cursor, df)
        load_facts(cursor, df)
        print(f"  ✅ Load: {time.time()-t1:.1f}s")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
    
    print(f"\n🎉 HOÀN THÀNH! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
