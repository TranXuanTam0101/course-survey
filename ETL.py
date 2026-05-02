#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - OPTIMIZED FOR SPEED
- Parse CSV siêu nhanh với NULL làm mốc
- NLP tối giản
- Bulk insert database
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
from collections import defaultdict

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
    f"MultipleActiveResultSets=yes;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"

NUM_WORKERS = cpu_count()
CHUNK_SIZE = 100000  # Tăng lên 100K
BATCH_SIZE = 100000

print("=" * 70)
print("📊 PIPELINE 2: SURVEY DATA (OPTIMIZED)")
print(f"   Workers: {NUM_WORKERS} | Chunk: {CHUNK_SIZE:,}")
print("=" * 70)

# ================= PATTERNS (Pre-compiled) =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_LOP_RE = re.compile(r'^\d{2}K\d{2}$')

# ================= NLP - Tối giản chỉ giữ keywords quan trọng =================
_TAG_KW = {
    0: ['nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức', 'chuẩn đầu ra', 'tài liệu', 'giáo trình', 'thực hành', 'lý thuyết'],
    1: ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng', 'truyền đạt', 'hướng dẫn', 'nhiệt tình', 'tận tâm', 'dễ hiểu'],
    2: ['kiểm tra', 'đánh giá', 'thi', 'đề thi', 'chấm điểm', 'công bằng', 'minh bạch', 'khách quan'],
    3: ['cơ sở vật chất', 'phòng học', 'máy chiếu', 'wifi', 'hỗ trợ', 'góp ý']
}

_SENT_POS = ['tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả', 'chất lượng', 'tuyệt vời', 'xuất sắc', 'nhiệt tình', 'dễ hiểu', 'công bằng']
_SENT_NEG = ['tệ', 'kém', 'chán', 'dở', 'không tốt', 'khó hiểu', 'nhàm chán', 'thiếu', 'hạn chế', 'thất vọng', 'cần cải thiện']
_SENT_NEU = ['không có góp ý', 'không ý kiến', 'không có', 'bình thường']

# ================= GLOBAL CACHE =================
_g_cn = {}          # MaChuyenNganh -> info dict
_g_hp = {}          # MaHP -> TenHP
_g_khoa_hp = {}     # MaHP -> MaKhoa
_g_cn_fast = {}     # Lop pattern -> info dict (cache lookup)

def load_master_fast():
    """Load master data 1 lần, tạo cache lookup nhanh"""
    global _g_cn, _g_hp, _g_khoa_hp, _g_cn_fast
    
    print("📚 Load master...", end=" ", flush=True)
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
        info = {
            'MaChuyenNganh': key,
            'TenChuyenNganh': str(row[1] or '').strip(),
            'MaNganh': str(row[2] or '').strip(),
            'TenNganh': str(row[3] or '').strip(),
            'MaKhoa': str(row[4] or '').strip(),
            'TenKhoa': str(row[5] or '').strip()
        }
        _g_cn[key] = info
        
        # Cache thêm: KXX -> info
        if key.startswith('K') and len(key) <= 4:
            _g_cn_fast[key] = info
    
    # Học phần
    cursor.execute("""
        SELECT hp.MaHP, hp.TenHP, hp.MaKhoa
        FROM DIM_HOC_PHAN hp
    """)
    for row in cursor.fetchall():
        key = str(row[0] or '').strip()
        if key:
            _g_hp[key] = str(row[1] or '').strip()
            _g_khoa_hp[key] = str(row[2] or 'KHOA01').strip()
    
    conn.close()
    print(f"CN={len(_g_cn)}, HP={len(_g_hp)}")

# ================= BLOB =================
def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except: return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    fn = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    yc = int(fn[:-1]); hk = int(fn[-1])
    nb = 2000 + yc - 1; nk = nb + 1
    return f"HK{hk}_{nb%100}{nk%100}", f"{nb}-{nk}", hk

def norm_lop(lop):
    if not isinstance(lop, str): return ""
    lop = lop.strip()
    if lop.upper().startswith('CTS-'): lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop: lop = lop.split(sep)[0]
    return lop.strip()

# ================= PARSE BATCH (TỐI ƯU) =================
def parse_batch(lines):
    """Parse batch - tối ưu không dùng dict"""
    results = []
    
    # Pre-load lookup functions vào local scope
    cn_fast = _g_cn_fast
    cn_all = _g_cn
    hp_dict = _g_hp
    khoa_hp_dict = _g_khoa_hp
    date_re = _DATE_RE
    gv_re = _MA_GV_RE
    lop_re = _LOP_RE
    file_name = FILE_NAME
    
    tag_kw = _TAG_KW
    sent_pos = _SENT_POS
    sent_neg = _SENT_NEG
    sent_neu = _SENT_NEU
    
    for line in lines:
        if not line: continue
        
        # Tìm NULL - dùng find nhanh hơn partition
        ni = line.find('NULL')
        if ni >= 0:
            # Tìm dấu phẩy trước NULL
            left_end = line.rfind(',', 0, ni)
            right_start = ni + 4
            if right_start < len(line) and line[right_start] == ',':
                right_start += 1
            left = line[:left_end] if left_end > 0 else line[:ni].rstrip(', \t')
            right = line[right_start:].strip()
        else:
            left = line
            right = ''
        
        # Split left - dùng split thay vì regex
        row = left.split(',')
        rl = len(row)
        if rl < 10: continue
        
        # Tìm ngày sinh (limit range)
        nsi = -1
        for i in range(2, min(12, rl)):
            if date_re.match(row[i].strip()): nsi = i; break
        if nsi == -1: continue
        
        # Tìm MaGV
        mgi = -1
        for i in range(nsi+1, min(nsi+20, rl)):
            if gv_re.match(row[i].strip()): mgi = i; break
        if mgi == -1: mgi = min(rl-1, nsi+8)
        
        # Extract
        lop = row[0].strip()
        ma_sv = row[1].strip()
        ns = row[nsi].strip()
        
        # Tên SV
        np = row[2:nsi]
        ten = np[-1].strip() if np else ''
        hd = ' '.join(x.strip() for x in np[:-1]) if len(np) > 1 else ''
        
        # HP
        ma_hp = row[nsi+1].strip() if nsi+1 < rl else ''
        thp_raw = ' '.join(x.strip() for x in row[nsi+2:mgi])
        
        # GV
        ma_gv = row[mgi].strip() if mgi < rl else ''
        hdgv = row[mgi+1].strip() if mgi+1 < rl else ''
        tgv = row[mgi+2].strip() if mgi+2 < rl else ''
        lhp = row[mgi+3].strip() if mgi+3 < rl else ''
        ch = row[mgi+4].strip() if mgi+4 < rl else ''
        gt = row[mgi+5].strip() if mgi+5 < rl else ''
        
        # Essay
        essay = right.replace(' , ', ', ').strip()
        
        # NLP siêu nhanh
        t1=t2=t3=t4=0; sent='NEUTRAL'; valid=0
        if essay and len(essay) > 5:
            tl = essay.lower()
            t1 = 1 if sum(1 for k in tag_kw[0] if k in tl) >= 2 else 0
            t2 = 1 if sum(1 for k in tag_kw[1] if k in tl) >= 2 else 0
            t3 = 1 if sum(1 for k in tag_kw[2] if k in tl) >= 2 else 0
            t4 = 1 if sum(1 for k in tag_kw[3] if k in tl) >= 2 else 0
            
            p = sum(1 for k in sent_pos if k in tl)
            n = sum(1 for k in sent_neg if k in tl) + (1 if 'không' in tl else 0)
            e = sum(1 for k in sent_neu if k in tl)
            
            sent = 'POSITIVE' if p > n and p > e else ('NEGATIVE' if n > p else 'NEUTRAL')
            valid = 1 if len(tl) > 10 and not tl.startswith('không') else 0
        
        # Lookup CN (dùng cache)
        ml = norm_lop(lop)
        if lop_re.match(ml):
            ma_cn = f"K{ml[3:5]}"
            cn = cn_fast.get(ma_cn, {'MaChuyenNganh': ma_cn, 'TenChuyenNganh': f'CN {ma_cn}',
                                      'MaNganh': 'KHOA01NG01', 'TenNganh': 'Ngành',
                                      'MaKhoa': 'KHOA01', 'TenKhoa': 'Trường ĐHKT'})
        else:
            cn = {'MaChuyenNganh': ml or lop, 'TenChuyenNganh': lop,
                  'MaNganh': 'KHOA01NG01', 'TenNganh': 'Ngành',
                  'MaKhoa': 'KHOA01', 'TenKhoa': 'Trường ĐHKT'}
        
        # Lookup HP
        thp = hp_dict.get(ma_hp, thp_raw)
        mkhp = khoa_hp_dict.get(ma_hp, 'KHOA01')
        
        # IDs
        mlhp = lhp or f"{ma_hp}_{ma_gv}"
        sid = f"{ma_sv}_{mlhp}_{ma_gv}_{file_name}"
        
        # Kết quả dạng list (nhanh hơn dict)
        results.append([
            sid, ma_sv, hd, ten, ns,
            ml, lop, cn['MaChuyenNganh'], cn['TenChuyenNganh'],
            cn['MaNganh'], cn['TenNganh'], cn['MaKhoa'], cn['TenKhoa'],
            ma_hp, thp, mkhp, '',
            ma_gv, hdgv, tgv, mlhp, lhp,
            ch, gt, essay,
            t1, t2, t3, t4, sent, valid
        ])
    
    return results

# Column names
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

def parse_survey(content):
    print(f"Parsing {NUM_WORKERS} workers...")
    t0 = time.time()
    
    lines = [l for l in content.split('\n') if l.strip()]
    print(f"  {len(lines):,} lines | {len(lines)//CHUNK_SIZE + 1} batches")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_results = []
    with Pool(NUM_WORKERS) as pool:
        for i, res in enumerate(pool.imap_unordered(parse_batch, batches)):
            all_results.extend(res)
            if (i+1) % 5 == 0 or i == len(batches)-1:
                print(f"  Batch {i+1}/{len(batches)}: {len(res):,} rows | Total: {len(all_results):,}")
    
    # Tạo DataFrame 1 lần duy nhất
    df = pd.DataFrame(all_results, columns=COLUMNS)
    
    # Clean types
    for c in ['Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Is_Valid']:
        df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0).astype('int8')
    
    print(f"  ✅ {len(df):,} rows ({time.time()-t0:.1f}s)")
    return df

# ================= LOAD DB (TỐI ƯU) =================
def load_db_fast(cursor, df):
    """Load database tối ưu - giảm số lần query"""
    mhk, nh, hk = derive_ma_hoc_ky()
    
    # ========== 1. INSERT MISSING INTO DIM_HOC_KY ==========
    cursor.execute("IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy=?) INSERT INTO DIM_HOC_KY VALUES(?,?,?)",
                   (mhk, mhk, nh, hk))
    cursor.connection.commit()
    print(f"  HOC_KY: OK")
    
    # ========== 2. LẤY TẤT CẢ EXISTING IDs (1 lần query mỗi bảng) ==========
    def get_existing(table, col):
        cursor.execute(f"SELECT {col} FROM {table}")
        return {str(r[0]).strip() for r in cursor.fetchall()}
    
    print("  Loading existing IDs...")
    ex_khoa = get_existing('DIM_KHOA', 'MaKhoa')
    ex_cn = get_existing('DIM_CHUYEN_NGANH', 'MaChuyenNganh')
    ex_nganh = get_existing('DIM_NGANH', 'MaNganh')
    ex_hp = get_existing('DIM_HOC_PHAN', 'MaHP')
    ex_gv = get_existing('DIM_GIANG_VIEN', 'MaGV')
    ex_sv = get_existing('DIM_SINH_VIEN', 'MaSV')
    ex_lop = get_existing('DIM_LOP_SINH_VIEN', 'MaLop')
    ex_lhp = get_existing('DIM_LOP_HOC_PHAN', 'MaLopHP')
    
    # ========== 3. INSERT MISSING (BULK) ==========
    def bulk_insert(table, df, cols, id_col, existing_set, update_cols=None):
        if df is None or df.empty: return 0
        df = df.drop_duplicates(id_col).fillna('')
        df['_id'] = df[id_col].astype(str).str.strip()
        new = df[~df['_id'].isin(existing_set)]
        if new.empty: return 0
        
        ph = ','.join(['?']*len(cols))
        q = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph})"
        
        data = [tuple(str(r[c])[:500] if r[c] and pd.notna(r[c]) else '' for c in cols) for _, r in new.iterrows()]
        
        # Insert từng batch
        ins = 0
        for i in range(0, len(data), BATCH_SIZE):
            b = data[i:i+BATCH_SIZE]
            try:
                cursor.fast_executemany = True
                cursor.executemany(q, b)
                cursor.connection.commit()
                ins += len(b)
            except:
                for d in b:
                    try: cursor.execute(q, d); cursor.connection.commit(); ins += 1
                    except: pass
        
        if ins > 0:
            existing_set.update(new['_id'].tolist())
            print(f"    {table}: {ins} new")
        return ins
    
    print("\n  Inserting dimensions...")
    
    # DIM_HOC_PHAN
    bulk_insert('DIM_HOC_PHAN',
                df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}),
                ['MaHP','TenHP','MaKhoa'], 'MaHP', ex_hp)
    
    # DIM_GIANG_VIEN
    bulk_insert('DIM_GIANG_VIEN',
                df[['MaGV','HoDemGV','TenGV']],
                ['MaGV','HoDemGV','TenGV'], 'MaGV', ex_gv)
    
    # DIM_LOP_SINH_VIEN
    bulk_insert('DIM_LOP_SINH_VIEN',
                df[['MaLop','Lop','MaChuyenNganh']],
                ['MaLop','Lop','MaChuyenNganh'], 'MaLop', ex_lop)
    
    # DIM_SINH_VIEN
    sv_cols = ['MaSV','HoDem','Ten','NgaySinh','MaLop']
    df_sv = df[sv_cols].drop_duplicates('MaSV').fillna('')
    df_sv['_id'] = df_sv['MaSV'].astype(str).str.strip()
    sv_new = df_sv[~df_sv['_id'].isin(ex_sv)]
    if not sv_new.empty:
        data = []
        for _, r in sv_new.iterrows():
            ns = None
            try:
                dt = pd.to_datetime(r['NgaySinh'], format='%d/%m/%Y')
                ns = dt.strftime('%Y-%m-%d')
            except: pass
            data.append((str(r['MaSV'])[:20], str(r['HoDem'])[:100], str(r['Ten'])[:50], ns, str(r['MaLop'])[:20]))
        
        ins = 0
        q = "INSERT INTO DIM_SINH_VIEN (MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES (?,?,?,?,?)"
        for i in range(0, len(data), BATCH_SIZE):
            b = data[i:i+BATCH_SIZE]
            try:
                cursor.executemany(q, b); cursor.connection.commit(); ins += len(b)
            except:
                for d in b:
                    try: cursor.execute(q, d); cursor.connection.commit(); ins += 1
                    except: pass
        if ins > 0:
            ex_sv.update(sv_new['_id'].tolist())
            print(f"    DIM_SINH_VIEN: {ins} new")
    
    # DIM_LOP_HOC_PHAN
    dlhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    dlhp['MaHocKy'] = mhk
    bulk_insert('DIM_LOP_HOC_PHAN', dlhp,
                ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], 'MaLopHP', ex_lhp)
    
    # ========== 4. LOAD FACTS ==========
    print("\n  Inserting facts...")
    
    # FACT_GOP_Y - dùng MERGE hoặc INSERT với NOT EXISTS
    de = df[(df['EssayText']!='') & (df['EssayText'].notna())].drop_duplicates('SubmissionID')
    if not de.empty:
        ex_sub = get_existing('FACT_GOP_Y_TU_LUAN', 'SubmissionID')
        dn = de[~de['SubmissionID'].astype(str).str.strip().isin(ex_sub)]
        
        if not dn.empty:
            print(f"    FACT_GOP_Y: {len(dn):,} new")
            # Tạo temp table để bulk insert nhanh hơn
            data = [(str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                     str(r['EssayText']), str(r['Sentiment'])[:20], int(r['Is_Valid']),
                     int(r['Tag_HocPhan']), int(r['Tag_DayHoc']), int(r['Tag_KiemTra']), int(r['Tag_Khac']))
                    for _, r in dn.iterrows()]
            
            q = "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)"
            ins = 0
            for i in range(0, len(data), BATCH_SIZE):
                b = data[i:i+BATCH_SIZE]
                try:
                    cursor.executemany(q, b); cursor.connection.commit(); ins += len(b)
                except:
                    for d in b:
                        try: cursor.execute(q, d); cursor.connection.commit(); ins += 1
                        except: pass
            print(f"      Inserted: {ins:,}")
    
    # FACT_KET_QUA
    rows = []
    # Trắc nghiệm
    for _, r in df[(df['CauHoi']!='') & (df['GiaTri']!='')].iterrows():
        try:
            mc = int(float(r['CauHoi'])); d = int(float(r['GiaTri']))
            if 1<=mc<=12: rows.append((str(r['SubmissionID'])[:150], mc, d))
        except: pass
    
    # Tự luận
    for _, r in de.iterrows():
        s = r['Sentiment']; d = 5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if rows:
        print(f"    FACT_KET_QUA: {len(rows):,} rows")
        q = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)"
        ins = 0
        for i in range(0, len(rows), BATCH_SIZE):
            b = rows[i:i+BATCH_SIZE]
            try:
                cursor.executemany(q, b); cursor.connection.commit(); ins += len(b)
            except:
                for d in b:
                    try: cursor.execute(q, d); cursor.connection.commit(); ins += 1
                    except: pass
        print(f"      Inserted: {ins:,}")

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Connect...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Load master
    load_master_fast()
    
    # Download & Parse
    print(f"\n📄 Download: {SURVEY_FILE}...", end=" ", flush=True)
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content: print("❌"); sys.exit(1)
    print(f"OK ({len(content):,} bytes)")
    
    print("\n📝 PARSE + NLP")
    t1 = time.time()
    df = parse_survey(content)
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    if df.empty: print("❌ No data"); sys.exit(1)
    
    # Backup
    bp = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(bp, index=False)
    print(f"📁 Backup: {bp}")
    
    # Load DB
    print("\n💾 LOAD DATABASE")
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        load_db_fast(cursor, df)
        print(f"  ✅ Load: {time.time()-t1:.1f}s")
    except Exception as e:
        print(f"  ❌ {e}")
        import traceback; traceback.print_exc()
    finally:
        conn.close()
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
