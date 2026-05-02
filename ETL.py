#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - VECTORIZED (FIXED)
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
BATCH_SIZE = 100000

print("=" * 70)
print("📊 PIPELINE 2: SURVEY DATA (VECTORIZED)")
print(f"   File: {SURVEY_FILE}")
print("=" * 70)

# ================= PATTERNS =================
_DATE_RE = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_MA_GV_RE = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_LOP_RE = re.compile(r'^\d{2}K\d{2}$')

# ================= NLP KEYWORDS =================
_TAG_KW = {
    'Tag_HocPhan': ['nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức', 'chuẩn đầu ra', 'tài liệu', 'giáo trình', 'thực hành', 'lý thuyết'],
    'Tag_DayHoc': ['giảng viên', 'thầy', 'cô', 'dạy', 'giảng', 'truyền đạt', 'hướng dẫn', 'nhiệt tình', 'tận tâm', 'dễ hiểu'],
    'Tag_KiemTra': ['kiểm tra', 'đánh giá', 'thi', 'đề thi', 'chấm điểm', 'công bằng', 'minh bạch', 'khách quan'],
    'Tag_Khac': ['cơ sở vật chất', 'phòng học', 'máy chiếu', 'wifi', 'hỗ trợ', 'góp ý']
}

_SENT_POS = ['tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả', 'chất lượng', 'tuyệt vời', 'nhiệt tình', 'dễ hiểu', 'công bằng']
_SENT_NEG = ['tệ', 'kém', 'chán', 'dở', 'không tốt', 'khó hiểu', 'nhàm chán', 'thiếu', 'hạn chế', 'thất vọng', 'cần cải thiện']
_SENT_NEU = ['không có góp ý', 'không ý kiến', 'không có', 'bình thường']

# ================= GLOBAL CACHE =================
_g_cn = {}
_g_hp = {}
_g_khoa_hp = {}

def load_master():
    global _g_cn, _g_hp, _g_khoa_hp
    print("📚 Load master...", end=" ", flush=True)
    conn = pyodbc.connect(CONN_STR)
    c = conn.cursor()
    
    # Sửa: thêm alias rõ ràng, chỉ lấy từ DIM_CHUYEN_NGANH
    c.execute("""
        SELECT cn.MaChuyenNganh, cn.TenChuyenNganh, cn.MaNganh, 
               n.TenNganh, n.MaKhoa, k.TenKhoa
        FROM DIM_CHUYEN_NGANH cn
        JOIN DIM_NGANH n ON cn.MaNganh = n.MaNganh
        JOIN DIM_KHOA k ON n.MaKhoa = k.MaKhoa
    """)
    
    for row in c.fetchall():
        key = str(row[0] or '').strip()
        if key:
            _g_cn[key] = {
                'MaChuyenNganh': key,
                'TenChuyenNganh': str(row[1] or '').strip(),
                'MaNganh': str(row[2] or '').strip(),
                'TenNganh': str(row[3] or '').strip(),
                'MaKhoa': str(row[4] or '').strip(),
                'TenKhoa': str(row[5] or '').strip()
            }
    
    c.execute("SELECT MaHP, TenHP, MaKhoa FROM DIM_HOC_PHAN")
    for row in c.fetchall():
        key = str(row[0] or '').strip()
        if key:
            _g_hp[key] = str(row[1] or '').strip()
            _g_khoa_hp[key] = str(row[2] or 'KHOA01').strip()
    
    conn.close()
    print(f"CN={len(_g_cn)}, HP={len(_g_hp)}")

# ================= BLOB =================
def download_blob(bs, container, path):
    try:
        cl = bs.get_container_client(container).get_blob_client(path)
        return cl.download_blob().readall().decode('utf-8-sig') if cl.exists() else ""
    except:
        return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    fn = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    yc = int(fn[:-1])
    hk = int(fn[-1])
    nb = 2000 + yc - 1
    nk = nb + 1
    return f"HK{hk}_{nb%100}{nk%100}", f"{nb}-{nk}", hk

# ================= PARSE VECTORIZED =================
def parse_vectorized(content):
    """Parse toàn bộ dùng pandas vectorized"""
    print("Parsing vectorized...")
    t0 = time.time()
    
    # B1: Tạo DataFrame
    lines = [l for l in content.split('\n') if l.strip()]
    print(f"  {len(lines):,} lines")
    
    df = pd.DataFrame({'raw': lines})
    
    # B2: Tìm NULL - VECTORIZED
    df['null_pos'] = df['raw'].str.upper().str.find('NULL')
    
    # Tách left/right
    mask_null = df['null_pos'] >= 0
    
    # Left: từ đầu đến trước NULL
    def get_left(row):
        if row['null_pos'] < 0:
            return row['raw']
        # Tìm dấu phẩy cuối cùng trước NULL
        pos = row['raw'].upper().rfind(',', 0, row['null_pos'])
        if pos > 0:
            return row['raw'][:pos].strip()
        return row['raw'][:row['null_pos']].rstrip(', ')
    
    df['left_raw'] = df.apply(get_left, axis=1)
    
    # Right: sau NULL
    def get_right(row):
        if row['null_pos'] < 0:
            return ''
        right = row['raw'][row['null_pos']+4:]
        return right.lstrip(', ').strip()
    
    df['right_raw'] = df.apply(get_right, axis=1)
    
    # B3: Split thành columns
    left_split = df['left_raw'].str.split(',', expand=True)
    max_cols = left_split.shape[1]
    
    # B4: Tìm ngày sinh
    date_cols = []
    for i in range(2, min(15, max_cols)):
        col = left_split[i].str.strip()
        if col.str.match(_DATE_RE, na=False).any():
            date_cols.append(i)
    
    # Lấy cột ngày sinh đầu tiên cho mỗi dòng
    def find_nsi(row):
        for i in range(2, min(15, max_cols)):
            val = str(row[i]).strip() if pd.notna(row[i]) else ''
            if _DATE_RE.match(val):
                return i
        return -1
    
    df['nsi'] = left_split.apply(find_nsi, axis=1)
    df = df[df['nsi'] >= 2].copy()
    
    # Extract ngày sinh
    def get_col(row, col_idx):
        if col_idx < 0 or col_idx >= max_cols:
            return ''
        return str(row[col_idx]).strip() if pd.notna(row[col_idx]) else ''
    
    df['NgaySinh'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['nsi'])), axis=1)
    
    # B5: Tìm MaGV
    def find_mgi(row):
        nsi = int(row['nsi'])
        for i in range(nsi+1, min(nsi+25, max_cols)):
            val = get_col(row, i)
            if _MA_GV_RE.match(val):
                return i
        return min(max_cols-1, nsi+8)
    
    df['mgi'] = left_split.iloc[df.index].apply(find_mgi, axis=1)
    
    # B6: Extract các trường
    df['Lop'] = left_split[0].str.strip()
    df['MaSV'] = left_split[1].str.strip()
    df['MaLop'] = df['Lop'].apply(lambda x: x.split('.')[0].split('-')[0].split('_')[0] if isinstance(x, str) else '')
    
    df['Ten'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['nsi'])-1), axis=1)
    df['HoDem'] = df.apply(lambda r: ' '.join([get_col(left_split.iloc[r.name], i) for i in range(2, int(r['nsi'])-1) if get_col(left_split.iloc[r.name], i)]), axis=1)
    
    df['MaHP'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['nsi'])+1), axis=1)
    df['TenHP_raw'] = df.apply(lambda r: ' '.join([get_col(left_split.iloc[r.name], i) for i in range(int(r['nsi'])+2, int(r['mgi'])) if get_col(left_split.iloc[r.name], i)]), axis=1)
    
    df['MaGV'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])), axis=1)
    df['HoDemGV'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])+1), axis=1)
    df['TenGV'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])+2), axis=1)
    df['LopHP'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])+3), axis=1)
    df['CauHoi'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])+4), axis=1)
    df['GiaTri'] = df.apply(lambda r: get_col(left_split.iloc[r.name], int(r['mgi'])+5), axis=1)
    
    df['EssayText'] = df['right_raw'].str.strip()
    
    # B7: Lookup Chuyên ngành
    def lookup_cn_vec(lop):
        if not isinstance(lop, str):
            return ('K01', 'CN K01', 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT')
        lop = lop.strip()
        for sep in ['.', '-', '_']:
            if sep in lop:
                lop = lop.split(sep)[0]
        
        if _LOP_RE.match(lop):
            ma_cn = f"K{lop[3:5]}"
            cn = _g_cn.get(ma_cn)
            if cn:
                return (cn['MaChuyenNganh'], cn['TenChuyenNganh'], cn['MaNganh'],
                        cn['TenNganh'], cn['MaKhoa'], cn['TenKhoa'])
            return (ma_cn, f'CN {ma_cn}', 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT')
        return (lop, lop, 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT')
    
    cn_tuples = df['Lop'].apply(lookup_cn_vec)
    df['MaChuyenNganh'] = [t[0] for t in cn_tuples]
    df['TenChuyenNganh'] = [t[1] for t in cn_tuples]
    df['MaNganh'] = [t[2] for t in cn_tuples]
    df['TenNganh'] = [t[3] for t in cn_tuples]
    df['MaKhoa_CN'] = [t[4] for t in cn_tuples]
    df['TenKhoa_CN'] = [t[5] for t in cn_tuples]
    
    # B8: Lookup HP
    df['TenHP'] = df['MaHP'].map(_g_hp).fillna(df['TenHP_raw'])
    df['MaKhoa_HP'] = df['MaHP'].map(_g_khoa_hp).fillna('KHOA01')
    df['TenKhoa_HP'] = ''
    
    # B9: IDs
    df['MaLopHP'] = np.where(df['LopHP'] != '', df['LopHP'], df['MaHP'] + '_' + df['MaGV'])
    df['SubmissionID'] = df['MaSV'] + '_' + df['MaLopHP'] + '_' + df['MaGV'] + '_' + FILE_NAME
    
    # B10: NLP
    print("  NLP...")
    
    def nlp_batch(texts):
        """Xử lý NLP cho list texts"""
        res = {'Tag_HocPhan': [], 'Tag_DayHoc': [], 'Tag_KiemTra': [], 'Tag_Khac': [],
               'Sentiment': [], 'Is_Valid': []}
        
        for text in texts:
            if not isinstance(text, str) or len(text) < 5:
                res['Tag_HocPhan'].append(0); res['Tag_DayHoc'].append(0)
                res['Tag_KiemTra'].append(0); res['Tag_Khac'].append(0)
                res['Sentiment'].append('NEUTRAL'); res['Is_Valid'].append(0)
                continue
            
            tl = text.lower()
            res['Tag_HocPhan'].append(1 if sum(1 for k in _TAG_KW['Tag_HocPhan'] if k in tl) >= 2 else 0)
            res['Tag_DayHoc'].append(1 if sum(1 for k in _TAG_KW['Tag_DayHoc'] if k in tl) >= 2 else 0)
            res['Tag_KiemTra'].append(1 if sum(1 for k in _TAG_KW['Tag_KiemTra'] if k in tl) >= 2 else 0)
            res['Tag_Khac'].append(1 if sum(1 for k in _TAG_KW['Tag_Khac'] if k in tl) >= 2 else 0)
            
            p = sum(1 for k in _SENT_POS if k in tl)
            n = sum(1 for k in _SENT_NEG if k in tl) + (1 if 'không' in tl else 0)
            e = sum(1 for k in _SENT_NEU if k in tl)
            
            s = 'POSITIVE' if p > n and p > e else ('NEGATIVE' if n > p else 'NEUTRAL')
            res['Sentiment'].append(s)
            res['Is_Valid'].append(1 if len(tl) > 10 else 0)
        
        return res
    
    texts = df['EssayText'].tolist()
    
    # Xử lý theo batch để tránh memory
    batch_nlp = 50000
    all_nlp = {k: [] for k in ['Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid']}
    
    for i in range(0, len(texts), batch_nlp):
        batch_texts = texts[i:i+batch_nlp]
        batch_res = nlp_batch(batch_texts)
        for k in all_nlp:
            all_nlp[k].extend(batch_res[k])
    
    for k in all_nlp:
        df[k] = all_nlp[k]
    
    # B11: Output
    output_cols = [
        'SubmissionID', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
        'MaLop', 'Lop', 'MaChuyenNganh', 'TenChuyenNganh',
        'MaNganh', 'TenNganh', 'MaKhoa_CN', 'TenKhoa_CN',
        'MaHP', 'TenHP', 'MaKhoa_HP', 'TenKhoa_HP',
        'MaGV', 'HoDemGV', 'TenGV', 'MaLopHP', 'LopHP',
        'CauHoi', 'GiaTri', 'EssayText',
        'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac',
        'Sentiment', 'Is_Valid'
    ]
    
    df_out = df[output_cols].fillna('')
    print(f"  ✅ {len(df_out):,} rows ({time.time()-t0:.1f}s)")
    
    return df_out

# ================= LOAD DB =================
def load_db(cursor, df):
    mhk, nh, hk = derive_ma_hoc_ky()
    
    # DIM_HOC_KY
    cursor.execute("IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy=?) INSERT INTO DIM_HOC_KY VALUES(?,?,?)",
                   (mhk, mhk, nh, hk))
    cursor.connection.commit()
    
    # Lấy existing IDs
    def get_ids(table, col):
        cursor.execute(f"SELECT {col} FROM {table}")
        return {str(r[0]).strip() for r in cursor.fetchall()}
    
    print("  Loading existing IDs...")
    ex_hp = get_ids('DIM_HOC_PHAN', 'MaHP')
    ex_gv = get_ids('DIM_GIANG_VIEN', 'MaGV')
    ex_sv = get_ids('DIM_SINH_VIEN', 'MaSV')
    ex_lop = get_ids('DIM_LOP_SINH_VIEN', 'MaLop')
    ex_lhp = get_ids('DIM_LOP_HOC_PHAN', 'MaLopHP')
    ex_sub = get_ids('FACT_GOP_Y_TU_LUAN', 'SubmissionID')
    
    def bulk_ins(table, df_in, cols, id_col, existing):
        if df_in is None or df_in.empty:
            return
        df_in = df_in.drop_duplicates(id_col).fillna('').astype(str)
        new = df_in[~df_in[id_col].isin(existing)]
        if new.empty:
            return
        
        data = []
        for _, r in new.iterrows():
            row_data = []
            for c in cols:
                val = r.get(c, '')
                row_data.append(str(val)[:500] if val else '')
            data.append(tuple(row_data))
        
        q = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
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
                    try:
                        cursor.execute(q, d)
                        cursor.connection.commit()
                        ins += 1
                    except:
                        pass
        if ins > 0:
            existing.update(new[id_col].tolist())
            print(f"    {table}: {ins}")
    
    print("\n  Inserting dimensions...")
    
    bulk_ins('DIM_HOC_PHAN',
             df[['MaHP', 'TenHP', 'MaKhoa_HP']].rename(columns={'MaKhoa_HP': 'MaKhoa'}),
             ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP', ex_hp)
    
    bulk_ins('DIM_GIANG_VIEN',
             df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV'),
             ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV', ex_gv)
    
    bulk_ins('DIM_LOP_SINH_VIEN',
             df[['MaLop', 'Lop', 'MaChuyenNganh']].drop_duplicates('MaLop'),
             ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop', ex_lop)
    
    # DIM_SINH_VIEN
    sv = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']].drop_duplicates('MaSV').fillna('')
    sv['_id'] = sv['MaSV'].astype(str).str.strip()
    sv_new = sv[~sv['_id'].isin(ex_sv)]
    if not sv_new.empty:
        data = []
        for _, r in sv_new.iterrows():
            ns = None
            try:
                dt = pd.to_datetime(r['NgaySinh'], format='%d/%m/%Y')
                ns = dt.strftime('%Y-%m-%d')
            except:
                pass
            data.append((str(r['MaSV'])[:20], str(r['HoDem'])[:100], str(r['Ten'])[:50], ns, str(r['MaLop'])[:20]))
        
        q = "INSERT INTO DIM_SINH_VIEN (MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES (?,?,?,?,?)"
        ins = 0
        for i in range(0, len(data), BATCH_SIZE):
            b = data[i:i+BATCH_SIZE]
            try:
                cursor.executemany(q, b)
                cursor.connection.commit()
                ins += len(b)
            except:
                for d in b:
                    try:
                        cursor.execute(q, d)
                        cursor.connection.commit()
                        ins += 1
                    except:
                        pass
        if ins > 0:
            ex_sv.update(sv_new['_id'].tolist())
            print(f"    DIM_SINH_VIEN: {ins}")
    
    # DIM_LOP_HOC_PHAN
    lhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates('MaLopHP').fillna('')
    lhp['MaHocKy'] = mhk
    bulk_ins('DIM_LOP_HOC_PHAN', lhp,
             ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP', ex_lhp)
    
    # FACTS
    print("\n  Inserting facts...")
    
    de = df[(df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    if not de.empty:
        dn = de[~de['SubmissionID'].str.strip().isin(ex_sub)]
        if not dn.empty:
            print(f"    FACT_GOP_Y: {len(dn):,}")
            data = [(str(r['SubmissionID'])[:150], str(r['MaSV'])[:20], str(r['MaLopHP'])[:50],
                     str(r['EssayText']), str(r['Sentiment'])[:20], int(r['Is_Valid']),
                     int(r['Tag_HocPhan']), int(r['Tag_DayHoc']), int(r['Tag_KiemTra']), int(r['Tag_Khac']))
                    for _, r in dn.iterrows()]
            
            q = "INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)"
            for i in range(0, len(data), BATCH_SIZE):
                try:
                    cursor.executemany(q, data[i:i+BATCH_SIZE])
                    cursor.connection.commit()
                except:
                    for d in data[i:i+BATCH_SIZE]:
                        try:
                            cursor.execute(q, d)
                            cursor.connection.commit()
                        except:
                            pass
    
    # FACT_KET_QUA
    rows = []
    for _, r in df[(df['CauHoi'] != '') & (df['GiaTri'] != '')].iterrows():
        try:
            mc = int(float(r['CauHoi']))
            d = int(float(r['GiaTri']))
            if 1 <= mc <= 12:
                rows.append((str(r['SubmissionID'])[:150], mc, d))
        except:
            pass
    
    for _, r in de.iterrows():
        s = r['Sentiment']
        d = 5 if s == 'POSITIVE' else (2 if s == 'NEGATIVE' else 3)
        for mc in [13, 14, 15, 16]:
            rows.append((str(r['SubmissionID'])[:150], mc, d))
    
    if rows:
        print(f"    FACT_KET_QUA: {len(rows):,}")
        q = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)"
        for i in range(0, len(rows), BATCH_SIZE):
            try:
                cursor.executemany(q, rows[i:i+BATCH_SIZE])
                cursor.connection.commit()
            except:
                for d in rows[i:i+BATCH_SIZE]:
                    try:
                        cursor.execute(q, d)
                        cursor.connection.commit()
                    except:
                        pass

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Connect...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master()
    
    print(f"\n📄 Download: {SURVEY_FILE}...", end=" ", flush=True)
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content:
        print("❌")
        sys.exit(1)
    print(f"OK ({len(content):,} bytes)")
    
    print(f"\n📝 PARSE + NLP (Vectorized)")
    t1 = time.time()
    df = parse_vectorized(content)
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    if df.empty:
        print("❌ No data")
        sys.exit(1)
    
    # Backup
    bp = f"/tmp/{FILE_NAME}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    df.to_parquet(bp, index=False)
    print(f"📁 {bp}")
    
    # Load DB
    print("\n💾 LOAD DATABASE")
    t1 = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        load_db(cursor, df)
        print(f"  ✅ {time.time()-t1:.1f}s")
    except Exception as e:
        print(f"  ❌ {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
