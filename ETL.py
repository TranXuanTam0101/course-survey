#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2: SURVEY DATA - VECTORIZED
- Parse CSV bằng pandas vectorized
- NLP vectorized
- Bulk insert database
- KHÔNG xử lý từng dòng
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

_SENT_POS = ['tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả', 'chất lượng', 'tuyệt vời', 'xuất sắc', 'nhiệt tình', 'dễ hiểu', 'công bằng']
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
    
    c.execute("SELECT MaChuyenNganh,TenChuyenNganh,MaNganh,n.TenNganh,n.MaKhoa,k.TenKhoa FROM DIM_CHUYEN_NGANH cn JOIN DIM_NGANH n ON cn.MaNganh=n.MaNganh JOIN DIM_KHOA k ON n.MaKhoa=k.MaKhoa")
    for r in c.fetchall():
        _g_cn[str(r[0]).strip()] = {'MaChuyenNganh':str(r[0]).strip(),'TenChuyenNganh':str(r[1] or '').strip(),'MaNganh':str(r[2]).strip(),'TenNganh':str(r[3] or '').strip(),'MaKhoa':str(r[4]).strip(),'TenKhoa':str(r[5] or '').strip()}
    
    c.execute("SELECT MaHP,TenHP,MaKhoa FROM DIM_HOC_PHAN")
    for r in c.fetchall():
        k = str(r[0]).strip()
        _g_hp[k] = str(r[1] or '').strip()
        _g_khoa_hp[k] = str(r[2] or 'KHOA01').strip()
    
    conn.close()
    print(f"CN={len(_g_cn)}, HP={len(_g_hp)}")

# ================= BLOB =================
def download_blob(bs, container, path):
    try:
        cl = bs.get_container_client(container).get_blob_client(path)
        return cl.download_blob().readall().decode('utf-8-sig') if cl.exists() else ""
    except: return ""

# ================= UTILS =================
def derive_ma_hoc_ky():
    fn = SURVEY_FILE.replace('.csv','').split('_')[-1]
    yc = int(fn[:-1]); hk = int(fn[-1])
    nb = 2000+yc-1; nk = nb+1
    return f"HK{hk}_{nb%100}{nk%100}", f"{nb}-{nk}", hk

# ================= PARSE VECTORIZED =================
def parse_vectorized(content):
    """Parse toàn bộ dùng pandas vectorized - KHÔNG loop từng dòng"""
    print("Parsing vectorized...")
    t0 = time.time()
    
    # B1: Tách dòng, tìm vị trí NULL
    lines = content.split('\n')
    lines = [l for l in lines if l.strip()]
    print(f"  {len(lines):,} lines")
    
    # Tạo DataFrame gốc
    df = pd.DataFrame({'raw': lines})
    
    # B2: Tìm vị trí NULL - VECTORIZED
    df['null_pos'] = df['raw'].str.upper().str.find('NULL')
    
    # Tách left/right
    mask_has_null = df['null_pos'] >= 0
    df['left_raw'] = df['raw']
    df['right_raw'] = ''
    df.loc[mask_has_null, 'left_raw'] = df.loc[mask_has_null].apply(
        lambda r: r['raw'][:r['raw'].upper().rfind(',', 0, r['null_pos'])].rstrip(', ') 
        if r['raw'].upper().rfind(',', 0, r['null_pos']) > 0 
        else r['raw'][:r['null_pos']].rstrip(', '), axis=1
    )
    df.loc[mask_has_null, 'right_raw'] = df.loc[mask_has_null, 'raw'].str.extract(r'NULL[,]?(.*)', expand=False).fillna('')
    
    # B3: Split left thành columns - VECTORIZED
    # Tạo DataFrame từ split
    left_split = df['left_raw'].str.split(',', expand=True)
    left_split.columns = [f'c{i}' for i in range(left_split.shape[1])]
    left_split = left_split.apply(lambda x: x.str.strip())
    
    # B4: Tìm cột ngày sinh - VECTORIZED
    date_mask = left_split.apply(lambda col: col.str.match(_DATE_RE, na=False))
    # Lấy vị trí cột đầu tiên có ngày sinh cho mỗi dòng
    df['nsi'] = date_mask.idxmax(axis=1).apply(lambda x: int(x[1:]) if isinstance(x, str) and x.startswith('c') else -1)
    df = df[df['nsi'] >= 2]  # Lọc dòng hợp lệ
    
    # Lấy ngày sinh
    df['NgaySinh'] = left_split.lookup(df.index, df['nsi'].apply(lambda x: f'c{x}'))
    
    # B5: Tìm cột MaGV - VECTORIZED
    gv_mask = left_split.apply(lambda col: col.str.match(_MA_GV_RE, na=False))
    
    def find_gv_col(row):
        nsi = int(row['nsi'])
        for i in range(nsi+1, min(nsi+25, left_split.shape[1])):
            col_name = f'c{i}'
            if col_name in gv_mask.columns and gv_mask.loc[row.name, col_name]:
                return i
        return min(left_split.shape[1]-1, nsi+8)
    
    df['mgi'] = df.apply(find_gv_col, axis=1)
    
    # B6: Extract các cột - VECTORIZED
    df['Lop'] = left_split['c0']
    df['MaSV'] = left_split['c1']
    df['MaLop'] = df['Lop'].apply(lambda x: x.strip().split('-')[0].split('.')[0].split('_')[0] if isinstance(x, str) else '')
    
    # Tên SV
    df['Ten'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["nsi"])-1}'] if r['nsi'] > 0 else '', axis=1)
    df['HoDem'] = df.apply(lambda r: ' '.join([left_split.loc[r.name, f'c{i}'] for i in range(2, int(r['nsi'])-1) if pd.notna(left_split.loc[r.name, f'c{i}'])]), axis=1)
    
    # HP
    df['MaHP'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["nsi"])+1}'] if r['nsi']+1 < left_split.shape[1] else '', axis=1)
    df['TenHP_raw'] = df.apply(lambda r: ' '.join([str(left_split.loc[r.name, f'c{i}']) for i in range(int(r['nsi'])+2, int(r['mgi'])) if pd.notna(left_split.loc[r.name, f'c{i}'])]), axis=1)
    
    # GV
    df['MaGV'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])}'] if r['mgi'] < left_split.shape[1] else '', axis=1)
    df['HoDemGV'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])+1}'] if r['mgi']+1 < left_split.shape[1] else '', axis=1)
    df['TenGV'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])+2}'] if r['mgi']+2 < left_split.shape[1] else '', axis=1)
    df['LopHP'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])+3}'] if r['mgi']+3 < left_split.shape[1] else '', axis=1)
    df['CauHoi'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])+4}'] if r['mgi']+4 < left_split.shape[1] else '', axis=1)
    df['GiaTri'] = df.apply(lambda r: left_split.loc[r.name, f'c{int(r["mgi"])+5}'] if r['mgi']+5 < left_split.shape[1] else '', axis=1)
    
    # Essay
    df['EssayText'] = df['right_raw'].str.strip()
    
    # B7: Lookup CN - VECTORIZED
    def lookup_cn_vec(lop):
        if not isinstance(lop, str): return 'K01', 'CN K01', 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT'
        lop = lop.strip()
        for sep in ['.','-','_']:
            if sep in lop: lop = lop.split(sep)[0]
        
        if _LOP_RE.match(lop):
            ma_cn = f"K{lop[3:5]}"
            cn = _g_cn.get(ma_cn)
            if cn: return cn['MaChuyenNganh'], cn['TenChuyenNganh'], cn['MaNganh'], cn['TenNganh'], cn['MaKhoa'], cn['TenKhoa']
            return ma_cn, f'CN {ma_cn}', 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT'
        return lop, lop, 'KHOA01NG01', 'Ngành', 'KHOA01', 'Trường ĐHKT'
    
    cn_cols = df['Lop'].apply(lookup_cn_vec)
    df['MaChuyenNganh'] = [c[0] for c in cn_cols]
    df['TenChuyenNganh'] = [c[1] for c in cn_cols]
    df['MaNganh'] = [c[2] for c in cn_cols]
    df['TenNganh'] = [c[3] for c in cn_cols]
    df['MaKhoa_CN'] = [c[4] for c in cn_cols]
    df['TenKhoa_CN'] = [c[5] for c in cn_cols]
    
    # B8: Lookup HP - VECTORIZED
    df['TenHP'] = df['MaHP'].map(_g_hp).fillna(df['TenHP_raw'])
    df['MaKhoa_HP'] = df['MaHP'].map(_g_khoa_hp).fillna('KHOA01')
    df['TenKhoa_HP'] = ''
    
    # B9: Tạo IDs
    df['MaLopHP'] = df['LopHP'].where(df['LopHP']!='', df['MaHP']+'_'+df['MaGV'])
    df['SubmissionID'] = df['MaSV']+'_'+df['MaLopHP']+'_'+df['MaGV']+'_'+FILE_NAME
    
    # B10: NLP - VECTORIZED
    print("  NLP...")
    
    def nlp_vectorized(texts):
        """Xử lý NLP cho toàn bộ series"""
        results = {'Tag_HocPhan':[], 'Tag_DayHoc':[], 'Tag_KiemTra':[], 'Tag_Khac':[],
                   'Sentiment':[], 'Is_Valid':[]}
        
        for text in texts:
            if not isinstance(text, str) or len(text) < 5:
                results['Tag_HocPhan'].append(0); results['Tag_DayHoc'].append(0)
                results['Tag_KiemTra'].append(0); results['Tag_Khac'].append(0)
                results['Sentiment'].append('NEUTRAL'); results['Is_Valid'].append(0)
                continue
            
            tl = text.lower()
            
            # Tags
            results['Tag_HocPhan'].append(1 if sum(1 for k in _TAG_KW['Tag_HocPhan'] if k in tl) >= 2 else 0)
            results['Tag_DayHoc'].append(1 if sum(1 for k in _TAG_KW['Tag_DayHoc'] if k in tl) >= 2 else 0)
            results['Tag_KiemTra'].append(1 if sum(1 for k in _TAG_KW['Tag_KiemTra'] if k in tl) >= 2 else 0)
            results['Tag_Khac'].append(1 if sum(1 for k in _TAG_KW['Tag_Khac'] if k in tl) >= 2 else 0)
            
            # Sentiment
            p = sum(1 for k in _SENT_POS if k in tl)
            n = sum(1 for k in _SENT_NEG if k in tl) + (1 if 'không' in tl else 0)
            e = sum(1 for k in _SENT_NEU if k in tl)
            
            if p > n and p > e: s = 'POSITIVE'
            elif n > p: s = 'NEGATIVE'
            else: s = 'NEUTRAL'
            results['Sentiment'].append(s)
            
            results['Is_Valid'].append(1 if len(tl) > 10 else 0)
        
        return results
    
    # Áp dụng NLP
    essay_texts = df['EssayText'].tolist()
    nlp_res = nlp_vectorized(essay_texts)
    
    for col in ['Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac','Sentiment','Is_Valid']:
        df[col] = nlp_res[col]
    
    # B11: Chọn columns output
    output_cols = [
        'SubmissionID','MaSV','HoDem','Ten','NgaySinh',
        'MaLop','Lop','MaChuyenNganh','TenChuyenNganh',
        'MaNganh','TenNganh','MaKhoa_CN','TenKhoa_CN',
        'MaHP','TenHP','MaKhoa_HP','TenKhoa_HP',
        'MaGV','HoDemGV','TenGV','MaLopHP','LopHP',
        'CauHoi','GiaTri','EssayText',
        'Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac',
        'Sentiment','Is_Valid'
    ]
    
    df_out = df[output_cols].fillna('')
    print(f"  ✅ {len(df_out):,} rows ({time.time()-t0:.1f}s)")
    
    return df_out

# ================= LOAD DB =================
def load_db(cursor, df):
    mhk, nh, hk = derive_ma_hoc_ky()
    
    # DIM_HOC_KY
    cursor.execute("IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy=?) INSERT INTO DIM_HOC_KY VALUES(?,?,?)", (mhk,mhk,nh,hk))
    cursor.connection.commit()
    
    # Lấy existing IDs
    def get_ids(table, col):
        cursor.execute(f"SELECT {col} FROM {table}")
        return {str(r[0]).strip() for r in cursor.fetchall()}
    
    print("  Loading existing IDs...")
    ex_gv = get_ids('DIM_GIANG_VIEN','MaGV')
    ex_sv = get_ids('DIM_SINH_VIEN','MaSV')
    ex_lop = get_ids('DIM_LOP_SINH_VIEN','MaLop')
    ex_hp = get_ids('DIM_HOC_PHAN','MaHP')
    ex_lhp = get_ids('DIM_LOP_HOC_PHAN','MaLopHP')
    ex_sub = get_ids('FACT_GOP_Y_TU_LUAN','SubmissionID')
    
    # Bulk insert helper
    def bulk_ins(table, df, cols, id_col, existing):
        if df is None or df.empty: return
        df = df.drop_duplicates(id_col).fillna('').astype(str)
        new = df[~df[id_col].isin(existing)]
        if new.empty: return
        
        data = [tuple(r[c][:500] if c in r.index and r[c] else '' for c in cols) for _, r in new.iterrows()]
        q = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?']*len(cols))})"
        
        ins = 0
        for i in range(0, len(data), BATCH_SIZE):
            b = data[i:i+BATCH_SIZE]
            try:
                cursor.executemany(q, b); cursor.connection.commit(); ins += len(b)
            except:
                for d in b:
                    try: cursor.execute(q, d); cursor.connection.commit(); ins += 1
                    except: pass
        if ins > 0:
            existing.update(new[id_col].tolist())
            print(f"    {table}: {ins}")
    
    print("\n  Inserting dimensions...")
    bulk_ins('DIM_HOC_PHAN', df[['MaHP','TenHP','MaKhoa_HP']].rename(columns={'MaKhoa_HP':'MaKhoa'}), ['MaHP','TenHP','MaKhoa'], 'MaHP', ex_hp)
    bulk_ins('DIM_GIANG_VIEN', df[['MaGV','HoDemGV','TenGV']].drop_duplicates('MaGV'), ['MaGV','HoDemGV','TenGV'], 'MaGV', ex_gv)
    bulk_ins('DIM_LOP_SINH_VIEN', df[['MaLop','Lop','MaChuyenNganh']].drop_duplicates('MaLop'), ['MaLop','Lop','MaChuyenNganh'], 'MaLop', ex_lop)
    
    # DIM_SINH_VIEN - xử lý ngày sinh
    sv = df[['MaSV','HoDem','Ten','NgaySinh','MaLop']].drop_duplicates('MaSV').fillna('')
    sv['_id'] = sv['MaSV'].astype(str).str.strip()
    sv_new = sv[~sv['_id'].isin(ex_sv)]
    if not sv_new.empty:
        data = []
        for _,r in sv_new.iterrows():
            ns = None
            try: dt=pd.to_datetime(r['NgaySinh'],format='%d/%m/%Y'); ns=dt.strftime('%Y-%m-%d')
            except: pass
            data.append((str(r['MaSV'])[:20],str(r['HoDem'])[:100],str(r['Ten'])[:50],ns,str(r['MaLop'])[:20]))
        q="INSERT INTO DIM_SINH_VIEN (MaSV,HoDem,Ten,NgaySinh,MaLop) VALUES (?,?,?,?,?)"
        ins=0
        for i in range(0,len(data),BATCH_SIZE):
            b=data[i:i+BATCH_SIZE]
            try: cursor.executemany(q,b); cursor.connection.commit(); ins+=len(b)
            except:
                for d in b:
                    try: cursor.execute(q,d); cursor.connection.commit(); ins+=1
                    except: pass
        if ins>0: ex_sv.update(sv_new['_id'].tolist()); print(f"    DIM_SINH_VIEN: {ins}")
    
    # DIM_LOP_HOC_PHAN
    lhp = df[['MaLopHP','LopHP','MaHP','MaGV']].drop_duplicates('MaLopHP').fillna('')
    lhp['MaHocKy'] = mhk
    bulk_ins('DIM_LOP_HOC_PHAN', lhp, ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy'], 'MaLopHP', ex_lhp)
    
    # FACTS
    print("\n  Inserting facts...")
    de = df[(df['EssayText']!='')].drop_duplicates('SubmissionID')
    
    if not de.empty:
        dn = de[~de['SubmissionID'].str.strip().isin(ex_sub)]
        if not dn.empty:
            print(f"    FACT_GOP_Y: {len(dn):,}")
            data = [(str(r['SubmissionID'])[:150],str(r['MaSV'])[:20],str(r['MaLopHP'])[:50],str(r['EssayText']),str(r['Sentiment'])[:20],int(r['Is_Valid']),int(r['Tag_HocPhan']),int(r['Tag_DayHoc']),int(r['Tag_KiemTra']),int(r['Tag_Khac'])) for _,r in dn.iterrows()]
            q="INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac) VALUES (?,?,?,?,?,?,?,?,?,?)"
            for i in range(0,len(data),BATCH_SIZE):
                try: cursor.executemany(q,data[i:i+BATCH_SIZE]); cursor.connection.commit()
                except:
                    for d in data[i:i+BATCH_SIZE]:
                        try: cursor.execute(q,d); cursor.connection.commit()
                        except: pass
    
    # FACT_KET_QUA
    rows = []
    for _,r in df[(df['CauHoi']!='')&(df['GiaTri']!='')].iterrows():
        try:
            mc=int(float(r['CauHoi'])); d=int(float(r['GiaTri']))
            if 1<=mc<=12: rows.append((str(r['SubmissionID'])[:150],mc,d))
        except: pass
    for _,r in de.iterrows():
        s=r['Sentiment']; d=5 if s=='POSITIVE' else (2 if s=='NEGATIVE' else 3)
        for mc in [13,14,15,16]: rows.append((str(r['SubmissionID'])[:150],mc,d))
    
    if rows:
        print(f"    FACT_KET_QUA: {len(rows):,}")
        q="INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID,MaCauHoi,Diem) VALUES (?,?,?)"
        for i in range(0,len(rows),BATCH_SIZE):
            try: cursor.executemany(q,rows[i:i+BATCH_SIZE]); cursor.connection.commit()
            except:
                for d in rows[i:i+BATCH_SIZE]:
                    try: cursor.execute(q,d); cursor.connection.commit()
                    except: pass

# ================= MAIN =================
def main():
    t0 = time.time()
    
    print("\n📥 Connect...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    load_master()
    
    print(f"\n📄 Download: {SURVEY_FILE}...", end=" ", flush=True)
    content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    if not content: print("❌"); sys.exit(1)
    print("OK")
    
    print(f"\n📝 PARSE + NLP (Vectorized)")
    t1 = time.time()
    df = parse_vectorized(content)
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    if df.empty: print("❌"); sys.exit(1)
    
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
    load_db(cursor, df)
    conn.close()
    print(f"  ✅ {time.time()-t1:.1f}s")
    
    print(f"\n🎉 DONE! Total: {time.time()-t0:.1f}s | Rows: {len(df):,}")

if __name__ == "__main__":
    main()
