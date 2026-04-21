#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - LOAD TO DATABASE
- Đọc dữ liệu từ file Parquet
- Load vào SQL Server
"""

import os
import sys
import time
import pickle
import pandas as pd
import pyodbc

# ================= CONFIG =================
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/tmp/etl_output")

# ODBC Connection
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
)

BATCH_SIZE = 25000

# ========== HELPER FUNCTIONS ==========
def get_existing_ids(cursor, table: str, id_col: str) -> set:
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return {row[0] for row in cursor.fetchall()}

def load_dimension(cursor, table: str, df: pd.DataFrame, columns: list, id_col: str) -> int:
    if df.empty: return 0
    df = df.fillna('')
    existing = get_existing_ids(cursor, table, id_col)
    new_data = df[~df[id_col].isin(existing)]
    if new_data.empty: return 0
    print(f"    -> Inserting {len(new_data)} new records into {table}...")
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    data = []
    for _, row in new_data.iterrows():
        tuple_data = []
        for c in columns:
            if c == 'NgaySinh':
                val = row[c]
                if pd.isna(val) or val == '':
                    tuple_data.append(None)
                else:
                    try:
                        dt = pd.to_datetime(val, format='%d/%m/%Y', errors='coerce')
                        tuple_data.append(dt.strftime('%Y-%m-%d') if pd.notna(dt) else None)
                    except:
                        tuple_data.append(None)
            else:
                val = row[c]
                tuple_data.append(str(val)[:500] if val else '')
        data.append(tuple(tuple_data))
    cursor.executemany(query, data)
    cursor.connection.commit()
    return len(new_data)

def load_fact(cursor, fact_df: pd.DataFrame) -> int:
    if fact_df.empty: return 0
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    valid_sv = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    valid_lhp = {row[0] for row in cursor.fetchall()}
    
    fact_df_valid = fact_df[fact_df['MaSV'].isin(valid_sv) & fact_df['MaLopHP'].isin(valid_lhp)]
    
    if fact_df_valid.empty:
        print("  ❌ KHÔNG CÓ DÒNG NÀO HỢP LỆ!")
        return 0
    
    data = []
    for _, row in fact_df_valid.iterrows():
        sub_id = str(row['SubmissionID'])[:150] if pd.notna(row['SubmissionID']) else ''
        try:
            ma_cau = int(row['MaCauHoi'])
        except:
            ma_cau = 0
        ma_sv = str(row['MaSV'])[:20] if pd.notna(row['MaSV']) else ''
        ma_lop = str(row['MaLopHP'])[:50] if pd.notna(row['MaLopHP']) else ''
        
        tra_loi_so = None
        val = row.get('TraLoiSo')
        if pd.notna(val) and val != '' and val is not None:
            try:
                num = float(val)
                if num > 0: tra_loi_so = num
            except:
                pass
        
        tra_loi_text = None
        val = row.get('TraLoiText')
        if pd.notna(val) and val != '' and val is not None:
            tra_loi_text = str(val)
        
        data.append((sub_id, ma_cau, ma_sv, ma_lop, tra_loi_so, tra_loi_text))
    
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    total = 0
    for i in range(0, len(data), BATCH_SIZE):
        batch = data[i:i+BATCH_SIZE]
        cursor.executemany("""
            INSERT INTO FACT_TRA_LOI_KHAO_SAT 
            (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText)
            VALUES (?, ?, ?, ?, ?, ?)
        """, batch)
        cursor.connection.commit()
        total += len(batch)
        if (i // BATCH_SIZE + 1) % 10 == 0:
            print(f"    -> Đã insert {total:,}/{len(data):,} dòng")
    
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    print(f"  ✅ FACT done: {total:,} dòng ({time.time()-start:.2f}s)")
    return total

def load_from_parquet(output_dir: str):
    """Đọc dữ liệu từ parquet và load vào database"""
    print(f"  -> Đọc dữ liệu từ: {output_dir}")
    
    # Đọc metadata
    with open(os.path.join(output_dir, "metadata.pkl"), 'rb') as f:
        meta = pickle.load(f)
    print(f"  -> Metadata: {meta}")
    
    # Đọc tất cả parquet files
    dims = {}
    for table in ['DIM_HOC_KY', 'DIM_KHOA', 'DIM_CHUYEN_NGANH', 'DIM_HOC_PHAN', 
                  'DIM_GIANG_VIEN', 'DIM_LOP_HOC_PHAN', 'DIM_LOP_SINH_VIEN', 
                  'DIM_SINH_VIEN', 'FACT']:
        filepath = os.path.join(output_dir, f"{table}.parquet")
        if os.path.exists(filepath):
            dims[table] = pd.read_parquet(filepath)
            print(f"  -> Đọc {table}: {len(dims[table]):,} rows")
        else:
            print(f"  ⚠️ Không tìm thấy {filepath}")
            dims[table] = pd.DataFrame()
    
    return dims

def load_to_database(dims: dict):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        count = load_dimension(cursor, 'DIM_HOC_KY', dims.get('DIM_HOC_KY', pd.DataFrame()),
                               ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        
        count = load_dimension(cursor, 'DIM_KHOA', dims.get('DIM_KHOA', pd.DataFrame()),
                               ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        print("  ✅ DIM_CTDT: ensured")
        
        count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', dims.get('DIM_CHUYEN_NGANH', pd.DataFrame()),
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        
        count = load_dimension(cursor, 'DIM_HOC_PHAN', dims.get('DIM_HOC_PHAN', pd.DataFrame()),
                               ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        
        count = load_dimension(cursor, 'DIM_GIANG_VIEN', dims.get('DIM_GIANG_VIEN', pd.DataFrame()),
                               ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', dims.get('DIM_LOP_HOC_PHAN', pd.DataFrame()),
                               ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        
        count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', dims.get('DIM_LOP_SINH_VIEN', pd.DataFrame()),
                               ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        
        count = load_dimension(cursor, 'DIM_SINH_VIEN', dims.get('DIM_SINH_VIEN', pd.DataFrame()),
                               ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        
        count = load_fact(cursor, dims.get('FACT', pd.DataFrame()))
        print(f"  ✅ FACT: {count:,} dòng")
        
        print(f"  ✅ Load: {time.time()-start:.2f}s")
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        conn.close()

def main():
    total_start = time.time()
    print("=" * 60)
    print("💾 SURVEY ETL - LOAD TO DATABASE")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)
    
    if not os.path.exists(OUTPUT_DIR):
        print(f"❌ Output directory không tồn tại: {OUTPUT_DIR}")
        sys.exit(1)
    
    print("\n📂 1. ĐỌC DỮ LIỆU TỪ PARQUET")
    start = time.time()
    dims = load_from_parquet(OUTPUT_DIR)
    print(f"  ✅ Đọc dữ liệu: {time.time()-start:.2f}s")
    
    print("\n💾 2. LOAD TO DATABASE")
    start = time.time()
    load_to_database(dims)
    print(f"  ✅ Load: {time.time()-start:.2f}s")
    
    total = time.time() - total_start
    print("\n" + "=" * 60)
    print(f"🎉 LOAD HOÀN THÀNH! Tổng thời gian: {total:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
