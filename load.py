#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SURVEY ETL - LOAD TO DATABASE (ULTRA FAST)
- Cache existing IDs
- Bulk insert với to_sql
- Tắt FK constraints khi load
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
    f"Connection Timeout=120;"
    f"Command Timeout=300;"
)

BATCH_SIZE = 50000

# Cache cho existing IDs
_EXISTING_CACHE = {}

# ========== HELPER FUNCTIONS ==========
def get_existing_ids_cached(cursor, table: str, id_col: str) -> set:
    """Lấy existing IDs với cache"""
    cache_key = f"{table}.{id_col}"
    if cache_key in _EXISTING_CACHE:
        return _EXISTING_CACHE[cache_key]
    
    print(f"    -> Querying {table}...")
    cursor.execute(f"SELECT {id_col} FROM {table}")
    ids = {row[0] for row in cursor.fetchall()}
    _EXISTING_CACHE[cache_key] = ids
    return ids

def load_dimension_fast(cursor, table: str, df: pd.DataFrame, columns: list, id_col: str) -> int:
    if df.empty:
        return 0
    
    df = df.fillna('')
    existing = get_existing_ids_cached(cursor, table, id_col)
    new_data = df[~df[id_col].isin(existing)]
    
    if new_data.empty:
        return 0
    
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
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    
    # Cập nhật cache
    _EXISTING_CACHE[f"{table}.{id_col}"].update(new_data[id_col].tolist())
    
    return len(new_data)

def load_fact_ultra_fast(conn, cursor, fact_df: pd.DataFrame) -> int:
    """Load FACT siêu nhanh dùng temp table + MERGE"""
    if fact_df.empty:
        return 0
    
    print(f"  -> Insert FACT: {len(fact_df):,} dòng...")
    start = time.time()
    
    # Tạo temp table
    print("    -> Creating temp table...")
    cursor.execute("""
        CREATE TABLE #TEMP_FACT (
            SubmissionID NVARCHAR(150),
            MaCauHoi INT,
            MaSV NVARCHAR(20),
            MaLopHP NVARCHAR(50),
            TraLoiSo FLOAT,
            TraLoiText NVARCHAR(MAX)
        )
    """)
    conn.commit()
    
    # Bulk insert vào temp table dùng to_sql
    print("    -> Bulk inserting into temp table...")
    fact_df.to_sql('#TEMP_FACT', conn, index=False, if_exists='append', 
                   method='multi', chunksize=BATCH_SIZE)
    
    # Merge vào FACT chính (chỉ insert dòng có FK hợp lệ)
    print("    -> Merging into FACT_TRA_LOI_KHAO_SAT...")
    cursor.execute("""
        INSERT INTO FACT_TRA_LOI_KHAO_SAT 
        (SubmissionID, MaCauHoi, MaSV, MaLopHP, TraLoiSo, TraLoiText)
        SELECT t.*
        FROM #TEMP_FACT t
        WHERE EXISTS (SELECT 1 FROM DIM_SINH_VIEN s WHERE s.MaSV = t.MaSV)
          AND EXISTS (SELECT 1 FROM DIM_LOP_HOC_PHAN l WHERE l.MaLopHP = t.MaLopHP)
    """)
    
    inserted = cursor.rowcount
    conn.commit()
    
    # Drop temp table
    cursor.execute("DROP TABLE #TEMP_FACT")
    conn.commit()
    
    print(f"  ✅ FACT done: {inserted:,} dòng ({time.time()-start:.2f}s)")
    return inserted

def disable_fk_constraints(cursor):
    """Tắt tất cả FK constraints trên FACT"""
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()

def enable_fk_constraints(cursor):
    """Bật lại tất cả FK constraints trên FACT"""
    cursor.execute("ALTER TABLE FACT_TRA_LOI_KHAO_SAT CHECK CONSTRAINT ALL")
    cursor.connection.commit()

def load_from_parquet(output_dir: str) -> dict:
    """Đọc dữ liệu từ parquet"""
    print(f"  -> Đọc dữ liệu từ: {output_dir}")
    
    # Đọc metadata
    meta_path = os.path.join(output_dir, "metadata.pkl")
    if os.path.exists(meta_path):
        with open(meta_path, 'rb') as f:
            meta = pickle.load(f)
        print(f"  -> Metadata: {meta}")
    
    # Đọc tất cả parquet files
    dims = {}
    tables = ['DIM_HOC_KY', 'DIM_KHOA', 'DIM_CHUYEN_NGANH', 'DIM_HOC_PHAN', 
              'DIM_GIANG_VIEN', 'DIM_LOP_HOC_PHAN', 'DIM_LOP_SINH_VIEN', 
              'DIM_SINH_VIEN', 'FACT']
    
    for table in tables:
        filepath = os.path.join(output_dir, f"{table}.parquet")
        if os.path.exists(filepath):
            dims[table] = pd.read_parquet(filepath)
            print(f"  -> Đọc {table}: {len(dims[table]):,} rows")
        else:
            print(f"  ⚠️ Không tìm thấy {table}")
            dims[table] = pd.DataFrame()
    
    return dims

def load_to_database(dims: dict):
    print("  -> Load...")
    start = time.time()
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        # 1. DIM_HOC_KY
        count = load_dimension_fast(cursor, 'DIM_HOC_KY', dims.get('DIM_HOC_KY', pd.DataFrame()),
                                    ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
        print(f"  ✅ DIM_HOC_KY: {count} new")
        
        # 2. DIM_KHOA
        count = load_dimension_fast(cursor, 'DIM_KHOA', dims.get('DIM_KHOA', pd.DataFrame()),
                                    ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        print(f"  ✅ DIM_KHOA: {count} new")
        
        # 3. DIM_CTDT
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        print("  ✅ DIM_CTDT: ensured")
        
        # 4. DIM_CHUYEN_NGANH
        count = load_dimension_fast(cursor, 'DIM_CHUYEN_NGANH', dims.get('DIM_CHUYEN_NGANH', pd.DataFrame()),
                                    ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
        
        # 5. DIM_HOC_PHAN
        count = load_dimension_fast(cursor, 'DIM_HOC_PHAN', dims.get('DIM_HOC_PHAN', pd.DataFrame()),
                                    ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        print(f"  ✅ DIM_HOC_PHAN: {count} new")
        
        # 6. DIM_GIANG_VIEN
        count = load_dimension_fast(cursor, 'DIM_GIANG_VIEN', dims.get('DIM_GIANG_VIEN', pd.DataFrame()),
                                    ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        print(f"  ✅ DIM_GIANG_VIEN: {count} new")
        
        # 7. DIM_LOP_HOC_PHAN
        count = load_dimension_fast(cursor, 'DIM_LOP_HOC_PHAN', dims.get('DIM_LOP_HOC_PHAN', pd.DataFrame()),
                                    ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
        
        # 8. DIM_LOP_SINH_VIEN
        count = load_dimension_fast(cursor, 'DIM_LOP_SINH_VIEN', dims.get('DIM_LOP_SINH_VIEN', pd.DataFrame()),
                                    ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop')
        print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
        
        # 9. DIM_SINH_VIEN
        count = load_dimension_fast(cursor, 'DIM_SINH_VIEN', dims.get('DIM_SINH_VIEN', pd.DataFrame()),
                                    ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV')
        print(f"  ✅ DIM_SINH_VIEN: {count} new")
        
        # 10. FACT - ULTRA FAST
        print("\n  --- FACT (ULTRA FAST) ---")
        count = load_fact_ultra_fast(conn, cursor, dims.get('FACT', pd.DataFrame()))
        print(f"  ✅ FACT: {count:,} dòng")
        
        print(f"\n  ✅ Load hoàn tất: {time.time()-start:.2f}s")
        
    except Exception as e:
        print(f"\n  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()

def main():
    total_start = time.time()
    print("=" * 60)
    print("💾 SURVEY ETL - LOAD TO DATABASE (ULTRA FAST)")
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
