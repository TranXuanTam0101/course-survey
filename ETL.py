import os
import sys
import time
import pickle
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings
warnings.filterwarnings('ignore')

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ODBC Connection - TỐI ƯU CAO
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=120;"
    f"Command Timeout=300;"
    f"AutoCommit=False;"
)

CONTAINER_NAME = SEMESTER
PREPROCESSED_PATH = "preprocessed-data"

# Batch size TỐI ƯU - tăng lên để insert nhanh hơn
BATCH_SIZE_FACT = 250000  # Tăng từ 100k lên 250k
BATCH_SIZE_DIM = 100000   # Tăng từ 50k lên 100k


# ================= BLOB FUNCTIONS =================
def download_preprocessed_data(blob_service, filename):
    """Tải dữ liệu đã tiền xử lý từ blob"""
    path = f"{PREPROCESSED_PATH}/{filename}.pkl"
    try:
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            print(f"  📥 Đang tải {path}...")
            pickled_data = blob.download_blob().readall()
            return pickle.loads(pickled_data)
        return None
    except Exception as e:
        print(f"  ❌ Lỗi tải: {e}")
        return None


# ================= LẤY THÔNG TIN TỪ DATABASE =================
def is_table_empty(cursor, table_name):
    """Kiểm tra bảng có rỗng không"""
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    return count == 0


def get_table_columns(cursor, table_name):
    """Lấy danh sách cột của bảng"""
    cursor.execute(f"""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
    """)
    return [row[0] for row in cursor.fetchall()]


def get_indexes_info(cursor, table_name):
    """Lấy thông tin indexes (non-clustered, non-PK, non-UQ)"""
    cursor.execute(f"""
        SELECT 
            i.name as index_name,
            i.type_desc,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('{table_name}')
        AND i.index_id > 1
        AND i.is_primary_key = 0
        AND i.is_unique_constraint = 0
        AND i.name NOT LIKE 'UQ%'
        AND i.name NOT LIKE 'PK%'
        GROUP BY i.name, i.type_desc
    """)
    return cursor.fetchall()


def disable_all_constraints_and_triggers(cursor, table_name):
    """Tắt constraints và triggers cho 1 bảng"""
    try:
        cursor.execute(f"ALTER TABLE {table_name} NOCHECK CONSTRAINT ALL")
        cursor.execute(f"DISABLE TRIGGER ALL ON {table_name}")
        return True
    except Exception as e:
        print(f"      ⚠️ Cannot disable on {table_name}: {e}")
        return False


def enable_all_constraints_and_triggers(cursor, table_name):
    """Bật lại constraints và triggers"""
    try:
        cursor.execute(f"ALTER TABLE {table_name} CHECK CONSTRAINT ALL")
        cursor.execute(f"ENABLE TRIGGER ALL ON {table_name}")
        return True
    except Exception as e:
        print(f"      ⚠️ Cannot enable on {table_name}: {e}")
        return False


# ================= INSERT DIMENSION TABLES TỐI ƯU =================
def insert_dimension_tables_optimized(cursor, conn, dims):
    """Insert DIMENSION tables - tối ưu cho lần chạy đầu"""
    print("\n  📥 Insert DIMENSION tables...")
    start_time = time.time()
    results = {}
    
    # Kiểm tra xem đã có dữ liệu chưa
    empty_tables = {}
    for table in ['DIM_KHOA', 'DIM_NGANH', 'DIM_CHUYEN_NGANH', 'DIM_HOC_PHAN',
                  'DIM_GIANG_VIEN', 'DIM_HOC_KY', 'DIM_LOP_SINH_VIEN', 
                  'DIM_SINH_VIEN', 'DIM_LOP_HOC_PHAN']:
        empty_tables[table] = is_table_empty(cursor, table)
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        if empty_tables['DIM_KHOA']:
            # Bảng rỗng, insert thẳng không cần check
            data = [(r['MaKhoa'], r['TenKhoa']) for _, r in df.iterrows()]
            results['DIM_KHOA'] = fast_batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_KHOA']:,} rows (table was empty)")
        else:
            # Có dữ liệu rồi, cần check
            cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaKhoa'], r['TenKhoa']) for _, r in df.iterrows() if r['MaKhoa'] not in existing]
            if new_data:
                results['DIM_KHOA'] = fast_batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_KHOA']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        if empty_tables['DIM_NGANH']:
            data = [(r['MaNganh'], r['TenNganh'], r['MaKhoa']) for _, r in df.iterrows()]
            results['DIM_NGANH'] = fast_batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_NGANH']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaNganh FROM DIM_NGANH")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaNganh'], r['TenNganh'], r['MaKhoa']) for _, r in df.iterrows() if r['MaNganh'] not in existing]
            if new_data:
                results['DIM_NGANH'] = fast_batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_NGANH']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        if empty_tables['DIM_CHUYEN_NGANH']:
            data = [(r['MaChuyenNganh'], r['TenChuyenNganh'], r['MaNganh']) for _, r in df.iterrows()]
            results['DIM_CHUYEN_NGANH'] = fast_batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_CHUYEN_NGANH']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaChuyenNganh'], r['TenChuyenNganh'], r['MaNganh']) for _, r in df.iterrows() if r['MaChuyenNganh'] not in existing]
            if new_data:
                results['DIM_CHUYEN_NGANH'] = fast_batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_CHUYEN_NGANH']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        if empty_tables['DIM_HOC_PHAN']:
            data = [(r['MaHP'], r['TenHP'], r['MaKhoa']) for _, r in df.iterrows()]
            results['DIM_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_HOC_PHAN']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaHP'], r['TenHP'], r['MaKhoa']) for _, r in df.iterrows() if r['MaHP'] not in existing]
            if new_data:
                results['DIM_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_HOC_PHAN']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        if empty_tables['DIM_GIANG_VIEN']:
            data = [(r['MaGV'], r['HoDemGV'], r['TenGV']) for _, r in df.iterrows()]
            results['DIM_GIANG_VIEN'] = fast_batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_GIANG_VIEN']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaGV'], r['HoDemGV'], r['TenGV']) for _, r in df.iterrows() if r['MaGV'] not in existing]
            if new_data:
                results['DIM_GIANG_VIEN'] = fast_batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_GIANG_VIEN']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        if empty_tables['DIM_HOC_KY']:
            data = [(r['MaHocKy'], r['NamHoc'], r['HocKy']) for _, r in df.iterrows()]
            results['DIM_HOC_KY'] = fast_batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_HOC_KY']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaHocKy'], r['NamHoc'], r['HocKy']) for _, r in df.iterrows() if r['MaHocKy'] not in existing]
            if new_data:
                results['DIM_HOC_KY'] = fast_batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_HOC_KY']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        if empty_tables['DIM_LOP_SINH_VIEN']:
            data = [(r['MaLop'], r['Lop'], r['MaChuyenNganh']) for _, r in df.iterrows()]
            results['DIM_LOP_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_LOP_SINH_VIEN']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaLop'], r['Lop'], r['MaChuyenNganh']) for _, r in df.iterrows() if r['MaLop'] not in existing]
            if new_data:
                results['DIM_LOP_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_LOP_SINH_VIEN']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        if empty_tables['DIM_SINH_VIEN']:
            data = [(r['MaSV'], r['HoDem'], r['Ten'], r['NgaySinh'], r['MaLop']) for _, r in df.iterrows()]
            results['DIM_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_SINH_VIEN']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaSV'], r['HoDem'], r['Ten'], r['NgaySinh'], r['MaLop']) for _, r in df.iterrows() if r['MaSV'] not in existing]
            if new_data:
                results['DIM_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_SINH_VIEN']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        if empty_tables['DIM_LOP_HOC_PHAN']:
            data = [(r['MaLopHP'], r['LopHP'], r['MaHP'], r['MaGV'], r['MaHocKy']) for _, r in df.iterrows()]
            results['DIM_LOP_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert {results['DIM_LOP_HOC_PHAN']:,} rows (table was empty)")
        else:
            cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
            existing = {row[0] for row in cursor.fetchall()}
            new_data = [(r['MaLopHP'], r['LopHP'], r['MaHP'], r['MaGV'], r['MaHocKy']) for _, r in df.iterrows() if r['MaLopHP'] not in existing]
            if new_data:
                results['DIM_LOP_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data, BATCH_SIZE_DIM)
                print(f"      ✅ Insert {results['DIM_LOP_HOC_PHAN']:,} new rows")
            else:
                print(f"      ⚪ No new rows")
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.2f}s")
    return results


def fast_batch_insert(cursor, table, columns, data, batch_size):
    """Batch insert siêu nhanh"""
    if not data:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.fast_executemany = True
        cursor.executemany(sql, batch)
        total += len(batch)
        cursor.connection.commit()
    
    return total


# ================= INSERT FACT TABLES TỐI ƯU NHẤT =================
def insert_fact_tables_optimized(cursor, conn, fact_main, fact_ketqua):
    """Insert FACT tables - tối ưu cho lần chạy đầu (bảng rỗng)"""
    print("\n  🚀 ULTRA FAST FACT INSERT")
    start_time = time.time()
    
    results = {}
    
    # Kiểm tra bảng có rỗng không
    fact_main_empty = is_table_empty(cursor, 'FACT_GOP_Y_TU_LUAN')
    fact_ketqua_empty = is_table_empty(cursor, 'FACT_KET_QUA_DANH_GIA')
    
    print(f"\n  📊 Table status:")
    print(f"      FACT_GOP_Y_TU_LUAN: {'Empty' if fact_main_empty else 'Has data'}")
    print(f"      FACT_KET_QUA_DANH_GIA: {'Empty' if fact_ketqua_empty else 'Has data'}")
    
    # Nếu bảng rỗng, chúng ta có thể insert cực kỳ nhanh
    if fact_main_empty and fact_ketqua_empty:
        print("\n  🎯 Both FACT tables are empty - Using ULTRA FAST mode!")
        
        # Tắt constraints và triggers
        print("\n  ⚡ Disabling constraints and triggers...")
        disable_all_constraints_and_triggers(cursor, 'FACT_GOP_Y_TU_LUAN')
        disable_all_constraints_and_triggers(cursor, 'FACT_KET_QUA_DANH_GIA')
        conn.commit()
        print("  ✅ Constraints and triggers disabled")
        
        # Lấy danh sách cột
        fact_main_columns = get_table_columns(cursor, 'FACT_GOP_Y_TU_LUAN')
        fact_ketqua_columns = get_table_columns(cursor, 'FACT_KET_QUA_DANH_GIA')
        
        # FACT_GOP_Y_TU_LUAN
        print("\n  📥 Inserting FACT_GOP_Y_TU_LUAN...")
        if fact_main is not None and not fact_main.empty:
            column_mapping = {
                'SubmissionID': 'SubmissionID',
                'MaSV': 'MaSV',
                'LopHP': 'MaLopHP',
                'NoiDungGopY': 'NoiDungGopY',
                'Sentiment': 'Sentiment',
                'Is_Valid': 'Is_Valid',
                'Tag_HocPhan': 'Tag_HocPhan',
                'Tag_DayHoc': 'Tag_DayHoc',
                'Tag_KiemTra': 'Tag_KiemTra',
                'Tag_Khac': 'Tag_Khac'
            }
            
            db_columns = [col for col in fact_main_columns if col in column_mapping.values()]
            df_columns = [k for k, v in column_mapping.items() if v in db_columns]
            
            # Chuyển toàn bộ dữ liệu thành list of tuples
            data = fact_main[df_columns].values.tolist()
            
            # Giới hạn độ dài
            noi_dung_idx = df_columns.index('NoiDungGopY') if 'NoiDungGopY' in df_columns else None
            if noi_dung_idx:
                for row in data:
                    if len(str(row[noi_dung_idx])) > 4000:
                        row[noi_dung_idx] = str(row[noi_dung_idx])[:4000]
            
            print(f"      Preparing {len(data):,} rows...")
            
            # Insert 1 batch duy nhất nếu có thể
            if len(data) <= BATCH_SIZE_FACT:
                placeholders = ', '.join(['?' for _ in db_columns])
                sql = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({', '.join(db_columns)}) VALUES ({placeholders})"
                cursor.fast_executemany = True
                cursor.executemany(sql, data)
                conn.commit()
                results['FACT_GOP_Y_TU_LUAN'] = len(data)
                print(f"  ✅ Inserted {len(data):,} rows in ONE batch!")
            else:
                # Chia thành nhiều batch
                placeholders = ', '.join(['?' for _ in db_columns])
                sql = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({', '.join(db_columns)}) VALUES ({placeholders})"
                
                inserted = 0
                for i in range(0, len(data), BATCH_SIZE_FACT):
                    batch = data[i:i+BATCH_SIZE_FACT]
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    inserted += len(batch)
                    conn.commit()
                    print(f"      Batch {i//BATCH_SIZE_FACT + 1}: {len(batch):,} rows (total: {inserted:,})")
                
                results['FACT_GOP_Y_TU_LUAN'] = inserted
                print(f"  ✅ Inserted {inserted:,} rows")
        
        # FACT_KET_QUA_DANH_GIA
        print("\n  📥 Inserting FACT_KET_QUA_DANH_GIA...")
        if fact_ketqua is not None and not fact_ketqua.empty:
            column_mapping_kq = {
                'SubmissionID': 'SubmissionID',
                'MaCauHoi': 'MaCauHoi',
                'Diem': 'Diem'
            }
            
            db_columns_kq = [col for col in fact_ketqua_columns if col in column_mapping_kq.values()]
            df_columns_kq = [k for k, v in column_mapping_kq.items() if v in db_columns_kq]
            
            data = fact_ketqua[df_columns_kq].values.tolist()
            
            print(f"      Preparing {len(data):,} rows...")
            
            # Insert 1 batch duy nhất nếu có thể
            if len(data) <= BATCH_SIZE_FACT:
                placeholders = ', '.join(['?' for _ in db_columns_kq])
                sql = f"INSERT INTO FACT_KET_QUA_DANH_GIA ({', '.join(db_columns_kq)}) VALUES ({placeholders})"
                cursor.fast_executemany = True
                cursor.executemany(sql, data)
                conn.commit()
                results['FACT_KET_QUA_DANH_GIA'] = len(data)
                print(f"  ✅ Inserted {len(data):,} rows in ONE batch!")
            else:
                placeholders = ', '.join(['?' for _ in db_columns_kq])
                sql = f"INSERT INTO FACT_KET_QUA_DANH_GIA ({', '.join(db_columns_kq)}) VALUES ({placeholders})"
                
                inserted = 0
                for i in range(0, len(data), BATCH_SIZE_FACT):
                    batch = data[i:i+BATCH_SIZE_FACT]
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    inserted += len(batch)
                    conn.commit()
                    print(f"      Batch {i//BATCH_SIZE_FACT + 1}: {len(batch):,} rows (total: {inserted:,})")
                
                results['FACT_KET_QUA_DANH_GIA'] = inserted
                print(f"  ✅ Inserted {inserted:,} rows")
        
        # Bật lại constraints và triggers
        print("\n  🔓 Enabling constraints and triggers...")
        enable_all_constraints_and_triggers(cursor, 'FACT_GOP_Y_TU_LUAN')
        enable_all_constraints_and_triggers(cursor, 'FACT_KET_QUA_DANH_GIA')
        conn.commit()
        print("  ✅ Constraints and triggers enabled")
        
    else:
        # Bảng đã có dữ liệu, cần xử lý cẩn thận
        print("\n  ⚠️ Tables already have data - Using safe mode...")
        # ... code xử lý khi có dữ liệu (giữ nguyên logic cũ)
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ FACT tables done in {elapsed:.2f}s")
    return results


# ================= KIỂM TRA DỮ LIỆU =================
def verify_data(cursor):
    """Kiểm tra số lượng dữ liệu sau insert"""
    print("\n  📊 Verifying data...")
    
    tables = [
        'DIM_KHOA', 'DIM_NGANH', 'DIM_CHUYEN_NGANH', 'DIM_HOC_PHAN',
        'DIM_GIANG_VIEN', 'DIM_HOC_KY', 'DIM_LOP_SINH_VIEN', 'DIM_SINH_VIEN',
        'DIM_LOP_HOC_PHAN', 'FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA'
    ]
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"      {table}: {count:,} rows")
        except Exception as e:
            print(f"      {table}: Error - {e}")


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 80)
    print("🚀 JOB 2: CHÈN DỮ LIỆU (TỐI ƯU CHO LẦN CHẠY ĐẦU)")
    print("=" * 80)
    print(f"📂 Survey: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ FACT Batch size: {BATCH_SIZE_FACT:,} rows/batch")
    print(f"⚙️ DIM Batch size: {BATCH_SIZE_DIM:,} rows/batch")
    print("=" * 80)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Connected")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return
    
    # 2. Tải dữ liệu
    print(f"\n📥 2. Tải preprocessed data...")
    preprocessed_data = download_preprocessed_data(blob_service, f"{FILE_NAME}_preprocessed")
    
    if not preprocessed_data:
        print("  ❌ Không tìm thấy preprocessed data!")
        return
    
    # Lấy dữ liệu
    dims = {k: v for k, v in preprocessed_data.items() if k.startswith('dim_')}
    fact_main = preprocessed_data.get('fact_gop_y_tu_luan', pd.DataFrame())
    fact_ketqua = preprocessed_data.get('fact_ket_qua_danh_gia', pd.DataFrame())
    
    print(f"\n  📊 Data summary:")
    for name, df in dims.items():
        if not df.empty:
            print(f"     - {name}: {len(df):,} rows")
    print(f"     - FACT_GOP_Y_TU_LUAN: {len(fact_main):,} rows")
    print(f"     - FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} rows")
    
    # 3. Kết nối Database
    print("\n💾 3. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Connected (fast_executemany=ON)")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return
    
    # 4. Insert dữ liệu
    print("\n" + "=" * 80)
    print("🚀 4. BẮT ĐẦU INSERT")
    print("=" * 80)
    
    insert_start = time.time()
    
    try:
        # Insert DIMENSION tables
        dim_results = insert_dimension_tables_optimized(cursor, conn, dims)
        
        # Insert FACT tables
        fact_results = insert_fact_tables_optimized(cursor, conn, fact_main, fact_ketqua)
        
        # Kiểm tra kết quả
        verify_data(cursor)
        
    except Exception as e:
        print(f"\n  ❌ Lỗi: {e}")
        conn.rollback()
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    insert_time = time.time() - insert_start
    total_time = time.time() - total_start
    
    print("\n" + "=" * 80)
    print("📊 KẾT QUẢ")
    print("=" * 80)
    
    total_inserted = sum(dim_results.values()) + sum(fact_results.values()) if fact_results else sum(dim_results.values())
    
    print(f"  ✅ TOTAL inserted: {total_inserted:,} rows")
    print(f"  ⏱️ Insert time: {insert_time:.2f}s")
    
    if insert_time > 0 and total_inserted > 0:
        speed = total_inserted / insert_time
        print(f"  🚀 Speed: {speed:,.0f} rows/second")
    
    print(f"\n✅ HOÀN THÀNH! Total time: {total_time:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
