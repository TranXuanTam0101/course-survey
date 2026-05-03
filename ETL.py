
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

# ODBC Connection
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

# Batch size
BATCH_SIZE_FACT = 100000
BATCH_SIZE_DIM = 50000


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


# ================= LẤY THÔNG TIN CỘT TỪ DATABASE =================
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
    """Lấy thông tin indexes của bảng (chỉ non-clustered, không phải PK/UQ)"""
    cursor.execute(f"""
        SELECT 
            i.name as index_name,
            i.type_desc,
            STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) as columns
        FROM sys.indexes i
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('{table_name}')
        AND i.index_id > 1  -- Bỏ qua clustered index
        AND i.is_primary_key = 0
        AND i.is_unique_constraint = 0  -- Bỏ qua unique constraints
        AND i.name NOT LIKE 'UQ%'  -- Bỏ qua unique indexes
        GROUP BY i.name, i.type_desc
    """)
    return cursor.fetchall()


def drop_nonclustered_indexes(cursor, table_name):
    """Drop tất cả non-clustered indexes (trừ PK và UQ)"""
    indexes = get_indexes_info(cursor, table_name)
    dropped = []
    for idx in indexes:
        try:
            cursor.execute(f"DROP INDEX {idx[0]} ON {table_name}")
            dropped.append(idx[0])
            print(f"      Dropped index: {idx[0]}")
        except Exception as e:
            print(f"      ⚠️ Cannot drop {idx[0]}: {e}")
    return dropped


def recreate_indexes(cursor, table_name, indexes_info):
    """Tạo lại indexes sau khi insert"""
    for idx in indexes_info:
        try:
            cursor.execute(f"""
                CREATE {idx[1]} INDEX {idx[0]} ON {table_name} ({idx[2]})
            """)
            print(f"      Recreated index: {idx[0]}")
        except Exception as e:
            print(f"      ⚠️ Cannot create {idx[0]}: {e}")


# ================= INSERT DIMENSION TABLES =================
def fast_batch_insert(cursor, table, columns, data, batch_size):
    """Batch insert nhanh"""
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


def insert_dimension_tables(cursor, dims):
    """Insert DIMENSION tables"""
    print("\n  📥 Insert DIMENSION tables...")
    start_time = time.time()
    results = {}
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaKhoa'], r['TenKhoa']) for _, r in df.iterrows() if r['MaKhoa'] not in existing]
        if new_data:
            results['DIM_KHOA'] = fast_batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_KHOA']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaNganh'], r['TenNganh'], r['MaKhoa']) for _, r in df.iterrows() if r['MaNganh'] not in existing]
        if new_data:
            results['DIM_NGANH'] = fast_batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_NGANH']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaChuyenNganh'], r['TenChuyenNganh'], r['MaNganh']) for _, r in df.iterrows() if r['MaChuyenNganh'] not in existing]
        if new_data:
            results['DIM_CHUYEN_NGANH'] = fast_batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_CHUYEN_NGANH']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaHP'], r['TenHP'], r['MaKhoa']) for _, r in df.iterrows() if r['MaHP'] not in existing]
        if new_data:
            results['DIM_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_HOC_PHAN']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaGV'], r['HoDemGV'], r['TenGV']) for _, r in df.iterrows() if r['MaGV'] not in existing]
        if new_data:
            results['DIM_GIANG_VIEN'] = fast_batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_GIANG_VIEN']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaHocKy'], r['NamHoc'], r['HocKy']) for _, r in df.iterrows() if r['MaHocKy'] not in existing]
        if new_data:
            results['DIM_HOC_KY'] = fast_batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_HOC_KY']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaLop'], r['Lop'], r['MaChuyenNganh']) for _, r in df.iterrows() if r['MaLop'] not in existing]
        if new_data:
            results['DIM_LOP_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_LOP_SINH_VIEN']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaSV'], r['HoDem'], r['Ten'], r['NgaySinh'], r['MaLop']) for _, r in df.iterrows() if r['MaSV'] not in existing]
        if new_data:
            results['DIM_SINH_VIEN'] = fast_batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_SINH_VIEN']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(r['MaLopHP'], r['LopHP'], r['MaHP'], r['MaGV'], r['MaHocKy']) for _, r in df.iterrows() if r['MaLopHP'] not in existing]
        if new_data:
            results['DIM_LOP_HOC_PHAN'] = fast_batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data, BATCH_SIZE_DIM)
            print(f"      ✅ Insert: {results['DIM_LOP_HOC_PHAN']:,} dòng mới")
        else:
            print(f"      ⚪ No new rows")
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.2f}s")
    return results


# ================= INSERT FACT TABLES TỐI ƯU =================
def insert_fact_tables_ultra_fast(cursor, conn, fact_main, fact_ketqua):
    """Insert FACT tables với tốc độ cao"""
    print("\n  🚀 ULTRA FAST FACT INSERT")
    start_time = time.time()
    
    results = {}
    
    # Lấy danh sách cột thực tế từ database
    fact_main_columns = get_table_columns(cursor, 'FACT_GOP_Y_TU_LUAN')
    fact_ketqua_columns = get_table_columns(cursor, 'FACT_KET_QUA_DANH_GIA')
    
    print(f"\n  📋 FACT_GOP_Y_TU_LUAN columns: {fact_main_columns}")
    print(f"  📋 FACT_KET_QUA_DANH_GIA columns: {fact_ketqua_columns}")
    
    # === BƯỚC 1: LƯU THÔNG TIN INDEXES ===
    print("\n  📋 Saving index information...")
    main_indexes = get_indexes_info(cursor, 'FACT_GOP_Y_TU_LUAN')
    ketqua_indexes = get_indexes_info(cursor, 'FACT_KET_QUA_DANH_GIA')
    print(f"      Found {len(main_indexes)} indexes on FACT_GOP_Y_TU_LUAN")
    print(f"      Found {len(ketqua_indexes)} indexes on FACT_KET_QUA_DANH_GIA")
    
    # === BƯỚC 2: TẮT CONSTRAINTS ===
    print("\n  ⚡ Disabling constraints...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        conn.commit()
        print("  ✅ Constraints disabled")
    except Exception as e:
        print(f"  ⚠️ Could not disable constraints: {e}")
    
    # === BƯỚC 3: DROP NON-CLUSTERED INDEXES ===
    print("\n  🗑️ Dropping non-clustered indexes...")
    try:
        dropped_main = drop_nonclustered_indexes(cursor, 'FACT_GOP_Y_TU_LUAN')
        dropped_ketqua = drop_nonclustered_indexes(cursor, 'FACT_KET_QUA_DANH_GIA')
        conn.commit()
        print(f"  ✅ Dropped {len(dropped_main) + len(dropped_ketqua)} indexes")
    except Exception as e:
        print(f"  ⚠️ Could not drop indexes: {e}")
    
    # === BƯỚC 4: INSERT DỮ LIỆU ===
    try:
        # FACT_GOP_Y_TU_LUAN
        print("\n  📥 Inserting FACT_GOP_Y_TU_LUAN...")
        if fact_main is not None and not fact_main.empty:
            # Map cột từ DataFrame sang database columns
            column_mapping = {
                'SubmissionID': 'SubmissionID',
                'MaSV': 'MaSV',
                'LopHP': 'MaLopHP',  # Tên cột trong DB là MaLopHP
                'NoiDungGopY': 'NoiDungGopY',
                'Sentiment': 'Sentiment',
                'Is_Valid': 'Is_Valid',
                'Tag_HocPhan': 'Tag_HocPhan',
                'Tag_DayHoc': 'Tag_DayHoc',
                'Tag_KiemTra': 'Tag_KiemTra',
                'Tag_Khac': 'Tag_Khac'
            }
            
            # Chỉ lấy các cột có trong database
            db_columns = [col for col in fact_main_columns if col in column_mapping.values()]
            df_columns = [k for k, v in column_mapping.items() if v in db_columns]
            
            print(f"      Mapping columns: {dict(zip(df_columns, db_columns))}")
            
            # Chuẩn bị dữ liệu
            data = fact_main[df_columns].values.tolist()
            
            # Giới hạn độ dài nội dung
            for i, row in enumerate(data):
                if len(str(row[df_columns.index('NoiDungGopY')])) > 4000:
                    row[df_columns.index('NoiDungGopY')] = str(row[df_columns.index('NoiDungGopY')])[:4000]
            
            print(f"      Preparing {len(data):,} rows...")
            
            # Insert từng batch
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
            print(f"  ✅ Inserted {inserted:,} rows to FACT_GOP_Y_TU_LUAN")
        
        # FACT_KET_QUA_DANH_GIA
        print("\n  📥 Inserting FACT_KET_QUA_DANH_GIA...")
        if fact_ketqua is not None and not fact_ketqua.empty:
            # Map cột
            column_mapping_kq = {
                'SubmissionID': 'SubmissionID',
                'MaCauHoi': 'MaCauHoi',
                'Diem': 'Diem'
            }
            
            db_columns_kq = [col for col in fact_ketqua_columns if col in column_mapping_kq.values()]
            df_columns_kq = [k for k, v in column_mapping_kq.items() if v in db_columns_kq]
            
            data = fact_ketqua[df_columns_kq].values.tolist()
            
            print(f"      Preparing {len(data):,} rows...")
            
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
            print(f"  ✅ Inserted {inserted:,} rows to FACT_KET_QUA_DANH_GIA")
        
        conn.commit()
        
    except Exception as e:
        print(f"  ❌ Error during insert: {e}")
        conn.rollback()
        raise
    
    # === BƯỚC 5: RECREATE INDEXES ===
    print("\n  🔨 Recreating indexes...")
    try:
        if main_indexes:
            print("    Recreating indexes for FACT_GOP_Y_TU_LUAN...")
            recreate_indexes(cursor, 'FACT_GOP_Y_TU_LUAN', main_indexes)
        
        if ketqua_indexes:
            print("    Recreating indexes for FACT_KET_QUA_DANH_GIA...")
            recreate_indexes(cursor, 'FACT_KET_QUA_DANH_GIA', ketqua_indexes)
        
        conn.commit()
        print("  ✅ Indexes recreated")
    except Exception as e:
        print(f"  ⚠️ Could not recreate indexes: {e}")
    
    # === BƯỚC 6: BẬT LẠI CONSTRAINTS ===
    print("\n  🔓 Enabling constraints...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        conn.commit()
        print("  ✅ Constraints enabled")
    except Exception as e:
        print(f"  ⚠️ Could not enable constraints: {e}")
    
    # === BƯỚC 7: UPDATE STATISTICS ===
    print("\n  📊 Updating statistics...")
    try:
        cursor.execute("UPDATE STATISTICS FACT_GOP_Y_TU_LUAN")
        cursor.execute("UPDATE STATISTICS FACT_KET_QUA_DANH_GIA")
        conn.commit()
        print("  ✅ Statistics updated")
    except Exception as e:
        print(f"  ⚠️ Could not update statistics: {e}")
    
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
    print("🚀 JOB 2: CHÈN DỮ LIỆU (TỐC ĐỘ CAO - ĐÃ SỬA LỖI)")
    print("=" * 80)
    print(f"📂 Survey: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
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
    print(f"     - FACT_GOP_Y_TU_LUAN: {len(fact_main):,} rows")
    print(f"     - FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} rows")
    
    # 3. Kết nối Database
    print("\n💾 3. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Connected")
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
        dim_results = insert_dimension_tables(cursor, dims)
        
        # Insert FACT tables
        fact_results = insert_fact_tables_ultra_fast(cursor, conn, fact_main, fact_ketqua)
        
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
    
    total_inserted = sum(fact_results.values()) if fact_results else 0
    
    print(f"  ✅ TOTAL inserted: {total_inserted:,} rows")
    print(f"  ⏱️ Insert time: {insert_time:.2f}s")
    
    if insert_time > 0 and total_inserted > 0:
        speed = total_inserted / insert_time
        print(f"  🚀 Speed: {speed:,.0f} rows/second")
    
    print(f"\n✅ HOÀN THÀNH! Total time: {total_time:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
