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

# ODBC Connection - TỐI ƯU KẾT NỐI
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

# Batch size tối ưu
BATCH_SIZE = 100000  # 100k dòng/batch


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


# ================= DATABASE FUNCTIONS TỐI ƯU =================
def fast_batch_insert(cursor, table, columns, data, batch_size=BATCH_SIZE):
    """Batch insert siêu nhanh với fast_executemany"""
    if not data:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    num_batches = (len(data) + batch_size - 1) // batch_size
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        batch_num = i // batch_size + 1
        
        try:
            cursor.fast_executemany = True
            cursor.executemany(sql, batch)
            total += len(batch)
            cursor.connection.commit()
            
            if batch_num % 10 == 0 or batch_num == num_batches:
                print(f"      ✅ Batch {batch_num}/{num_batches}: {len(batch):,} dòng (total: {total:,})")
        except Exception as e:
            print(f"      ❌ Lỗi batch {batch_num}: {e}")
            cursor.connection.rollback()
            continue
    
    return total


def insert_dimension_table(cursor, df, table_name, key_column, columns, data_converter):
    """Insert dimension table generic"""
    if df is None or df.empty:
        return 0
    
    # Lấy existing keys
    cursor.execute(f"SELECT {key_column} FROM {table_name}")
    existing = {row[0] for row in cursor.fetchall()}
    
    # Lọc dữ liệu mới
    new_data = []
    for _, row in df.iterrows():
        if row[key_column] not in existing:
            new_data.append(data_converter(row))
            existing.add(row[key_column])
    
    if new_data:
        return fast_batch_insert(cursor, table_name, columns, new_data)
    return 0


# ================= INSERT DIMENSION TABLES =================
def insert_all_dimensions(cursor, dims):
    """Insert tất cả dimension tables"""
    print("\n  📥 Insert DIMENSION tables...")
    start_time = time.time()
    
    results = {}
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        results['DIM_KHOA'] = insert_dimension_table(
            cursor, df, 'DIM_KHOA', 'MaKhoa',
            ['MaKhoa', 'TenKhoa'],
            lambda r: (r['MaKhoa'], r['TenKhoa'])
        )
        print(f"      ✅ Insert: {results['DIM_KHOA']:,} dòng mới")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        results['DIM_NGANH'] = insert_dimension_table(
            cursor, df, 'DIM_NGANH', 'MaNganh',
            ['MaNganh', 'TenNganh', 'MaKhoa'],
            lambda r: (r['MaNganh'], r['TenNganh'], r['MaKhoa'])
        )
        print(f"      ✅ Insert: {results['DIM_NGANH']:,} dòng mới")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        results['DIM_CHUYEN_NGANH'] = insert_dimension_table(
            cursor, df, 'DIM_CHUYEN_NGANH', 'MaChuyenNganh',
            ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'],
            lambda r: (r['MaChuyenNganh'], r['TenChuyenNganh'], r['MaNganh'])
        )
        print(f"      ✅ Insert: {results['DIM_CHUYEN_NGANH']:,} dòng mới")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        results['DIM_HOC_PHAN'] = insert_dimension_table(
            cursor, df, 'DIM_HOC_PHAN', 'MaHP',
            ['MaHP', 'TenHP', 'MaKhoa'],
            lambda r: (r['MaHP'], r['TenHP'], r['MaKhoa'])
        )
        print(f"      ✅ Insert: {results['DIM_HOC_PHAN']:,} dòng mới")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        results['DIM_GIANG_VIEN'] = insert_dimension_table(
            cursor, df, 'DIM_GIANG_VIEN', 'MaGV',
            ['MaGV', 'HoDemGV', 'TenGV'],
            lambda r: (r['MaGV'], r['HoDemGV'], r['TenGV'])
        )
        print(f"      ✅ Insert: {results['DIM_GIANG_VIEN']:,} dòng mới")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        results['DIM_HOC_KY'] = insert_dimension_table(
            cursor, df, 'DIM_HOC_KY', 'MaHocKy',
            ['MaHocKy', 'NamHoc', 'HocKy'],
            lambda r: (r['MaHocKy'], r['NamHoc'], r['HocKy'])
        )
        print(f"      ✅ Insert: {results['DIM_HOC_KY']:,} dòng mới")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        results['DIM_LOP_SINH_VIEN'] = insert_dimension_table(
            cursor, df, 'DIM_LOP_SINH_VIEN', 'MaLop',
            ['MaLop', 'Lop', 'MaChuyenNganh'],
            lambda r: (r['MaLop'], r['Lop'], r['MaChuyenNganh'])
        )
        print(f"      ✅ Insert: {results['DIM_LOP_SINH_VIEN']:,} dòng mới")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        results['DIM_SINH_VIEN'] = insert_dimension_table(
            cursor, df, 'DIM_SINH_VIEN', 'MaSV',
            ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'],
            lambda r: (r['MaSV'], r['HoDem'], r['Ten'], r['NgaySinh'], r['MaLop'])
        )
        print(f"      ✅ Insert: {results['DIM_SINH_VIEN']:,} dòng mới")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        results['DIM_LOP_HOC_PHAN'] = insert_dimension_table(
            cursor, df, 'DIM_LOP_HOC_PHAN', 'MaLopHP',
            ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'],
            lambda r: (r['MaLopHP'], r['LopHP'], r['MaHP'], r['MaGV'], r['MaHocKy'])
        )
        print(f"      ✅ Insert: {results['DIM_LOP_HOC_PHAN']:,} dòng mới")
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.1f}s")
    return results


# ================= INSERT FACT TABLES =================
def insert_fact_tables(cursor, fact_main, fact_ketqua):
    """Insert fact tables - CHÈN THÊM MỚI, KHÔNG CHECK"""
    print("\n  📥 Insert FACT tables...")
    start_time = time.time()
    
    results = {}
    
    # TẮT CONSTRAINTS để tăng tốc
    try:
        print("  ⚡ Disabling constraints...")
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        cursor.connection.commit()
        print("  ✅ Constraints disabled")
    except Exception as e:
        print(f"  ⚠️ Cannot disable constraints: {e}")
    
    try:
        # 1. FACT_GOP_Y_TU_LUAN
        print("\n    📌 FACT_GOP_Y_TU_LUAN")
        if fact_main is not None and not fact_main.empty:
            # Chuẩn bị dữ liệu
            data = fact_main[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                             'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                             'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].values.tolist()
            
            print(f"      -> Chuẩn bị {len(data):,} dòng để insert")
            
            inserted = fast_batch_insert(cursor, 'FACT_GOP_Y_TU_LUAN',
                                        ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY',
                                         'Sentiment', 'Is_Valid', 'Tag_HocPhan',
                                         'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'],
                                        data)
            results['FACT_GOP_Y_TU_LUAN'] = inserted
            print(f"      ✅ Inserted {inserted:,} rows")
        else:
            print(f"      ⚠️ No data")
        
        # 2. FACT_KET_QUA_DANH_GIA
        print("\n    📌 FACT_KET_QUA_DANH_GIA")
        if fact_ketqua is not None and not fact_ketqua.empty:
            # Chuẩn bị dữ liệu
            data = fact_ketqua[['SubmissionID', 'MaCauHoi', 'Diem']].values.tolist()
            
            print(f"      -> Chuẩn bị {len(data):,} dòng để insert")
            
            inserted = fast_batch_insert(cursor, 'FACT_KET_QUA_DANH_GIA',
                                        ['SubmissionID', 'MaCauHoi', 'Diem'],
                                        data)
            results['FACT_KET_QUA_DANH_GIA'] = inserted
            print(f"      ✅ Inserted {inserted:,} rows")
        else:
            print(f"      ⚠️ No data")
        
        cursor.connection.commit()
        
    except Exception as e:
        print(f"  ❌ Error: {e}")
        cursor.connection.rollback()
        raise
    finally:
        # BẬT LẠI CONSTRAINTS
        try:
            print("\n  ⚡ Enabling constraints...")
            cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
            cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
            cursor.connection.commit()
            print("  ✅ Constraints enabled")
        except Exception as e:
            print(f"  ⚠️ Cannot enable constraints: {e}")
    
    elapsed = time.time() - start_time
    print(f"\n  ✅ FACT tables done in {elapsed:.1f}s")
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
    
    results = {}
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            results[table] = count
            print(f"      {table}: {count:,} rows")
        except Exception as e:
            print(f"      {table}: Error - {e}")
    
    return results


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 70)
    print("🚀 JOB 2: CHÈN DỮ LIỆU (SIÊU TỐC)")
    print("=" * 70)
    print(f"📂 Survey: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ Batch size: {BATCH_SIZE:,} rows/batch")
    print("=" * 70)
    print("\n📌 INSERT STRATEGY:")
    print("   ✅ DIMENSION: Only insert new records (check exists)")
    print("   ✅ FACT: Insert all records (no check, faster)")
    print("   ✅ Fast_executemany: ON")
    print("   ✅ Constraints: Disabled during insert")
    print("=" * 70)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Connected")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return
    
    # 2. Tải dữ liệu đã tiền xử lý
    print(f"\n📥 2. Tải preprocessed data...")
    preprocessed_data = download_preprocessed_data(blob_service, f"{FILE_NAME}_preprocessed")
    
    if not preprocessed_data:
        print("  ❌ Không tìm thấy preprocessed data!")
        print("  💡 Hãy chạy JOB 1 trước!")
        return
    
    # Lấy metadata
    metadata = preprocessed_data.get('metadata', {})
    print(f"\n  📋 Metadata:")
    print(f"     - Timestamp: {metadata.get('timestamp', 'N/A')}")
    print(f"     - Semester: {metadata.get('semester', 'N/A')}")
    print(f"     - MaHocKy: {metadata.get('ma_hoc_ky', 'N/A')}")
    
    # Lấy dữ liệu
    dims = {k: v for k, v in preprocessed_data.items() if k.startswith('dim_')}
    fact_main = preprocessed_data.get('fact_gop_y_tu_luan', pd.DataFrame())
    fact_ketqua = preprocessed_data.get('fact_ket_qua_danh_gia', pd.DataFrame())
    
    print(f"\n  📊 Data summary:")
    print(f"     - Dimension tables: {len(dims)} tables")
    for name, df in dims.items():
        if not df.empty:
            print(f"        * {name}: {len(df):,} rows")
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
    print("\n" + "=" * 70)
    print("🚀 4. BẮT ĐẦU INSERT")
    print("=" * 70)
    
    insert_start = time.time()
    
    try:
        # Insert DIMENSION tables
        dim_results = insert_all_dimensions(cursor, dims)
        
        # Insert FACT tables
        fact_results = insert_fact_tables(cursor, fact_main, fact_ketqua)
        
        # Commit tất cả
        cursor.connection.commit()
        
        # Kiểm tra kết quả
        final_counts = verify_data(cursor)
        
    except Exception as e:
        print(f"\n  ❌ Lỗi: {e}")
        cursor.connection.rollback()
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    insert_time = time.time() - insert_start
    
    # 5. Thống kê
    total_time = time.time() - total_start
    
    print("\n" + "=" * 70)
    print("📊 KẾT QUẢ INSERT")
    print("=" * 70)
    
    total_inserted = 0
    if dim_results:
        print("\n  📌 DIMENSION tables (new records):")
        for table, count in dim_results.items():
            if count > 0:
                print(f"      ✅ {table}: {count:,} new rows")
                total_inserted += count
            else:
                print(f"      ⚪ {table}: no new rows")
    
    if fact_results:
        print("\n  📌 FACT tables (all records):")
        for table, count in fact_results.items():
            print(f"      ✅ {table}: {count:,} rows inserted")
            total_inserted += count
    
    print(f"\n  ✅ TOTAL inserted: {total_inserted:,} rows")
    print(f"  ⏱️ Insert time: {insert_time:.1f}s")
    
    if insert_time > 0:
        speed = total_inserted / insert_time
        print(f"  🚀 Speed: {speed:,.0f} rows/second")
    
    print("\n" + "=" * 70)
    print(f"✅ HOÀN THÀNH! Total time: {total_time:.1f}s")
    print("=" * 70)


if __name__ == "__main__":
    main()
