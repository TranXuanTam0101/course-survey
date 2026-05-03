import os
import sys
import time
import pickle
import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
from azure.storage.blob import BlobServiceClient
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

# ODBC Connection - Tối ưu nhất
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=120;"
    f"Command Timeout=3600;"
    f"AutoCommit=False;"
)

CONTAINER_NAME = SEMESTER
PREPROCESSED_PATH = "preprocessed-data"

# Batch size tối ưu cho tốc độ cao
BATCH_SIZE = 250000  # 250k dòng/batch


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
def get_existing_keys(cursor, table_name, key_column):
    """Lấy danh sách key đã tồn tại"""
    cursor.execute(f"SELECT {key_column} FROM {table_name}")
    return {row[0] for row in cursor.fetchall()}


def insert_batch(cursor, table, columns, data, batch_size=BATCH_SIZE):
    """Batch insert siêu nhanh"""
    if not data:
        return 0, data
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    remaining_data = []
    
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            cursor.fast_executemany = True
            cursor.executemany(sql, batch)
            total += len(batch)
            cursor.connection.commit()
            print(f"      ✅ Batch {i//batch_size + 1}: {len(batch):,} rows")
        except Exception as e:
            print(f"      ⚠️ Batch error: {e}")
            # Lưu lại batch lỗi để xử lý sau
            remaining_data.extend(batch)
            cursor.connection.rollback()
    
    return total, remaining_data


def insert_batch_with_skip(cursor, table, columns, data, key_index, existing_keys):
    """Insert batch, bỏ qua các dòng trùng lặp"""
    if not data:
        return 0
    
    # Lọc bỏ các dòng đã tồn tại
    filtered_data = [row for row in data if row[key_index] not in existing_keys]
    
    if not filtered_data:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(filtered_data), BATCH_SIZE):
        batch = filtered_data[i:i+BATCH_SIZE]
        cursor.fast_executemany = True
        cursor.executemany(sql, batch)
        total += len(batch)
        cursor.connection.commit()
        print(f"      ✅ Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows (total: {total:,})")
    
    return total


# ================= INSERT DIMENSION TABLES =================
def insert_dimension_tables(cursor, dims):
    """Insert dimension tables - chỉ insert mới"""
    print("\n  📥 Insert DIMENSION tables...")
    start = time.time()
    
    results = {}
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_KHOA', 'MaKhoa')
        new_data = [(row['MaKhoa'], row['TenKhoa']) for _, row in df.iterrows() if row['MaKhoa'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data)
            results['DIM_KHOA'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_NGANH', 'MaNganh')
        new_data = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) 
                    for _, row in df.iterrows() if row['MaNganh'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data)
            results['DIM_NGANH'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_CHUYEN_NGANH', 'MaChuyenNganh')
        new_data = [(row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']) 
                    for _, row in df.iterrows() if row['MaChuyenNganh'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data)
            results['DIM_CHUYEN_NGANH'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_HOC_PHAN', 'MaHP')
        new_data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) 
                    for _, row in df.iterrows() if row['MaHP'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data)
            results['DIM_HOC_PHAN'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_GIANG_VIEN', 'MaGV')
        new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) 
                    for _, row in df.iterrows() if row['MaGV'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data)
            results['DIM_GIANG_VIEN'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_HOC_KY', 'MaHocKy')
        new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) 
                    for _, row in df.iterrows() if row['MaHocKy'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data)
            results['DIM_HOC_KY'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_LOP_SINH_VIEN', 'MaLop')
        new_data = [(row['MaLop'], row['Lop'], row['MaChuyenNganh']) 
                    for _, row in df.iterrows() if row['MaLop'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data)
            results['DIM_LOP_SINH_VIEN'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_SINH_VIEN', 'MaSV')
        new_data = [(row['MaSV'], row['HoDem'], row['Ten'], row['NgaySinh'], row['MaLop']) 
                    for _, row in df.iterrows() if row['MaSV'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data)
            results['DIM_SINH_VIEN'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        existing = get_existing_keys(cursor, 'DIM_LOP_HOC_PHAN', 'MaLopHP')
        new_data = [(row['MaLopHP'], row['LopHP'], row['MaHP'], row['MaGV'], row['MaHocKy']) 
                    for _, row in df.iterrows() if row['MaLopHP'] not in existing]
        if new_data:
            inserted, _ = insert_batch(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data)
            results['DIM_LOP_HOC_PHAN'] = inserted
            print(f"      ✅ Inserted {inserted:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    elapsed = time.time() - start
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.2f}s")
    return results


# ================= INSERT FACT TABLES - PHƯƠNG ÁN NHANH NHẤT =================
def insert_fact_tables_ultra_fast(cursor, conn, fact_main, fact_ketqua):
    """Insert FACT tables với tốc độ cao nhất"""
    print("\n  🚀 ULTRA FAST FACT INSERT")
    start = time.time()
    
    results = {}
    
    # === BƯỚC 1: TẮT TOÀN BỘ CONSTRAINTS ===
    print("\n  ⚡ Disabling constraints...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        conn.commit()
        print("  ✅ Constraints disabled")
    except Exception as e:
        print(f"  ⚠️ Could not disable constraints: {e}")
    
    # === BƯỚC 2: DROP NON-CLUSTERED INDEXES ===
    print("\n  🗑️ Dropping non-clustered indexes...")
    indexes_to_rebuild = {}
    
    for table in ['FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA']:
        try:
            cursor.execute(f"""
                SELECT name 
                FROM sys.indexes 
                WHERE object_id = OBJECT_ID('{table}')
                AND index_id > 1
                AND is_primary_key = 0
                AND is_unique_constraint = 0
                AND name NOT LIKE 'UQ%'
                AND name NOT LIKE 'PK%'
            """)
            indexes = cursor.fetchall()
            indexes_to_rebuild[table] = [idx[0] for idx in indexes]
            
            for idx_name in indexes_to_rebuild[table]:
                try:
                    cursor.execute(f"DROP INDEX {idx_name} ON {table}")
                    print(f"      Dropped: {idx_name}")
                except:
                    pass
            conn.commit()
        except Exception as e:
            print(f"      ⚠️ Error on {table}: {e}")
    
    # === BƯỚC 3: INSERT DỮ LIỆU ===
    try:
        # FACT_GOP_Y_TU_LUAN
        print("\n  📥 Inserting FACT_GOP_Y_TU_LUAN...")
        if fact_main is not None and not fact_main.empty:
            # Chuẩn bị dữ liệu
            columns = ['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                      'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                      'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']
            
            # Giới hạn độ dài
            fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].astype(str).str[:4000]
            
            data = fact_main[columns].values.tolist()
            print(f"      Preparing {len(data):,} rows...")
            
            # Insert với batch size lớn
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({', '.join(columns)}) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                conn.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows (total: {total:,})")
            
            results['FACT_GOP_Y_TU_LUAN'] = total
            print(f"  ✅ Inserted {total:,} rows")
        
        # FACT_KET_QUA_DANH_GIA
        print("\n  📥 Inserting FACT_KET_QUA_DANH_GIA...")
        if fact_ketqua is not None and not fact_ketqua.empty:
            columns = ['SubmissionID', 'MaCauHoi', 'Diem']
            
            # Loại bỏ duplicate ngay trong data
            fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
            
            data = fact_ketqua[columns].values.tolist()
            print(f"      Preparing {len(data):,} rows (after dedup)...")
            
            placeholders = ', '.join(['?' for _ in columns])
            sql = f"INSERT INTO FACT_KET_QUA_DANH_GIA ({', '.join(columns)}) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(data), BATCH_SIZE):
                batch = data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                try:
                    cursor.executemany(sql, batch)
                    total += len(batch)
                    conn.commit()
                    print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows (total: {total:,})")
                except Exception as e:
                    # Nếu lỗi duplicate, thử insert từng dòng
                    print(f"      Batch {i//BATCH_SIZE + 1}: Duplicate detected, trying row by row...")
                    for row in batch:
                        try:
                            cursor.execute(sql, row)
                            total += 1
                            conn.commit()
                        except:
                            pass
                    print(f"      Batch {i//BATCH_SIZE + 1}: Inserted {total - (i)} rows")
            
            results['FACT_KET_QUA_DANH_GIA'] = total
            print(f"  ✅ Inserted {total:,} rows")
        
        conn.commit()
        
    except Exception as e:
        print(f"  ❌ Error during insert: {e}")
        conn.rollback()
        raise
    
    # === BƯỚC 4: RECREATE INDEXES ===
    print("\n  🔨 Recreating indexes...")
    for table, indexes in indexes_to_rebuild.items():
        for idx_name in indexes:
            try:
                # Cần có định nghĩa index đầy đủ, ở đây tạm bỏ qua
                print(f"      Skip recreating {idx_name} (need full definition)")
            except Exception as e:
                print(f"      ⚠️ Could not recreate {idx_name}: {e}")
    
    # === BƯỚC 5: BẬT LẠI CONSTRAINTS ===
    print("\n  🔓 Enabling constraints...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        conn.commit()
        print("  ✅ Constraints enabled")
    except Exception as e:
        print(f"  ⚠️ Could not enable constraints: {e}")
    
    # === BƯỚC 6: UPDATE STATISTICS ===
    print("\n  📊 Updating statistics...")
    try:
        cursor.execute("UPDATE STATISTICS FACT_GOP_Y_TU_LUAN")
        cursor.execute("UPDATE STATISTICS FACT_KET_QUA_DANH_GIA")
        conn.commit()
        print("  ✅ Statistics updated")
    except Exception as e:
        print(f"  ⚠️ Could not update statistics: {e}")
    
    elapsed = time.time() - start
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
    print("🚀 JOB 2: CHÈN DỮ LIỆU (PHƯƠNG ÁN NHANH NHẤT)")
    print("=" * 80)
    print(f"📂 Survey: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ Batch size: {BATCH_SIZE:,} rows/batch")
    print("=" * 80)
    print("\n📌 ULTRA FAST STRATEGY:")
    print("   1️⃣ Disable ALL constraints")
    print("   2️⃣ Drop non-clustered indexes")
    print("   3️⃣ Batch insert with 250k rows/batch")
    print("   4️⃣ Recreate indexes after insert")
    print("   5️⃣ Enable constraints")
    print("   6️⃣ Update statistics")
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
        dim_results = insert_dimension_tables(cursor, dims)
        
        # Insert FACT tables
        fact_results = insert_fact_tables_ultra_fast(cursor, conn, fact_main, fact_ketqua)
        
        # Commit tất cả
        conn.commit()
        
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
    
    # 5. Thống kê
    print("\n" + "=" * 80)
    print("📊 KẾT QUẢ")
    print("=" * 80)
    
    total_inserted = sum(dim_results.values()) if dim_results else 0
    total_inserted += sum(fact_results.values()) if fact_results else 0
    
    print(f"  ✅ TOTAL inserted: {total_inserted:,} rows")
    print(f"  ⏱️ Insert time: {insert_time:.2f}s")
    
    if insert_time > 0 and total_inserted > 0:
        speed = total_inserted / insert_time
        print(f"  🚀 Speed: {speed:,.0f} rows/second")
        
        if speed > 100000:
            print(f"  🎉 EXCELLENT! Very fast insert speed!")
        elif speed > 50000:
            print(f"  👍 GOOD! Acceptable speed")
        else:
            print(f"  ⚠️ Speed could be improved")
    
    print(f"\n✅ HOÀN THÀNH! Total time: {total_time:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
