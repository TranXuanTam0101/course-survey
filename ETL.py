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

# ODBC Connection - Tối ưu
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

# Batch size tối ưu cho FACT
BATCH_SIZE_FACT = 500000  # Tăng lên 500k


# ================= BLOB FUNCTIONS =================
def download_preprocessed_data(blob_service, filename):
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


# ================= INSERT DIMENSION TABLES =================
def insert_dimension_tables(cursor, dims):
    print("\n  📥 Insert DIMENSION tables...")
    start = time.time()
    results = {}
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaKhoa'], row['TenKhoa']) for _, row in df.iterrows() if row['MaKhoa'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_KHOA'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) 
                    for _, row in df.iterrows() if row['MaNganh'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_NGANH'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']) 
                    for _, row in df.iterrows() if row['MaChuyenNganh'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_CHUYEN_NGANH'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) 
                    for _, row in df.iterrows() if row['MaHP'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_HOC_PHAN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) 
                    for _, row in df.iterrows() if row['MaGV'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_GIANG_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) 
                    for _, row in df.iterrows() if row['MaHocKy'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_HOC_KY'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaLop'], row['Lop'], row['MaChuyenNganh']) 
                    for _, row in df.iterrows() if row['MaLop'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_LOP_SINH_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaSV'], row['HoDem'], row['Ten'], row['NgaySinh'], row['MaLop']) 
                    for _, row in df.iterrows() if row['MaSV'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES (?, ?, ?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_SINH_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [(row['MaLopHP'], row['LopHP'], row['MaHP'], row['MaGV'], row['MaHocKy']) 
                    for _, row in df.iterrows() if row['MaLopHP'] not in existing]
        if new_data:
            sql = "INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), 100000):
                batch = new_data[i:i+100000]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//100000 + 1}: {len(batch):,} rows")
            results['DIM_LOP_HOC_PHAN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
    
    elapsed = time.time() - start
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.2f}s")
    return results


# ================= INSERT FACT TABLES - TỐI ƯU CAO NHẤT =================
def insert_fact_tables_super_fast(cursor, conn, fact_main, fact_ketqua):
    """Insert FACT tables với tốc độ siêu cao"""
    print("\n  🚀 SUPER FAST FACT INSERT")
    start = time.time()
    results = {}
    
    # === BƯỚC 1: TẮT TOÀN BỘ RÀNG BUỘC ===
    print("\n  ⚡ Disabling constraints and triggers...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        cursor.execute("DISABLE TRIGGER ALL ON FACT_GOP_Y_TU_LUAN")
        cursor.execute("DISABLE TRIGGER ALL ON FACT_KET_QUA_DANH_GIA")
        conn.commit()
        print("  ✅ Constraints and triggers disabled")
    except Exception as e:
        print(f"  ⚠️ Could not disable: {e}")
    
    # === BƯỚC 2: DROP NON-CLUSTERED INDEXES ===
    print("\n  🗑️ Dropping non-clustered indexes...")
    for table in ['FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA']:
        try:
            cursor.execute(f"""
                SELECT name FROM sys.indexes 
                WHERE object_id = OBJECT_ID('{table}')
                AND index_id > 1
                AND is_primary_key = 0
                AND is_unique_constraint = 0
                AND name NOT LIKE 'PK%'
            """)
            indexes = cursor.fetchall()
            for idx in indexes:
                try:
                    cursor.execute(f"DROP INDEX {idx[0]} ON {table}")
                    print(f"      Dropped: {idx[0]}")
                except:
                    pass
            conn.commit()
        except Exception as e:
            print(f"      Error on {table}: {e}")
    
    # === BƯỚC 3: INSERT FACT_KET_QUA_DANH_GIA (Nhanh hơn) ===
    print("\n  📥 Inserting FACT_KET_QUA_DANH_GIA...")
    if fact_ketqua is not None and not fact_ketqua.empty:
        # Loại bỏ duplicate
        fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
        fact_ketqua = fact_ketqua.dropna(subset=['SubmissionID', 'MaCauHoi'])
        
        data = fact_ketqua[['SubmissionID', 'MaCauHoi', 'Diem']].values.tolist()
        print(f"      Preparing {len(data):,} rows...")
        
        sql = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
        
        total = 0
        for i in range(0, len(data), BATCH_SIZE_FACT):
            batch = data[i:i+BATCH_SIZE_FACT]
            cursor.fast_executemany = True
            cursor.executemany(sql, batch)
            total += len(batch)
            conn.commit()
            print(f"      Batch {i//BATCH_SIZE_FACT + 1}: {len(batch):,} rows (total: {total:,})")
        
        results['FACT_KET_QUA_DANH_GIA'] = total
        print(f"  ✅ Inserted {total:,} rows in {time.time()-start:.1f}s")
    
    # === BƯỚC 4: INSERT FACT_GOP_Y_TU_LUAN ===
    print("\n  📥 Inserting FACT_GOP_Y_TU_LUAN...")
    if fact_main is not None and not fact_main.empty:
        fact_main = fact_main.copy()
        
        # Đảm bảo text không quá dài (nếu column là NVARCHAR(MAX) thì không cần)
        fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].astype(str)
        
        # Kiểm tra độ dài tối đa
        max_len = fact_main['NoiDungGopY'].str.len().max()
        print(f"      Max text length: {max_len} chars")
        
        # Nếu column là NVARCHAR(MAX) trong DB, không cần cắt
        # Nếu column có giới hạn, comment dòng dưới và uncomment dòng sau
        # fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].str[:4000]
        
        data = fact_main[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                         'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                         'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].values.tolist()
        
        print(f"      Preparing {len(data):,} rows...")
        
        sql = """INSERT INTO FACT_GOP_Y_TU_LUAN 
                 (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, 
                  Is_Valid, Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) 
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        
        total = 0
        for i in range(0, len(data), BATCH_SIZE_FACT):
            batch = data[i:i+BATCH_SIZE_FACT]
            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                conn.commit()
                print(f"      Batch {i//BATCH_SIZE_FACT + 1}: {len(batch):,} rows (total: {total:,})")
            except Exception as e:
                print(f"      Batch {i//BATCH_SIZE_FACT + 1} error: {e}")
                # Nếu lỗi, thử với batch nhỏ hơn
                sub_batch_size = 10000
                for j in range(0, len(batch), sub_batch_size):
                    sub_batch = batch[j:j+sub_batch_size]
                    try:
                        cursor.fast_executemany = True
                        cursor.executemany(sql, sub_batch)
                        total += len(sub_batch)
                        conn.commit()
                    except Exception as sub_e:
                        print(f"        Sub-batch error: {sub_e}")
                        # Insert từng dòng nếu vẫn lỗi
                        for row in sub_batch:
                            try:
                                cursor.execute(sql, row)
                                total += 1
                                conn.commit()
                            except:
                                pass
                print(f"      Batch {i//BATCH_SIZE_FACT + 1}: Retry completed")
        
        results['FACT_GOP_Y_TU_LUAN'] = total
        print(f"  ✅ Inserted {total:,} rows in {time.time()-start:.1f}s")
    
    # === BƯỚC 5: REBUILD INDEXES ===
    print("\n  🔨 Rebuilding indexes...")
    for table in ['FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA']:
        try:
            cursor.execute(f"ALTER INDEX ALL ON {table} REBUILD")
            print(f"      Rebuilt indexes on {table}")
        except Exception as e:
            print(f"      Error rebuilding on {table}: {e}")
    conn.commit()
    
    # === BƯỚC 6: BẬT LẠI RÀNG BUỘC ===
    print("\n  🔓 Enabling constraints and triggers...")
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.execute("ENABLE TRIGGER ALL ON FACT_GOP_Y_TU_LUAN")
        cursor.execute("ENABLE TRIGGER ALL ON FACT_KET_QUA_DANH_GIA")
        conn.commit()
        print("  ✅ Constraints and triggers enabled")
    except Exception as e:
        print(f"  ⚠️ Could not enable: {e}")
    
    # === BƯỚC 7: UPDATE STATISTICS ===
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
    print("🚀 JOB 2: CHÈN DỮ LIỆU (SIÊU TỐC - TỐI ƯU FACT)")
    print("=" * 80)
    print(f"📂 Survey: {SURVEY_FILE}")
    print(f"📁 Semester: {SEMESTER}")
    print(f"⚙️ FACT Batch size: {BATCH_SIZE_FACT:,} rows/batch")
    print("=" * 80)
    print("\n📌 SUPER FAST STRATEGY:")
    print("   1️⃣ Disable ALL constraints & triggers")
    print("   2️⃣ Drop non-clustered indexes")
    print("   3️⃣ Batch insert with 500k rows/batch")
    print("   4️⃣ Rebuild indexes after insert")
    print("   5️⃣ Update statistics")
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
        print("  💡 Hãy chạy JOB 1 trước!")
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
        dim_results = insert_dimension_tables(cursor, dims)
        
        # Insert FACT tables (tối ưu cao nhất)
        fact_results = insert_fact_tables_super_fast(cursor, conn, fact_main, fact_ketqua)
        
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
    
    total_inserted = sum(dim_results.values()) + sum(fact_results.values())
    
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
