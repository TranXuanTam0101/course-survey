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

# ODBC Connection
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

# Batch size
BATCH_SIZE = 100000


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
    """Insert dimension tables - chỉ insert mới"""
    print("\n  📥 Insert DIMENSION tables...")
    start = time.time()
    
    results = {}
    
    # 1. DIM_KHOA
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        # Lấy existing keys
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        existing = {row[0] for row in cursor.fetchall()}
        
        # Chuẩn bị dữ liệu - ĐÚNG thứ tự cột
        new_data = []
        for _, row in df.iterrows():
            ma_khoa = row['MaKhoa']
            ten_khoa = row['TenKhoa']
            if ma_khoa not in existing:
                new_data.append((ma_khoa, ten_khoa))
                existing.add(ma_khoa)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(2)])
            sql = f"INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_KHOA'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        # Lấy existing keys
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        # Kiểm tra MaKhoa tồn tại
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        valid_khoa = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_nganh = row['MaNganh']
            ma_khoa = row['MaKhoa']
            if ma_nganh not in existing and ma_khoa in valid_khoa:
                new_data.append((ma_nganh, row['TenNganh'], ma_khoa))
                existing.add(ma_nganh)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_NGANH'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    📌 DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        # Kiểm tra MaNganh tồn tại
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        valid_nganh = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_cn = row['MaChuyenNganh']
            ma_nganh = row['MaNganh']
            if ma_cn not in existing and ma_nganh in valid_nganh:
                new_data.append((ma_cn, row['TenChuyenNganh'], ma_nganh))
                existing.add(ma_cn)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_CHUYEN_NGANH'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 4. DIM_HOC_PHAN
    print("\n    📌 DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        valid_khoa = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_hp = row['MaHP']
            ma_khoa = row['MaKhoa']
            if ma_hp not in existing and ma_khoa in valid_khoa:
                new_data.append((ma_hp, row['TenHP'], ma_khoa))
                existing.add(ma_hp)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_HOC_PHAN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 5. DIM_GIANG_VIEN
    print("\n    📌 DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) 
                    for _, row in df.iterrows() if row['MaGV'] not in existing]
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_GIANG_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 6. DIM_HOC_KY
    print("\n    📌 DIM_HOC_KY")
    df = dims.get('dim_hoc_ky')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) 
                    for _, row in df.iterrows() if row['MaHocKy'] not in existing]
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_HOC_KY'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    📌 DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        valid_cn = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_lop = row['MaLop']
            ma_cn = row['MaChuyenNganh']
            if ma_lop not in existing and ma_cn in valid_cn:
                new_data.append((ma_lop, row['Lop'], ma_cn))
                existing.add(ma_lop)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(3)])
            sql = f"INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_LOP_SINH_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 8. DIM_SINH_VIEN
    print("\n    📌 DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
        valid_lop = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_sv = row['MaSV']
            ma_lop = row['MaLop']
            if ma_sv not in existing and ma_lop in valid_lop:
                new_data.append((ma_sv, row['HoDem'], row['Ten'], row['NgaySinh'], ma_lop))
                existing.add(ma_sv)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(5)])
            sql = f"INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_SINH_VIEN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    📌 DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan')
    if df is not None and not df.empty:
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        valid_hp = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        valid_gv = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
        valid_hk = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            ma_lhp = row['MaLopHP']
            ma_hp = row['MaHP']
            ma_gv = row['MaGV']
            ma_hk = row['MaHocKy']
            if ma_lhp not in existing and ma_hp in valid_hp and ma_gv in valid_gv and ma_hk in valid_hk:
                new_data.append((ma_lhp, row['LopHP'], ma_hp, ma_gv, ma_hk))
                existing.add(ma_lhp)
        
        if new_data:
            placeholders = ', '.join(['?' for _ in range(5)])
            sql = f"INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES ({placeholders})"
            
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                cursor.connection.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
            
            results['DIM_LOP_HOC_PHAN'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    elapsed = time.time() - start
    print(f"\n  ✅ DIMENSION tables done in {elapsed:.2f}s")
    return results


# ================= INSERT FACT TABLES =================
def insert_fact_tables(cursor, conn, fact_main, fact_ketqua):
    """Insert FACT tables"""
    print("\n  📥 Insert FACT tables...")
    start = time.time()
    
    results = {}
    
    # TẮT CONSTRAINTS
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        conn.commit()
        print("  ✅ Constraints disabled")
    except Exception as e:
        print(f"  ⚠️ Could not disable constraints: {e}")
    
    try:
        # 1. FACT_GOP_Y_TU_LUAN
        print("\n    📌 FACT_GOP_Y_TU_LUAN")
        if fact_main is not None and not fact_main.empty:
            columns = ['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                      'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                      'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']
            
            # GIỚI HẠN ĐỘ DÀI TEXT - QUAN TRỌNG!
            fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].astype(str).str[:500]
            
            # Loại bỏ null
            fact_main = fact_main.dropna(subset=['SubmissionID', 'MaSV', 'LopHP'])
            
            data = fact_main[columns].values.tolist()
            print(f"      Preparing {len(data):,} rows...")
            
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
            print(f"      ✅ Inserted {total:,} rows")
        
        # 2. FACT_KET_QUA_DANH_GIA
        print("\n    📌 FACT_KET_QUA_DANH_GIA")
        if fact_ketqua is not None and not fact_ketqua.empty:
            columns = ['SubmissionID', 'MaCauHoi', 'Diem']
            
            # Loại bỏ duplicate
            fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
            
            data = fact_ketqua[columns].values.tolist()
            print(f"      Preparing {len(data):,} rows...")
            
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
                    # Nếu lỗi duplicate, thử từng dòng
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
            print(f"      ✅ Inserted {total:,} rows")
        
        conn.commit()
        
    except Exception as e:
        print(f"  ❌ Error during insert: {e}")
        conn.rollback()
        raise
    
    # BẬT LẠI CONSTRAINTS
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        conn.commit()
        print("\n  ✅ Constraints enabled")
    except Exception as e:
        print(f"  ⚠️ Could not enable constraints: {e}")
    
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
    print("🚀 JOB 2: CHÈN DỮ LIỆU")
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
        print("  ✅ Connected")
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return
    
    # 4. Insert dữ liệu
    print("\n" + "=" * 80)
    print("🚀 4. BẮT ĐẦU INSERT")
    print("=" * 80)
    
    insert_start = time.time()
    dim_results = {}
    fact_results = {}
    
    try:
        # Insert DIMENSION tables
        dim_results = insert_dimension_tables(cursor, dims)
        
        # Insert FACT tables
        fact_results = insert_fact_tables(cursor, conn, fact_main, fact_ketqua)
        
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
    
    print(f"\n✅ HOÀN THÀNH! Total time: {total_time:.2f}s")
    print("=" * 80)


if __name__ == "__main__":
    main()
