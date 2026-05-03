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
BATCH_SIZE = 50000


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
    
    # 1. DIM_KHOA - Xử lý đặc biệt
    print("\n    📌 DIM_KHOA")
    df = dims.get('dim_khoa')
    if df is not None and not df.empty:
        print(f"      Columns: {df.columns.tolist()}")
        print(f"      Sample:\n{df.head(2)}")
        
        # Xác định đúng cột MaKhoa và TenKhoa
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            # Nếu cột đầu tiên là tên khoa (chứa dấu cách, dài > 10)
            first_col = row.iloc[0]
            second_col = row.iloc[1] if len(row) > 1 else ''
            
            if isinstance(first_col, str) and (' ' in first_col or len(first_col) > 15):
                # Cột đầu là tên khoa, cần tạo mã
                ten_khoa = first_col
                # Tạo mã từ tên (lấy chữ cái đầu)
                ma_khoa = ''.join([w[0].upper() for w in ten_khoa.split() if w])[:10]
                if ma_khoa not in existing:
                    new_data.append((ma_khoa, ten_khoa))
                    existing.add(ma_khoa)
            else:
                # Cột đầu là mã
                ma_khoa = str(first_col)
                ten_khoa = second_col
                if ma_khoa not in existing and ma_khoa != 'nan':
                    new_data.append((ma_khoa, ten_khoa))
                    existing.add(ma_khoa)
        
        if new_data:
            sql = "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)"
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                try:
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    total += len(batch)
                    cursor.connection.commit()
                    print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
                except Exception as e:
                    print(f"      Batch error: {e}")
                    cursor.connection.rollback()
            results['DIM_KHOA'] = total
            print(f"      ✅ Inserted {total:,} new rows")
        else:
            print(f"      ⚪ No new rows")
    
    # 2. DIM_NGANH
    print("\n    📌 DIM_NGANH")
    df = dims.get('dim_nganh')
    if df is not None and not df.empty:
        print(f"      Columns: {df.columns.tolist()}")
        
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        valid_khoa = {row[0] for row in cursor.fetchall()}
        
        new_data = []
        for _, row in df.iterrows():
            # Xác định các cột
            cols = list(row.index)
            ma_nganh_col = cols[0] if len(cols) > 0 else None
            ten_nganh_col = cols[1] if len(cols) > 1 else None
            ma_khoa_col = cols[2] if len(cols) > 2 else None
            
            ma_nganh = str(row[ma_nganh_col]) if ma_nganh_col else ''
            ten_nganh = str(row[ten_nganh_col]) if ten_nganh_col else ''
            ma_khoa = str(row[ma_khoa_col]) if ma_khoa_col else ''
            
            # Nếu mã ngành có dấu cách, tạo mã mới
            if ' ' in ma_nganh or len(ma_nganh) > 15:
                ma_nganh = ''.join([w[0].upper() for w in ten_nganh.split() if w])[:10]
            
            if ma_nganh and ma_nganh not in existing and ma_khoa in valid_khoa:
                new_data.append((ma_nganh, ten_nganh[:100], ma_khoa))
                existing.add(ma_nganh)
        
        if new_data:
            sql = "INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                try:
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    total += len(batch)
                    cursor.connection.commit()
                    print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
                except Exception as e:
                    print(f"      Batch error: {e}")
                    cursor.connection.rollback()
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
        
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        valid_nganh = {row[0] for row in cursor.fetchall()}
        
        # Thêm các giá trị NULL mặc định
        default_cn = [
            ('NULL_CTS', 'Chuyên ngành NULL_CTS', 'NULL_CTS'),
            ('NULL_QT', 'Chuyên ngành NULL_QT', 'NULL_QT')
        ]
        
        new_data = []
        for ma_cn, ten_cn, ma_nganh in default_cn:
            if ma_cn not in existing and ma_nganh in valid_nganh:
                new_data.append((ma_cn, ten_cn, ma_nganh))
                existing.add(ma_cn)
        
        # Thêm dữ liệu từ file
        for _, row in df.iterrows():
            cols = list(row.index)
            ma_cn_col = cols[0] if len(cols) > 0 else None
            ten_cn_col = cols[1] if len(cols) > 1 else None
            ma_nganh_col = cols[2] if len(cols) > 2 else None
            
            ma_cn = str(row[ma_cn_col]) if ma_cn_col else ''
            ten_cn = str(row[ten_cn_col]) if ten_cn_col else ''
            ma_nganh = str(row[ma_nganh_col]) if ma_nganh_col else ''
            
            if ma_cn and ma_cn not in existing and ma_nganh in valid_nganh:
                new_data.append((ma_cn, ten_cn[:100], ma_nganh))
                existing.add(ma_cn)
        
        if new_data:
            sql = "INSERT INTO DIM_CHUYEN_NGANH (MaChuyenNganh, TenChuyenNganh, MaNganh) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                try:
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    total += len(batch)
                    cursor.connection.commit()
                    print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
                except Exception as e:
                    print(f"      Batch error: {e}")
                    cursor.connection.rollback()
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
            cols = list(row.index)
            ma_hp_col = cols[0] if len(cols) > 0 else None
            ten_hp_col = cols[1] if len(cols) > 1 else None
            ma_khoa_col = cols[2] if len(cols) > 2 else None
            
            ma_hp = str(row[ma_hp_col]) if ma_hp_col else ''
            ten_hp = str(row[ten_hp_col]) if ten_hp_col else ''
            ma_khoa = str(row[ma_khoa_col]) if ma_khoa_col else 'TĐHKT'
            
            if ma_hp and ma_hp not in existing and ma_khoa in valid_khoa:
                new_data.append((ma_hp, ten_hp[:200], ma_khoa))
                existing.add(ma_hp)
        
        if new_data:
            sql = "INSERT INTO DIM_HOC_PHAN (MaHP, TenHP, MaKhoa) VALUES (?, ?, ?)"
            total = 0
            for i in range(0, len(new_data), BATCH_SIZE):
                batch = new_data[i:i+BATCH_SIZE]
                try:
                    cursor.fast_executemany = True
                    cursor.executemany(sql, batch)
                    total += len(batch)
                    cursor.connection.commit()
                    print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows")
                except Exception as e:
                    print(f"      Batch error: {e}")
                    cursor.connection.rollback()
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
        
        new_data = []
        for _, row in df.iterrows():
            cols = list(row.index)
            ma_gv_col = cols[0] if len(cols) > 0 else None
            ho_dem_col = cols[1] if len(cols) > 1 else None
            ten_gv_col = cols[2] if len(cols) > 2 else None
            
            ma_gv = str(row[ma_gv_col]) if ma_gv_col else ''
            ho_dem = str(row[ho_dem_col])[:50] if ho_dem_col and row[ho_dem_col] else ''
            ten_gv = str(row[ten_gv_col])[:50] if ten_gv_col and row[ten_gv_col] else ''
            
            if ma_gv and ma_gv not in existing:
                new_data.append((ma_gv, ho_dem, ten_gv))
                existing.add(ma_gv)
        
        if new_data:
            sql = "INSERT INTO DIM_GIANG_VIEN (MaGV, HoDemGV, TenGV) VALUES (?, ?, ?)"
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
        
        new_data = []
        for _, row in df.iterrows():
            cols = list(row.index)
            ma_hk_col = cols[0] if len(cols) > 0 else None
            nam_hoc_col = cols[1] if len(cols) > 1 else None
            hoc_ky_col = cols[2] if len(cols) > 2 else None
            
            ma_hk = str(row[ma_hk_col]) if ma_hk_col else ''
            nam_hoc = str(row[nam_hoc_col]) if nam_hoc_col else ''
            hoc_ky = int(row[hoc_ky_col]) if hoc_ky_col and pd.notna(row[hoc_ky_col]) else 1
            
            if ma_hk and ma_hk not in existing:
                new_data.append((ma_hk, nam_hoc, hoc_ky))
                existing.add(ma_hk)
        
        if new_data:
            sql = "INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)"
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
            cols = list(row.index)
            ma_lop_col = cols[0] if len(cols) > 0 else None
            ten_lop_col = cols[1] if len(cols) > 1 else None
            ma_cn_col = cols[2] if len(cols) > 2 else None
            
            ma_lop = str(row[ma_lop_col]) if ma_lop_col else ''
            ten_lop = str(row[ten_lop_col]) if ten_lop_col else ''
            ma_cn = str(row[ma_cn_col]) if ma_cn_col else ''
            
            if ma_lop and ma_lop not in existing and ma_cn in valid_cn:
                new_data.append((ma_lop, ten_lop[:50], ma_cn))
                existing.add(ma_lop)
        
        if new_data:
            sql = "INSERT INTO DIM_LOP_SINH_VIEN (MaLop, Lop, MaChuyenNganh) VALUES (?, ?, ?)"
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
            cols = list(row.index)
            ma_sv_col = cols[0] if len(cols) > 0 else None
            ho_dem_col = cols[1] if len(cols) > 1 else None
            ten_col = cols[2] if len(cols) > 2 else None
            ngay_sinh_col = cols[3] if len(cols) > 3 else None
            ma_lop_col = cols[4] if len(cols) > 4 else None
            
            ma_sv = str(row[ma_sv_col]) if ma_sv_col else ''
            ma_lop = str(row[ma_lop_col]) if ma_lop_col else ''
            ho_dem = str(row[ho_dem_col])[:50] if ho_dem_col and row[ho_dem_col] else ''
            ten = str(row[ten_col])[:50] if ten_col and row[ten_col] else ''
            ngay_sinh = row[ngay_sinh_col] if ngay_sinh_col and pd.notna(row[ngay_sinh_col]) else None
            
            if ma_sv and ma_sv not in existing and ma_lop in valid_lop:
                new_data.append((ma_sv, ho_dem, ten, ngay_sinh, ma_lop))
                existing.add(ma_sv)
        
        if new_data:
            sql = "INSERT INTO DIM_SINH_VIEN (MaSV, HoDem, Ten, NgaySinh, MaLop) VALUES (?, ?, ?, ?, ?)"
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
            cols = list(row.index)
            ma_lhp_col = cols[0] if len(cols) > 0 else None
            ten_lhp_col = cols[1] if len(cols) > 1 else None
            ma_hp_col = cols[2] if len(cols) > 2 else None
            ma_gv_col = cols[3] if len(cols) > 3 else None
            ma_hk_col = cols[4] if len(cols) > 4 else None
            
            ma_lhp = str(row[ma_lhp_col]) if ma_lhp_col else ''
            ten_lhp = str(row[ten_lhp_col]) if ten_lhp_col else ''
            ma_hp = str(row[ma_hp_col]) if ma_hp_col else ''
            ma_gv = str(row[ma_gv_col]) if ma_gv_col else ''
            ma_hk = str(row[ma_hk_col]) if ma_hk_col else ''
            
            if ma_lhp and ma_lhp not in existing and ma_hp in valid_hp and ma_gv in valid_gv and ma_hk in valid_hk:
                new_data.append((ma_lhp, ten_lhp[:50], ma_hp, ma_gv, ma_hk))
                existing.add(ma_lhp)
        
        if new_data:
            sql = "INSERT INTO DIM_LOP_HOC_PHAN (MaLopHP, LopHP, MaHP, MaGV, MaHocKy) VALUES (?, ?, ?, ?, ?)"
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
    
    # FACT_GOP_Y_TU_LUAN
    print("\n    📌 FACT_GOP_Y_TU_LUAN")
    if fact_main is not None and not fact_main.empty:
        # Chuẩn bị dữ liệu với giới hạn độ dài
        fact_main = fact_main.copy()
        
        # Giới hạn độ dài text xuống 500 (buffer là 510)
        if 'NoiDungGopY' in fact_main.columns:
            fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].astype(str).str[:500]
        
        # Loại bỏ null
        fact_main = fact_main.dropna(subset=['SubmissionID', 'MaSV', 'LopHP'])
        
        # Xác định các cột có trong DataFrame
        columns = ['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY', 'Sentiment', 'Is_Valid', 'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']
        available_cols = [c for c in columns if c in fact_main.columns]
        
        data = fact_main[available_cols].values.tolist()
        print(f"      Preparing {len(data):,} rows...")
        
        placeholders = ', '.join(['?' for _ in available_cols])
        sql = f"INSERT INTO FACT_GOP_Y_TU_LUAN ({', '.join(available_cols)}) VALUES ({placeholders})"
        
        total = 0
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                conn.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows (total: {total:,})")
            except Exception as e:
                print(f"      Batch {i//BATCH_SIZE + 1} error: {e}")
                conn.rollback()
        
        results['FACT_GOP_Y_TU_LUAN'] = total
        print(f"      ✅ Inserted {total:,} rows")
    
    # FACT_KET_QUA_DANH_GIA
    print("\n    📌 FACT_KET_QUA_DANH_GIA")
    if fact_ketqua is not None and not fact_ketqua.empty:
        fact_ketqua = fact_ketqua.copy()
        
        # Loại bỏ duplicate
        fact_ketqua = fact_ketqua.drop_duplicates(subset=['SubmissionID', 'MaCauHoi'], keep='first')
        fact_ketqua = fact_ketqua.dropna(subset=['SubmissionID', 'MaCauHoi'])
        
        columns = ['SubmissionID', 'MaCauHoi', 'Diem']
        available_cols = [c for c in columns if c in fact_ketqua.columns]
        
        data = fact_ketqua[available_cols].values.tolist()
        print(f"      Preparing {len(data):,} rows...")
        
        placeholders = ', '.join(['?' for _ in available_cols])
        sql = f"INSERT INTO FACT_KET_QUA_DANH_GIA ({', '.join(available_cols)}) VALUES ({placeholders})"
        
        total = 0
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            try:
                cursor.fast_executemany = True
                cursor.executemany(sql, batch)
                total += len(batch)
                conn.commit()
                print(f"      Batch {i//BATCH_SIZE + 1}: {len(batch):,} rows (total: {total:,})")
            except Exception as e:
                print(f"      Batch {i//BATCH_SIZE + 1} error: {e}")
                # Thử insert từng dòng
                for row in batch:
                    try:
                        cursor.execute(sql, row)
                        total += 1
                        conn.commit()
                    except:
                        pass
                print(f"      Batch {i//BATCH_SIZE + 1}: Inserted {total} rows")
        
        results['FACT_KET_QUA_DANH_GIA'] = total
        print(f"      ✅ Inserted {total:,} rows")
    
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


# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 80)
    print("🚀 JOB 2: CHÈN DỮ LIỆU (FIX LỖI CẤU TRÚC)")
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
        print("  💡 Hãy chạy JOB 1 trước!")
        return
    
    # Lấy dữ liệu
    dims = {k: v for k, v in preprocessed_data.items() if k.startswith('dim_')}
    fact_main = preprocessed_data.get('fact_gop_y_tu_luan', pd.DataFrame())
    fact_ketqua = preprocessed_data.get('fact_ket_qua_danh_gia', pd.DataFrame())
    
    print(f"\n  📊 Data summary:")
    for name, df in dims.items():
        if not df.empty:
            print(f"     - {name}: {len(df):,} rows, columns: {df.columns.tolist()}")
    print(f"     - FACT_GOP_Y_TU_LUAN: {len(fact_main):,} rows, columns: {fact_main.columns.tolist() if not fact_main.empty else 'empty'}")
    print(f"     - FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} rows, columns: {fact_ketqua.columns.tolist() if not fact_ketqua.empty else 'empty'}")
    
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
        fact_results = insert_fact_tables(cursor, conn, fact_main, fact_ketqua)
        
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
