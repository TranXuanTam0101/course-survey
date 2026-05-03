import os
import sys
import time
import pickle
import pyodbc
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient

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
)

CONTAINER_NAME = SEMESTER
PREPROCESSED_PATH = "preprocessed-data"

# Batch size cho insert
BATCH_SIZE = 100000


# ================= BLOB FUNCTIONS =================
def download_preprocessed_data(blob_service, filename):
    """Tải dữ liệu đã tiền xử lý từ blob"""
    path = f"{PREPROCESSED_PATH}/{filename}.pkl"
    try:
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            pickled_data = blob.download_blob().readall()
            return pickle.loads(pickled_data)
        return None
    except Exception as e:
        print(f"  ❌ Lỗi tải preprocessed data: {e}")
        return None


# ================= DATABASE FUNCTIONS =================
def batch_insert(cursor, table, columns, data, batch_size=100000):
    """Batch insert dữ liệu vào database"""
    if data is None or len(data) == 0:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(sql, batch)
        total += len(batch)
        if total % 100000 == 0:
            print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
        cursor.connection.commit()
    return total


def insert_dimension_tables(cursor, preprocessed_data):
    """
    Insert dữ liệu vào các bảng DIMENSION
    CHỈ INSERT NHỮNG BẢN GHI CHƯA TỒN TẠI
    """
    print("\n  📥 Insert DIMENSION tables (chỉ insert mới)...")
    
    total_inserted = {}
    
    # 1. DIM_KHOA
    print("\n    -> DIM_KHOA")
    df = preprocessed_data['dim_khoa']
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaKhoa'], row['TenKhoa']) for _, row in df.iterrows() if row['MaKhoa'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data, BATCH_SIZE)
        total_inserted['DIM_KHOA'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 2. DIM_NGANH
    print("\n    -> DIM_NGANH")
    df = preprocessed_data['dim_nganh']
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) for _, row in df.iterrows() if row['MaNganh'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data, BATCH_SIZE)
        total_inserted['DIM_NGANH'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    -> DIM_CHUYEN_NGANH")
    df = preprocessed_data['dim_chuyen_nganh']
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']) 
                for _, row in df.iterrows() if row['MaChuyenNganh'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data, BATCH_SIZE)
        total_inserted['DIM_CHUYEN_NGANH'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 4. DIM_HOC_PHAN
    print("\n    -> DIM_HOC_PHAN")
    df = preprocessed_data['dim_hoc_phan']
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) for _, row in df.iterrows() if row['MaHP'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data, BATCH_SIZE)
        total_inserted['DIM_HOC_PHAN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 5. DIM_GIANG_VIEN
    print("\n    -> DIM_GIANG_VIEN")
    df = preprocessed_data['dim_giang_vien']
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) for _, row in df.iterrows() if row['MaGV'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data, BATCH_SIZE)
        total_inserted['DIM_GIANG_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 6. DIM_HOC_KY
    print("\n    -> DIM_HOC_KY")
    df = preprocessed_data['dim_hoc_ky']
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) for _, row in df.iterrows() if row['MaHocKy'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data, BATCH_SIZE)
        total_inserted['DIM_HOC_KY'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    -> DIM_LOP_SINH_VIEN")
    df = preprocessed_data['dim_lop_sinh_vien']
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaLop'], row['Lop'], row['MaChuyenNganh']) for _, row in df.iterrows() if row['MaLop'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data, BATCH_SIZE)
        total_inserted['DIM_LOP_SINH_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 8. DIM_SINH_VIEN
    print("\n    -> DIM_SINH_VIEN")
    df = preprocessed_data['dim_sinh_vien']
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaSV'], row['HoDem'], row['Ten'], row['NgaySinh'], row['MaLop']) 
                for _, row in df.iterrows() if row['MaSV'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data, BATCH_SIZE)
        total_inserted['DIM_SINH_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    -> DIM_LOP_HOC_PHAN")
    df = preprocessed_data['dim_lop_hoc_phan']
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaLopHP'], row['LopHP'], row['MaHP'], row['MaGV'], row['MaHocKy']) 
                for _, row in df.iterrows() if row['MaLopHP'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data, BATCH_SIZE)
        total_inserted['DIM_LOP_HOC_PHAN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    return total_inserted


def insert_fact_tables(cursor, preprocessed_data):
    """
    Insert dữ liệu vào các bảng FACT
    CHÈN THÊM MỚI (KHÔNG KIỂM TRA TỒN TẠI)
    """
    print("\n  📥 Insert FACT tables (chèn thêm mới)...")
    
    total_inserted = {}
    
    # TẮT CONSTRAINTS để insert nhanh hơn
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    try:
        # 1. FACT_GOP_Y_TU_LUAN - CHÈN THÊM MỚI (KHÔNG CHECK)
        print("\n    -> FACT_GOP_Y_TU_LUAN")
        df = preprocessed_data['fact_gop_y_tu_luan']
        
        if not df.empty:
            # Chuyển đổi dữ liệu thành list of tuples
            data = [(row['SubmissionID'], row['MaSV'], row['LopHP'], row['NoiDungGopY'],
                     row['Sentiment'], row['Is_Valid'], row['Tag_HocPhan'], 
                     row['Tag_DayHoc'], row['Tag_KiemTra'], row['Tag_Khac']) 
                    for _, row in df.iterrows()]
            
            if data:
                inserted = batch_insert(cursor, 'FACT_GOP_Y_TU_LUAN', 
                                       ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY',
                                        'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                        'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'], 
                                       data, BATCH_SIZE)
                total_inserted['FACT_GOP_Y_TU_LUAN'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới (KHÔNG KIỂM TRA TRÙNG)")
            else:
                print(f"      ⚠️ Không có dữ liệu để insert")
        else:
            print(f"      ⚠️ Không có dữ liệu tự luận")
        
        # 2. FACT_KET_QUA_DANH_GIA - CHÈN THÊM MỚI (KHÔNG CHECK)
        print("\n    -> FACT_KET_QUA_DANH_GIA")
        df = preprocessed_data['fact_ket_qua_danh_gia']
        
        if not df.empty:
            # Chuyển đổi dữ liệu thành list of tuples
            data = [(row['SubmissionID'], row['MaCauHoi'], row['Diem']) 
                    for _, row in df.iterrows()]
            
            if data:
                inserted = batch_insert(cursor, 'FACT_KET_QUA_DANH_GIA', 
                                       ['SubmissionID', 'MaCauHoi', 'Diem'], 
                                       data, BATCH_SIZE)
                total_inserted['FACT_KET_QUA_DANH_GIA'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới (KHÔNG KIỂM TRA TRÙNG)")
            else:
                print(f"      ⚠️ Không có dữ liệu để insert")
        else:
            print(f"      ⚠️ Không có dữ liệu trắc nghiệm")
        
        cursor.connection.commit()
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        # BẬT LẠI CONSTRAINTS
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
        cursor.connection.commit()
    
    return total_inserted


# ================= MAIN INSERT JOB =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 JOB 2: CHÈN DỮ LIỆU VÀO DATABASE")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
    print("=" * 60)
    print("\n📌 LOGIC:")
    print("   - DIMENSION tables: CHỈ INSERT bản ghi CHƯA TỒN TẠI")
    print("   - FACT tables: INSERT THÊM MỚI (có thể trùng lặp)")
    print("=" * 60)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    # 2. Tải dữ liệu đã tiền xử lý
    print("\n📥 2. Tải dữ liệu đã tiền xử lý...")
    preprocessed_data = download_preprocessed_data(blob_service, f"{FILE_NAME}_preprocessed")
    
    if not preprocessed_data:
        print("  ❌ Không tìm thấy dữ liệu đã tiền xử lý!")
        print("  💡 Hãy chạy JOB 1 trước!")
        return
    
    print(f"  ✅ Đã tải dữ liệu từ {preprocessed_data['metadata']['timestamp']}")
    print(f"     - Học kỳ: {preprocessed_data['metadata']['ma_hoc_ky']}")
    print(f"     - Số lượng DIM_KHOA: {len(preprocessed_data['dim_khoa']):,}")
    print(f"     - Số lượng DIM_NGANH: {len(preprocessed_data['dim_nganh']):,}")
    print(f"     - Số lượng DIM_CHUYEN_NGANH: {len(preprocessed_data['dim_chuyen_nganh']):,}")
    print(f"     - Số lượng DIM_HOC_PHAN: {len(preprocessed_data['dim_hoc_phan']):,}")
    print(f"     - Số lượng DIM_GIANG_VIEN: {len(preprocessed_data['dim_giang_vien']):,}")
    print(f"     - Số lượng DIM_LOP_SINH_VIEN: {len(preprocessed_data['dim_lop_sinh_vien']):,}")
    print(f"     - Số lượng DIM_SINH_VIEN: {len(preprocessed_data['dim_sinh_vien']):,}")
    print(f"     - Số lượng DIM_LOP_HOC_PHAN: {len(preprocessed_data['dim_lop_hoc_phan']):,}")
    print(f"     - Số lượng FACT_GOP_Y_TU_LUAN: {len(preprocessed_data['fact_gop_y_tu_luan']):,}")
    print(f"     - Số lượng FACT_KET_QUA_DANH_GIA: {len(preprocessed_data['fact_ket_qua_danh_gia']):,}")
    
    # 3. Kết nối Database
    print("\n💾 3. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Kết nối SQL thành công")
    except Exception as e:
        print(f"  ❌ Lỗi kết nối SQL: {e}")
        return
    
    # 4. Insert vào database
    db_start = time.time()
    
    try:
        # Insert DIMENSION tables (CHỈ INSERT MỚI)
        dim_inserted = insert_dimension_tables(cursor, preprocessed_data)
        
        # Insert FACT tables (CHÈN THÊM MỚI - KHÔNG CHECK)
        fact_inserted = insert_fact_tables(cursor, preprocessed_data)
        
        cursor.connection.commit()
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    db_time = time.time() - db_start
    
    # 5. Thống kê
    total_time = time.time() - total_start
    print("\n📊 5. KẾT QUẢ INSERT:")
    print(f"   Thời gian insert: {db_time:.1f}s")
    
    print("\n   📌 DIMENSION tables (CHỈ INSERT MỚI):")
    if dim_inserted:
        for table, count in dim_inserted.items():
            print(f"      - {table}: {count:,} dòng mới")
    else:
        print(f"      - Không có dòng mới nào được insert")
    
    print("\n   📌 FACT tables (CHÈN THÊM MỚI - KHÔNG KIỂM TRA):")
    if fact_inserted:
        for table, count in fact_inserted.items():
            print(f"      - {table}: {count:,} dòng đã insert")
    else:
        print(f"      - Không có dữ liệu để insert")
    
    # Cảnh báo về duplicate trong FACT tables
    if fact_inserted:
        print("\n   ⚠️ LƯU Ý:")
        print(f"      - FACT tables có thể bị trùng lặp dữ liệu")
        print(f"      - Nếu muốn tránh trùng, hãy xóa dữ liệu cũ trước hoặc thêm logic check")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH INSERT! Thời gian: {total_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
