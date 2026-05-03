import os
import sys
import time
import pickle
import io
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
    f"AutoCommit=False;"  # Tắt auto commit để batch insert nhanh
)

CONTAINER_NAME = SEMESTER
PROCESSED_PATH = "processed-data"
PREPROCESSED_PATH = "preprocessed-data"

# Batch size lớn hơn để insert nhanh
BATCH_SIZE = 100000  # Tăng lên 100k
PARALLEL_WORKERS = 2  # Số luồng parallel insert

# ================= COMPILE REGEX PATTERNS =================
import re
_sentiment_patterns = {
    'positive': re.compile(r'(tuyệt|tốt|hay|ổn|hài lòng|cảm ơn|ok|great|excellent|thoải mái|vui|sôi nổi|hấp dẫn|dễ|thân thiện|tâm lý|tận tâm|nhiệt tình|chu đáo|chi tiết|sáng tạo|thực tế|hiệu quả)'),
    'negative': re.compile(r'(tệ|kém|dở|chán|khó|mông lung|lan man|dài dòng|qua loa|chắp vá|đọc chép|cứng nhắc|đơn điệu|thiếu|cũ kỹ|nhanh|lố giờ|không|chưa|chẳng)'),
    'very_positive': re.compile(r'(tuyệt vời|xuất sắc|hoàn hảo|quá tuyệt|siêu|rất tốt|cực kỳ)'),
    'very_negative': re.compile(r'(tệ hại|tồi tệ|thất vọng|quá khó|rất chán)')
}

_tag_patterns = {
    'Tag_HocPhan': re.compile(r'(chuẩn đầu ra|mục tiêu|nội dung|chương trình|môn học|trang bị|cung cấp|đào tạo|bám sát|phù hợp|rõ ràng|đầy đủ)', re.IGNORECASE),
    'Tag_DayHoc': re.compile(r'(giảng viên|thầy|cô|tận tâm|nhiệt tình|truyền cảm hứng|dạy|giảng|bài giảng|sinh động|linh hoạt|tương tác|dễ hiểu)', re.IGNORECASE),
    'Tag_KiemTra': re.compile(r'(kiểm tra|đánh giá|công bằng|minh bạch|thi|đề thi|cho điểm|công khai)', re.IGNORECASE)
}


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


def download_csv_backup(blob_service, filename_pattern):
    """Tải CSV backup mới nhất"""
    try:
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        blobs = container_client.list_blobs(name_starts_with=f"{PROCESSED_PATH}/")
        
        matching_files = []
        for blob in blobs:
            if filename_pattern in blob.name:
                matching_files.append(blob.name)
        
        if not matching_files:
            return None
        
        latest_file = sorted(matching_files)[-1]
        blob_client = container_client.get_blob_client(latest_file)
        content = blob_client.download_blob().readall().decode('utf-8-sig')
        
        return pd.read_csv(io.StringIO(content))
    except Exception as e:
        print(f"  ❌ Lỗi tải CSV: {e}")
        return None


# ================= NLP FUNCTIONS TỐI ƯU =================
def analyze_sentiment_fast(text: str) -> str:
    """Phân tích sentiment nhanh"""
    if not isinstance(text, str) or len(text.strip()) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    if _sentiment_patterns['very_positive'].search(text_lower):
        return 'positive'
    if _sentiment_patterns['very_negative'].search(text_lower):
        return 'negative'
    
    pos_count = len(_sentiment_patterns['positive'].findall(text_lower))
    neg_count = len(_sentiment_patterns['negative'].findall(text_lower))
    
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    return 'neutral'


def extract_tags_fast(text: str) -> tuple:
    """Trích xuất tags nhanh"""
    if not isinstance(text, str):
        return (0, 0, 0, 1)
    
    text_lower = text.lower()
    
    tag_hp = 1 if _tag_patterns['Tag_HocPhan'].search(text_lower) else 0
    tag_dh = 1 if _tag_patterns['Tag_DayHoc'].search(text_lower) else 0
    tag_kt = 1 if _tag_patterns['Tag_KiemTra'].search(text_lower) else 0
    tag_khac = 1 if (tag_hp + tag_dh + tag_kt) == 0 else 0
    
    return (tag_hp, tag_dh, tag_kt, tag_khac)


def process_nlp_batch_vectorized(df):
    """Xử lý NLP vectorized - NHANH HƠN nhiều so với loop"""
    texts = df['NoiDungGopY'].fillna('').astype(str).values
    
    # Sử dụng list comprehension nhưng với vectorized operations
    sentiments = [analyze_sentiment_fast(t) for t in texts]
    tags = [extract_tags_fast(t) for t in texts]
    
    df['Sentiment'] = sentiments
    df['Tag_HocPhan'] = [t[0] for t in tags]
    df['Tag_DayHoc'] = [t[1] for t in tags]
    df['Tag_KiemTra'] = [t[2] for t in tags]
    df['Tag_Khac'] = [t[3] for t in tags]
    df['Is_Valid'] = 1
    
    return df


# ================= DATABASE FUNCTIONS TỐI ƯU =================
def fast_batch_insert(cursor, table, columns, data, batch_size=BATCH_SIZE):
    """Batch insert siêu nhanh với executemany"""
    if not data:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.fast_executemany = True  # Bật fast_executemany
        cursor.executemany(sql, batch)
        total += len(batch)
        cursor.connection.commit()  # Commit mỗi batch
        
        if total % 500000 == 0:
            print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
    
    return total


def insert_dimension_tables_fast(cursor, dims):
    """Insert DIMENSION tables siêu nhanh - chỉ insert mới"""
    print("\n  📥 Insert DIMENSION tables (chỉ insert mới)...")
    total_inserted = {}
    
    # Gom tất cả các câu lệnh SELECT vào 1 lần
    existing_data = {}
    
    # 1. DIM_KHOA
    print("\n    -> DIM_KHOA")
    df = dims.get('dim_khoa', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaKhoa'], row['TenKhoa']) for _, row in df.iterrows() if row['MaKhoa'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data)
            total_inserted['DIM_KHOA'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 2. DIM_NGANH
    print("\n    -> DIM_NGANH")
    df = dims.get('dim_nganh', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) 
                    for _, row in df.iterrows() if row['MaNganh'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data)
            total_inserted['DIM_NGANH'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    -> DIM_CHUYEN_NGANH")
    df = dims.get('dim_chuyen_nganh', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']) 
                    for _, row in df.iterrows() if row['MaChuyenNganh'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data)
            total_inserted['DIM_CHUYEN_NGANH'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 4. DIM_HOC_PHAN
    print("\n    -> DIM_HOC_PHAN")
    df = dims.get('dim_hoc_phan', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) for _, row in df.iterrows() if row['MaHP'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data)
            total_inserted['DIM_HOC_PHAN'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 5. DIM_GIANG_VIEN
    print("\n    -> DIM_GIANG_VIEN")
    df = dims.get('dim_giang_vien', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) for _, row in df.iterrows() if row['MaGV'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data)
            total_inserted['DIM_GIANG_VIEN'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 6. DIM_HOC_KY
    print("\n    -> DIM_HOC_KY")
    df = dims.get('dim_hoc_ky', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) for _, row in df.iterrows() if row['MaHocKy'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data)
            total_inserted['DIM_HOC_KY'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    -> DIM_LOP_SINH_VIEN")
    df = dims.get('dim_lop_sinh_vien', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaLop'], row['Lop'], row['MaChuyenNganh']) for _, row in df.iterrows() if row['MaLop'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data)
            total_inserted['DIM_LOP_SINH_VIEN'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 8. DIM_SINH_VIEN
    print("\n    -> DIM_SINH_VIEN")
    df = dims.get('dim_sinh_vien', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaSV'], row['HoDem'], row['Ten'], row['NgaySinh'], row['MaLop']) 
                    for _, row in df.iterrows() if row['MaSV'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data)
            total_inserted['DIM_SINH_VIEN'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    -> DIM_LOP_HOC_PHAN")
    df = dims.get('dim_lop_hoc_phan', pd.DataFrame())
    if not df.empty:
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        
        new_data = [(row['MaLopHP'], row['LopHP'], row['MaHP'], row['MaGV'], row['MaHocKy']) 
                    for _, row in df.iterrows() if row['MaLopHP'] not in existing]
        if new_data:
            inserted = fast_batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data)
            total_inserted['DIM_LOP_HOC_PHAN'] = inserted
            print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ✅ Không có dòng mới")
    
    return total_inserted


def insert_fact_tables_fast(cursor, fact_main, fact_ketqua):
    """Insert FACT tables siêu nhanh - chèn thêm mới, có NLP"""
    print("\n  📥 Insert FACT tables (chèn thêm mới)...")
    total_inserted = {}
    
    # TẮT CONSTRAINTS để insert nhanh hơn
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        cursor.connection.commit()
        print("  ✅ Đã tắt constraints để tăng tốc insert")
    except Exception as e:
        print(f"  ⚠️ Không thể tắt constraints: {e}")
    
    try:
        # 1. FACT_GOP_Y_TU_LUAN
        print("\n    -> FACT_GOP_Y_TU_LUAN")
        
        if fact_main is not None and not fact_main.empty:
            print(f"      -> Đang xử lý NLP cho {len(fact_main):,} bài...")
            nlp_start = time.time()
            
            # Xử lý NLP vectorized
            fact_main = process_nlp_batch_vectorized(fact_main)
            
            print(f"      -> NLP xong trong {time.time()-nlp_start:.2f}s")
            
            # Giới hạn độ dài nội dung
            fact_main['NoiDungGopY'] = fact_main['NoiDungGopY'].astype(str).str[:4000]
            
            # Chuẩn bị data để insert - dùng to_numpy() để nhanh hơn
            data = fact_main[['SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
                             'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                             'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']].to_numpy().tolist()
            
            if data:
                # Kiểm tra số dòng hiện có
                cursor.execute("SELECT COUNT(*) FROM FACT_GOP_Y_TU_LUAN")
                existing_count = cursor.fetchone()[0]
                print(f"      -> Hiện có {existing_count:,} dòng trong FACT_GOP_Y_TU_LUAN")
                
                inserted = fast_batch_insert(cursor, 'FACT_GOP_Y_TU_LUAN', 
                                           ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY',
                                            'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                            'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'], 
                                           data)
                total_inserted['FACT_GOP_Y_TU_LUAN'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ⚠️ Không có dữ liệu tự luận")
        
        # 2. FACT_KET_QUA_DANH_GIA
        print("\n    -> FACT_KET_QUA_DANH_GIA")
        
        if fact_ketqua is not None and not fact_ketqua.empty:
            # Chuẩn bị data để insert - dùng to_numpy()
            data = fact_ketqua[['SubmissionID', 'MaCauHoi', 'Diem']].to_numpy().tolist()
            
            if data:
                cursor.execute("SELECT COUNT(*) FROM FACT_KET_QUA_DANH_GIA")
                existing_count = cursor.fetchone()[0]
                print(f"      -> Hiện có {existing_count:,} dòng trong FACT_KET_QUA_DANH_GIA")
                
                inserted = fast_batch_insert(cursor, 'FACT_KET_QUA_DANH_GIA', 
                                           ['SubmissionID', 'MaCauHoi', 'Diem'], 
                                           data)
                total_inserted['FACT_KET_QUA_DANH_GIA'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới")
        else:
            print(f"      ⚠️ Không có dữ liệu trắc nghiệm")
        
        cursor.connection.commit()
        
    except Exception as e:
        print(f"  ❌ Lỗi insert fact: {e}")
        cursor.connection.rollback()
        raise
    finally:
        # BẬT LẠI CONSTRAINTS
        try:
            cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
            cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
            cursor.connection.commit()
            print("  ✅ Đã bật lại constraints")
        except Exception as e:
            print(f"  ⚠️ Không thể bật lại constraints: {e}")
    
    return total_inserted


def verify_inserted_data_fast(cursor):
    """Kiểm tra nhanh số lượng dữ liệu"""
    print("\n  📊 Kiểm tra dữ liệu sau insert:")
    
    tables = ['DIM_KHOA', 'DIM_NGANH', 'DIM_CHUYEN_NGANH', 'DIM_HOC_PHAN', 
              'DIM_GIANG_VIEN', 'DIM_HOC_KY', 'DIM_LOP_SINH_VIEN', 'DIM_SINH_VIEN',
              'DIM_LOP_HOC_PHAN', 'FACT_GOP_Y_TU_LUAN', 'FACT_KET_QUA_DANH_GIA']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"      - {table}: {count:,} dòng")
        except Exception as e:
            print(f"      - {table}: Lỗi ({e})")


# ================= MAIN INSERT JOB =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 JOB 2: CHÈN DỮ LIỆU VÀO DATABASE (TỐI ƯU TỐC ĐỘ)")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
    print(f"BATCH_SIZE: {BATCH_SIZE:,} dòng/batch")
    print("=" * 60)
    print("\n📌 LOGIC TỐI ƯU:")
    print("   - DIMENSION: INSERT bản ghi CHƯA TỒN TẠI")
    print("   - FACT: Chèn thêm mới + NLP vectorized")
    print("   - Batch size lớn + fast_executemany")
    print("   - Tắt constraints khi insert FACT")
    print("=" * 60)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    # 2. Tìm và tải dữ liệu đã tiền xử lý
    print("\n📥 2. Tìm kiếm dữ liệu...")
    
    dims = {}
    fact_main = pd.DataFrame()
    fact_ketqua = pd.DataFrame()
    
    # Thử tải từ preprocessed-data
    preprocessed_data = download_preprocessed_data(blob_service, f"{FILE_NAME}_preprocessed")
    
    if preprocessed_data:
        print("  ✅ Đã tải dữ liệu từ preprocessed-data")
        dims = {k: v for k, v in preprocessed_data.items() if k.startswith('dim_')}
        fact_main = preprocessed_data.get('fact_gop_y_tu_luan', pd.DataFrame())
        fact_ketqua = preprocessed_data.get('fact_ket_qua_danh_gia', pd.DataFrame())
    else:
        print("  ⚠️ Thử tải CSV backup...")
        fact_main = download_csv_backup(blob_service, f"{FILE_NAME}_main")
        fact_ketqua = download_csv_backup(blob_service, f"{FILE_NAME}_ketqua")
        
        if fact_main is None and fact_ketqua is None:
            print("  ❌ Không tìm thấy dữ liệu!")
            return
        
        print(f"  ✅ Đã tải CSV: main={len(fact_main) if fact_main is not None else 0}, ketqua={len(fact_ketqua) if fact_ketqua is not None else 0}")
    
    # 3. Kết nối Database với tối ưu
    print("\n💾 3. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        cursor = conn.cursor()
        cursor.fast_executemany = True  # BẮT BUỘC cho tốc độ cao
        print("  ✅ Kết nối SQL thành công (fast_executemany=ON)")
    except Exception as e:
        print(f"  ❌ Lỗi kết nối SQL: {e}")
        return
    
    # 4. Insert vào database
    db_start = time.time()
    
    try:
        # Insert DIMENSION tables
        if dims and any(len(df) > 0 for df in dims.values()):
            dim_inserted = insert_dimension_tables_fast(cursor, dims)
        else:
            print("\n  ⚠️ Bỏ qua DIMENSION tables")
            dim_inserted = {}
        
        # Insert FACT tables
        if not fact_main.empty or not fact_ketqua.empty:
            fact_inserted = insert_fact_tables_fast(cursor, fact_main, fact_ketqua)
        else:
            print("\n  ⚠️ Bỏ qua FACT tables")
            fact_inserted = {}
        
        # Kiểm tra dữ liệu
        verify_inserted_data_fast(cursor)
        cursor.connection.commit()
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        cursor.connection.rollback()
    finally:
        cursor.close()
        conn.close()
    
    db_time = time.time() - db_start
    
    # 5. Thống kê
    total_time = time.time() - total_start
    print("\n📊 5. KẾT QUẢ:")
    print(f"   Thời gian insert: {db_time:.2f}s")
    print(f"   Tổng thời gian: {total_time:.2f}s")
    
    total_rows = 0
    if dim_inserted:
        print("\n   📌 DIMENSION tables (mới):")
        for table, count in dim_inserted.items():
            if count > 0:
                print(f"      - {table}: {count:,} dòng")
                total_rows += count
    
    if fact_inserted:
        print("\n   📌 FACT tables (đã insert):")
        for table, count in fact_inserted.items():
            print(f"      - {table}: {count:,} dòng")
            total_rows += count
    
    if total_rows > 0:
        speed = total_rows / db_time if db_time > 0 else 0
        print(f"\n   🚀 Tốc độ insert: {speed:,.0f} dòng/giây")
    
    print("\n" + "=" * 60)
    print("✅ HOÀN THÀNH!")
    print("=" * 60)


if __name__ == "__main__":
    main()
