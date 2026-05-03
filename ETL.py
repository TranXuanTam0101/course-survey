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
)

CONTAINER_NAME = SEMESTER
PROCESSED_PATH = "processed-data"
PREPROCESSED_PATH = "preprocessed-data" 
# Batch size cho insert
BATCH_SIZE = 50000
MAX_WORKERS = 4

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


def download_csv_backup(blob_service, filename):
    """Tải CSV backup nếu không có pickle"""
    path = f"{PROCESSED_PATH}/{filename}"
    try:
        container_client = blob_service.get_container_client(CONTAINER_NAME)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            content = blob.download_blob().readall().decode('utf-8-sig')
            return pd.read_csv(io.StringIO(content))
        return None
    except Exception as e:
        print(f"  ❌ Lỗi tải CSV: {e}")
        return None


# ================= NLP FUNCTIONS TỐI ƯU =================
def analyze_sentiment_fast(text: str) -> str:
    """Phân tích sentiment nhanh không cần tokenize phức tạp"""
    if not isinstance(text, str) or len(text.strip()) < 3:
        return 'neutral'
    
    text_lower = text.lower()
    
    # Kiểm tra rất tích cực
    if _sentiment_patterns['very_positive'].search(text_lower):
        return 'positive'
    
    # Kiểm tra rất tiêu cực
    if _sentiment_patterns['very_negative'].search(text_lower):
        return 'negative'
    
    # Đếm số lượng từ tích cực và tiêu cực
    pos_count = len(_sentiment_patterns['positive'].findall(text_lower))
    neg_count = len(_sentiment_patterns['negative'].findall(text_lower))
    
    if pos_count > neg_count:
        return 'positive'
    elif neg_count > pos_count:
        return 'negative'
    else:
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


def process_nlp_batch(texts):
    """Xử lý NLP cho batch texts"""
    sentiments = [analyze_sentiment_fast(t) for t in texts]
    tags = [extract_tags_fast(t) for t in texts]
    return sentiments, tags


# ================= DATABASE FUNCTIONS =================
def batch_insert(cursor, table, columns, data, batch_size=BATCH_SIZE):
    """Batch insert dữ liệu vào database"""
    if data is None or len(data) == 0:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        try:
            cursor.executemany(sql, batch)
            total += len(batch)
            cursor.connection.commit()
            if total % 100000 == 0:
                print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
        except Exception as e:
            print(f"      ❌ Lỗi batch {i//batch_size + 1}: {e}")
            cursor.connection.rollback()
            continue
    
    return total


def insert_dimension_tables(cursor, dims):
    """
    Insert dữ liệu vào các bảng DIMENSION
    CHỈ INSERT NHỮNG BẢN GHI CHƯA TỒN TẠI
    """
    print("\n  📥 Insert DIMENSION tables (chỉ insert mới)...")
    
    total_inserted = {}
    
    # 1. DIM_KHOA
    print("\n    -> DIM_KHOA")
    df = dims['dim_khoa']
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaKhoa'], row['TenKhoa']) for _, row in df.iterrows() if row['MaKhoa'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data)
        total_inserted['DIM_KHOA'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 2. DIM_NGANH
    print("\n    -> DIM_NGANH")
    df = dims['dim_nganh']
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaNganh'], row['TenNganh'], row['MaKhoa']) 
                for _, row in df.iterrows() if row['MaNganh'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], new_data)
        total_inserted['DIM_NGANH'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 3. DIM_CHUYEN_NGANH
    print("\n    -> DIM_CHUYEN_NGANH")
    df = dims['dim_chuyen_nganh']
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaChuyenNganh'], row['TenChuyenNganh'], row['MaNganh']) 
                for _, row in df.iterrows() if row['MaChuyenNganh'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_CHUYEN_NGANH', ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], new_data)
        total_inserted['DIM_CHUYEN_NGANH'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 4. DIM_HOC_PHAN
    print("\n    -> DIM_HOC_PHAN")
    df = dims['dim_hoc_phan']
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) for _, row in df.iterrows() if row['MaHP'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data)
        total_inserted['DIM_HOC_PHAN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 5. DIM_GIANG_VIEN
    print("\n    -> DIM_GIANG_VIEN")
    df = dims['dim_giang_vien']
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) for _, row in df.iterrows() if row['MaGV'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data)
        total_inserted['DIM_GIANG_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 6. DIM_HOC_KY
    print("\n    -> DIM_HOC_KY")
    df = dims['dim_hoc_ky']
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaHocKy'], row['NamHoc'], row['HocKy']) for _, row in df.iterrows() if row['MaHocKy'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_HOC_KY', ['MaHocKy', 'NamHoc', 'HocKy'], new_data)
        total_inserted['DIM_HOC_KY'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 7. DIM_LOP_SINH_VIEN
    print("\n    -> DIM_LOP_SINH_VIEN")
    df = dims['dim_lop_sinh_vien']
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaLop'], row['Lop'], row['MaChuyenNganh']) for _, row in df.iterrows() if row['MaLop'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], new_data)
        total_inserted['DIM_LOP_SINH_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 8. DIM_SINH_VIEN
    print("\n    -> DIM_SINH_VIEN")
    df = dims['dim_sinh_vien']
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaSV'], row['HoDem'], row['Ten'], row['NgaySinh'], row['MaLop']) 
                for _, row in df.iterrows() if row['MaSV'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data)
        total_inserted['DIM_SINH_VIEN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    # 9. DIM_LOP_HOC_PHAN
    print("\n    -> DIM_LOP_HOC_PHAN")
    df = dims['dim_lop_hoc_phan']
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(row['MaLopHP'], row['LopHP'], row['MaHP'], row['MaGV'], row['MaHocKy']) 
                for _, row in df.iterrows() if row['MaLopHP'] not in existing]
    if new_data:
        inserted = batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data)
        total_inserted['DIM_LOP_HOC_PHAN'] = inserted
        print(f"      ✅ Đã insert {inserted} dòng mới")
    else:
        print(f"      ✅ Không có dòng mới (đã tồn tại {len(df)} dòng)")
    
    return total_inserted


def insert_fact_tables(cursor, fact_main, fact_ketqua):
    """
    Insert dữ liệu vào các bảng FACT
    CHÈN THÊM MỚI (KHÔNG KIỂM TRA TỒN TẠI)
    """
    print("\n  📥 Insert FACT tables (chèn thêm mới)...")
    
    total_inserted = {}
    
    # TẮT CONSTRAINTS để insert nhanh hơn
    try:
        cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
        cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
        cursor.connection.commit()
    except Exception as e:
        print(f"  ⚠️ Không thể tắt constraints: {e}")
    
    try:
        # 1. FACT_GOP_Y_TU_LUAN
        print("\n    -> FACT_GOP_Y_TU_LUAN")
        
        if not fact_main.empty:
            # Xử lý NLP cho tất cả nội dung
            texts = fact_main['NoiDungGopY'].fillna('').tolist()
            print(f"      -> Đang xử lý NLP cho {len(texts):,} bài...")
            
            sentiments, tags = process_nlp_batch(texts)
            
            # Chuẩn bị data để insert
            data = []
            for idx, row in fact_main.iterrows():
                noi_dung = row['NoiDungGopY']
                if isinstance(noi_dung, str) and len(noi_dung) > 4000:
                    noi_dung = noi_dung[:4000]
                
                data.append((
                    row['SubmissionID'], 
                    row['MaSV'], 
                    row['LopHP'], 
                    noi_dung,
                    sentiments[idx], 
                    1,  # Is_Valid
                    tags[idx][0],  # Tag_HocPhan
                    tags[idx][1],  # Tag_DayHoc
                    tags[idx][2],  # Tag_KiemTra
                    tags[idx][3]   # Tag_Khac
                ))
            
            if data:
                inserted = batch_insert(cursor, 'FACT_GOP_Y_TU_LUAN', 
                                       ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY',
                                        'Sentiment', 'Is_Valid', 'Tag_HocPhan', 
                                        'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'], 
                                       data)
                total_inserted['FACT_GOP_Y_TU_LUAN'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới (có NLP)")
            else:
                print(f"      ⚠️ Không có dữ liệu để insert")
        else:
            print(f"      ⚠️ Không có dữ liệu tự luận")
        
        # 2. FACT_KET_QUA_DANH_GIA
        print("\n    -> FACT_KET_QUA_DANH_GIA")
        
        if not fact_ketqua.empty:
            # Chuẩn bị data để insert
            data = [(row['SubmissionID'], row['MaCauHoi'], row['Diem']) 
                    for _, row in fact_ketqua.iterrows()]
            
            if data:
                inserted = batch_insert(cursor, 'FACT_KET_QUA_DANH_GIA', 
                                       ['SubmissionID', 'MaCauHoi', 'Diem'], 
                                       data)
                total_inserted['FACT_KET_QUA_DANH_GIA'] = inserted
                print(f"      ✅ Đã insert {inserted} dòng mới")
            else:
                print(f"      ⚠️ Không có dữ liệu để insert")
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
        except Exception as e:
            print(f"  ⚠️ Không thể bật lại constraints: {e}")
    
    return total_inserted


def verify_inserted_data(cursor):
    """Kiểm tra số lượng dữ liệu đã insert"""
    print("\n  📊 Kiểm tra dữ liệu sau insert:")
    
    tables = [
        ('DIM_KHOA', 'MaKhoa'),
        ('DIM_NGANH', 'MaNganh'),
        ('DIM_CHUYEN_NGANH', 'MaChuyenNganh'),
        ('DIM_HOC_PHAN', 'MaHP'),
        ('DIM_GIANG_VIEN', 'MaGV'),
        ('DIM_HOC_KY', 'MaHocKy'),
        ('DIM_LOP_SINH_VIEN', 'MaLop'),
        ('DIM_SINH_VIEN', 'MaSV'),
        ('DIM_LOP_HOC_PHAN', 'MaLopHP'),
        ('FACT_GOP_Y_TU_LUAN', 'SubmissionID'),
        ('FACT_KET_QUA_DANH_GIA', 'SubmissionID')
    ]
    
    for table, column in tables:
        try:
            cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"      - {table}: {count:,} dòng")
        except Exception as e:
            print(f"      - {table}: Lỗi ({e})")


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
    print("   - FACT tables: INSERT THÊM MỚI (có xử lý NLP)")
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
    print("\n📥 2. Tìm kiếm dữ liệu đã tiền xử lý...")
    
    # Thử tải từ preprocessed-data trước
    preprocessed_data = download_preprocessed_data(blob_service, f"{FILE_NAME}_preprocessed")
    
    if preprocessed_data:
        print("  ✅ Đã tải dữ liệu từ preprocessed-data")
        dims = {k: v for k, v in preprocessed_data.items() if k.startswith('dim_')}
        fact_main = preprocessed_data.get('fact_gop_y_tu_luan', pd.DataFrame())
        fact_ketqua = preprocessed_data.get('fact_ket_qua_danh_gia', pd.DataFrame())
        
        print(f"     - DIM_KHOA: {len(dims.get('dim_khoa', pd.DataFrame())):,} dòng")
        print(f"     - DIM_NGANH: {len(dims.get('dim_nganh', pd.DataFrame())):,} dòng")
        print(f"     - DIM_CHUYEN_NGANH: {len(dims.get('dim_chuyen_nganh', pd.DataFrame())):,} dòng")
        print(f"     - FACT_GOP_Y_TU_LUAN: {len(fact_main):,} dòng")
        print(f"     - FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        print("  ⚠️ Không tìm thấy preprocessed data, thử tìm CSV backup...")
        
        # Thử tìm CSV backup mới nhất
        import io
        timestamp = datetime.now().strftime('%Y%m%d')
        fact_main = download_csv_backup(blob_service, f"{FILE_NAME}_main_{timestamp}*.csv")
        fact_ketqua = download_csv_backup(blob_service, f"{FILE_NAME}_ketqua_{timestamp}*.csv")
        
        if fact_main is None and fact_ketqua is None:
            print("  ❌ Không tìm thấy dữ liệu đã tiền xử lý!")
            print("  💡 Hãy chạy JOB 1 trước!")
            return
        
        # Tạo dims tối thiểu từ fact data
        dims = {
            'dim_khoa': pd.DataFrame(),
            'dim_nganh': pd.DataFrame(),
            'dim_chuyen_nganh': pd.DataFrame(),
            'dim_hoc_phan': pd.DataFrame(),
            'dim_giang_vien': pd.DataFrame(),
            'dim_hoc_ky': pd.DataFrame(),
            'dim_lop_sinh_vien': pd.DataFrame(),
            'dim_sinh_vien': pd.DataFrame(),
            'dim_lop_hoc_phan': pd.DataFrame()
        }
        print("  ⚠️ Chỉ insert FACT tables (không có DIMENSION)")
    
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
        if dims and any(len(df) > 0 for df in dims.values()):
            dim_inserted = insert_dimension_tables(cursor, dims)
        else:
            print("\n  ⚠️ Bỏ qua DIMENSION tables (không có dữ liệu)")
            dim_inserted = {}
        
        # Insert FACT tables (CHÈN THÊM MỚI - CÓ NLP)
        if not fact_main.empty or not fact_ketqua.empty:
            fact_inserted = insert_fact_tables(cursor, fact_main, fact_ketqua)
        else:
            print("\n  ⚠️ Bỏ qua FACT tables (không có dữ liệu)")
            fact_inserted = {}
        
        # Kiểm tra dữ liệu sau insert
        verify_inserted_data(cursor)
        
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
    print("\n📊 5. KẾT QUẢ INSERT:")
    print(f"   Thời gian insert: {db_time:.1f}s")
    
    if dim_inserted:
        print("\n   📌 DIMENSION tables (CHỈ INSERT MỚI):")
        for table, count in dim_inserted.items():
            if count > 0:
                print(f"      - {table}: {count:,} dòng mới")
        if all(count == 0 for count in dim_inserted.values()):
            print(f"      - Không có dòng mới nào được insert")
    
    if fact_inserted:
        print("\n   📌 FACT tables (CHÈN THÊM MỚI - CÓ NLP):")
        for table, count in fact_inserted.items():
            print(f"      - {table}: {count:,} dòng đã insert")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH INSERT! Thời gian: {total_time:.1f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
