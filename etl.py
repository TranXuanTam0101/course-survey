import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import numpy as np
from datetime import datetime
import ftfy

print("🚀 Starting High-Speed ETL Pipeline...")

# ==================== 1. CẤU HÌNH MÔI TRƯỜNG ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing environment variables!")
    sys.exit(1)

def fast_clean(series):
    """Làm sạch dữ liệu nhanh trên toàn bộ cột"""
    return series.astype(str).str.strip().replace(['nan', 'NULL', 'None', ''], np.nan)

try:
    # ==================== 2. TẢI VÀ ĐỌC DỮ LIỆU ====================
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    raw_bytes = blob_client.download_blob().readall()
    
    # Thử đọc UTF-8 (65001), nếu lỗi chuyển sang CP1258 (Vietnamese Windows)
    # dtype=str và low_memory=False mô phỏng thao tác "Do not detect data types"
    try:
        df_raw = pd.read_csv(io.BytesIO(raw_bytes), sep='\t', header=None, dtype=str, encoding='utf-8-sig', low_memory=False)
    except UnicodeDecodeError:
        df_raw = pd.read_csv(io.BytesIO(raw_bytes), sep='\t', header=None, dtype=str, encoding='cp1258', low_memory=False)

    print(f"✅ Loaded {len(df_raw):,} rows.")

    # ==================== 3. XỬ LÝ TÁCH CỘT (VECTORIZED) ====================
    # Tìm mốc cột Ngày Sinh (chứa ký tự '/')
    is_date_col = df_raw.apply(lambda s: s.str.contains(r'\d{1,2}/\d{1,2}/', na=False)).any()
    date_idx = is_date_col[is_date_col == True].index[0]
    
    # Tạo DataFrame kết quả
    df = pd.DataFrame()
    df['Lop'] = fast_clean(df_raw[0])
    df['MaSV'] = fast_clean(df_raw[1])
    
    # Gộp Họ tên (xử lý nhảy cột giữa MaSV và NgaySinh)
    fullname = df_raw.iloc[:, 2:date_idx].fillna('').agg(' '.join, axis=1).str.strip()
    df['Ten'] = fullname.str.split().str[-1]
    df['HoDem'] = fullname.str.split().str[:-1].str.join(' ')
    
    df['NgaySinh'] = fast_clean(df_raw[date_idx])
    df['MaHP'] = fast_clean(df_raw[date_idx + 1])
    
    # Tìm cột Mã GV (Cột chứa số sau MaHP)
    search_gv = df_raw.iloc[:, date_idx + 2 : date_idx + 10]
    is_magv_col = search_gv.apply(lambda s: s.str.isdigit(), axis=0).any()
    magv_idx = is_magv_col[is_magv_col == True].index[0]
    
    df['MaGV'] = fast_clean(df_raw[magv_idx])
    df['TenHP'] = df_raw.iloc[:, date_idx + 2 : magv_idx].fillna('').agg(' '.join, axis=1).str.strip()
    
    df['HoDemGV'] = fast_clean(df_raw[magv_idx + 1])
    df['TenGV'] = fast_clean(df_raw[magv_idx + 2])
    df['LopHP'] = fast_clean(df_raw[magv_idx + 3])
    
    # Câu hỏi và Đánh giá (Dữ liệu dọc Q1-Q12)
    df['CauHoi'] = fast_clean(df_raw[magv_idx + 4])
    df['DanhGia'] = fast_clean(df_raw[magv_idx + 5])

    # ==================== 4. TẠO ID VÀ PIVOT ====================
    # Tạo khóa ID duy nhất theo yêu cầu
    df['ID'] = (
        df['Lop'].fillna('') + '|' + 
        df['MaSV'].fillna('') + '|' + 
        df['HoDem'].fillna('') + '|' + 
        df['Ten'].fillna('') + '|' + 
        df['NgaySinh'].astype(str).fillna('')
    )

    # Pivot để chuyển 12 dòng câu hỏi thành 1 dòng ngang
    pivot_df = df.pivot_table(
        index=['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()

    # Đổi tên các cột thành Q1, Q2, ... Q12
    pivot_df.columns = [f'Q{int(float(c))}' if str(c).replace('.0','').isdigit() else c for c in pivot_df.columns]

    # Thêm Metadata
    pivot_df['NamHoc'] = SEMESTER
    pivot_df['HocKy'] = 2 if "252" in SURVEY_FILE else 1

    # ==================== 5. XUẤT FILE & UPLOAD ====================
    # Tên file phải KHỚP với file mà load_to_sql.py đang tìm
    TEMP_FILE = "processed_data_temp.csv"
    pivot_df.to_csv(TEMP_FILE, index=False, encoding='utf-8-sig')
    
    # Upload dự phòng lên Azure
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists(): processed_container.create_container()
    
    with open(TEMP_FILE, "rb") as f:
        processed_container.upload_blob(name=output_path, data=f, overwrite=True)

    print(f"✅ SUCCESS: Exported {len(pivot_df):,} student records.")
    print(f"💾 Local file ready: {TEMP_FILE}")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
