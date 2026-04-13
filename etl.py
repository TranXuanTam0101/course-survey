import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import numpy as np
from datetime import datetime
import ftfy

print("🚀 Starting High-Speed ETL Pipeline...")

# ==================== 1. CONFIGURATION ====================
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
    # ==================== 2. DOWNLOAD & READ ====================
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    raw_bytes = blob_client.download_blob().readall()
    
    try:
        df_raw = pd.read_csv(io.BytesIO(raw_bytes), sep='\t', header=None, dtype=str, encoding='utf-8-sig', low_memory=False)
    except UnicodeDecodeError:
        df_raw = pd.read_csv(io.BytesIO(raw_bytes), sep='\t', header=None, dtype=str, encoding='cp1258', low_memory=False)

    print(f"✅ Loaded {len(df_raw):,} raw rows.")

    # ==================== 3. XỬ LÝ TÁCH CỘT (VECTORIZED) ====================
    # Tìm mốc cột Ngày Sinh
    is_date_col = df_raw.apply(lambda s: s.str.contains(r'\d{1,2}/\d{1,2}/', na=False)).any()
    date_idx = is_date_col[is_date_col == True].index[0]
    
    df = pd.DataFrame()
    df['Lop'] = fast_clean(df_raw[0])
    df['MaSV'] = fast_clean(df_raw[1])
    
    # Gộp Họ tên
    fullname = df_raw.iloc[:, 2:date_idx].fillna('').agg(' '.join, axis=1).str.strip()
    df['Ten'] = fullname.str.split().str[-1]
    df['HoDem'] = fullname.str.split().str[:-1].str.join(' ')
    
    df['NgaySinh'] = fast_clean(df_raw[date_idx])
    df['MaHP'] = fast_clean(df_raw[date_idx + 1])
    
    # Tìm cột Mã GV
    search_gv = df_raw.iloc[:, date_idx + 2 : date_idx + 10]
    is_magv_col = search_gv.apply(lambda s: s.str.isdigit(), axis=0).any()
    magv_idx = is_magv_col[is_magv_col == True].index[0]
    
    df['MaGV'] = fast_clean(df_raw[magv_idx])
    df['TenHP'] = df_raw.iloc[:, date_idx + 2 : magv_idx].fillna('').agg(' '.join, axis=1).str.strip()
    df['HoDemGV'] = fast_clean(df_raw[magv_idx + 1])
    df['TenGV'] = fast_clean(df_raw[magv_idx + 2])
    df['LopHP'] = fast_clean(df_raw[magv_idx + 3])
    df['CauHoi'] = fast_clean(df_raw[magv_idx + 4])
    df['DanhGia'] = fast_clean(df_raw[magv_idx + 5])

    # ==================== 4. TẠO ID VÀ PIVOT ====================
    # Khóa ID theo yêu cầu: Lop|MaSV|HoDem|Ten|NgaySinh
    df['ID'] = (
        df['Lop'].fillna('') + '|' + 
        df['MaSV'].fillna('') + '|' + 
        df['HoDem'].fillna('') + '|' + 
        df['Ten'].fillna('') + '|' + 
        df['NgaySinh'].astype(str).fillna('')
    )

    pivot_df = df.pivot_table(
        index=['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()

    # Đổi tên cột Q1..Q12
    pivot_df.columns = [f'Q{int(float(c))}' if str(c).replace('.0','').isdigit() else c for c in pivot_df.columns]
    pivot_df['NamHoc'] = SEMESTER
    pivot_df['HocKy'] = 2 if "252" in SURVEY_FILE else 1

    # ==================== 5. PRINT KẾT QUẢ ĐỂ KIỂM TRA ====================
    print("\n--- 📋 DỮ LIỆU MẪU SAU KHI XỬ LÝ (5 DÒNG ĐẦU) ---")
    # Chọn một số cột tiêu biểu để in ra xem thử
    preview_cols = ['ID', 'MaSV', 'Ten', 'MaHP', 'MaGV', 'Q1', 'Q2', 'Q3']
    # Chỉ in những cột thực sự tồn tại
    existing_preview = [c for c in preview_cols if c in pivot_df.columns]
    print(pivot_df[existing_preview].head(5).to_string(index=False))
    print(f"\n✅ Tổng số bản ghi (sinh viên - môn học): {len(pivot_df):,}")
    print("--------------------------------------------------\n")

    # ==================== 6. SAVE & UPLOAD ====================
    TEMP_FILE = "processed_data_temp.csv"
    pivot_df.to_csv(TEMP_FILE, index=False, encoding='utf-8-sig')
    
    # Upload lên Azure (vào container processed-data)
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists(): processed_container.create_container()
    
    with open(TEMP_FILE, "rb") as f:
        processed_container.upload_blob(name=output_path, data=f, overwrite=True)

    print(f"✅ SUCCESS: File saved as {TEMP_FILE}")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
