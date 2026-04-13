import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import numpy as np
from datetime import datetime
import ftfy

print("🚀 Starting Optimized ETL Pipeline...")

# ==================== CONFIGURATION ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing environment variables!")
    sys.exit(1)

def fast_clean(s):
    """Làm sạch dữ liệu nhanh cho Series"""
    return s.astype(str).str.strip().replace(['nan', 'NULL', 'None', ''], np.nan)

try:
    # 1. KẾT NỐI & TẢI DATA
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container_name = "rawdata"
    blob_client = blob_service.get_container_client(container_name).get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    
    # 2. ĐỌC DỮ LIỆU (Xử lý lỗi encoding tự động)
    try:
        df_raw = pd.read_csv(io.BytesIO(data), sep='\t', header=None, dtype=str, encoding='utf-8-sig')
    except:
        df_raw = pd.read_csv(io.BytesIO(data), sep='\t', header=None, dtype=str, encoding='cp1258')

    print(f"✅ Loaded {len(df_raw):,} rows.")

    # 3. XỬ LÝ VECTORIZED (THAY THẾ VÒNG LẶP FOR)
    # Xác định cột Ngày Sinh tự động trên toàn bộ cột (lấy cột đầu tiên khớp pattern)
    mask_date = df_raw.apply(lambda s: s.str.contains(r'\d{1,2}/\d{1,2}/', na=False)).any()
    date_col_idx = mask_date[mask_date == True].index[0]
    
    df = pd.DataFrame()
    df['Lop'] = fast_clean(df_raw[0])
    df['MaSV'] = fast_clean(df_raw[1])
    
    # Gộp Họ tên (giữa MaSV và NgaySinh)
    df['Fullname'] = df_raw.iloc[:, 2:date_col_idx].fillna('').agg(' '.join, axis=1).str.strip()
    df['Ten'] = df['Fullname'].str.split().str[-1]
    df['HoDem'] = df['Fullname'].str.split().str[:-1].str.join(' ')
    
    df['NgaySinh'] = fast_clean(df_raw[date_col_idx])
    df['MaHP'] = fast_clean(df_raw[date_col_idx + 1])
    
    # Xác định cột Mã GV (Tìm cột có số từ sau MaHP)
    # Giả sử cấu trúc khá ổn định, ta lấy cột có nhiều chữ số nhất sau MaHP
    search_range = df_raw.iloc[:, date_col_idx + 2 : date_col_idx + 8]
    magv_mask = search_range.apply(lambda s: s.str.isdigit() if s.name != date_col_idx+1 else False).any()
    magv_idx = magv_mask[magv_mask == True].index[0]
    
    df['MaGV'] = fast_clean(df_raw[magv_idx])
    df['TenHP'] = df_raw.iloc[:, date_col_idx + 2 : magv_idx].fillna('').agg(' '.join, axis=1).str.strip()
    
    df['HoDemGV'] = fast_clean(df_raw[magv_idx + 1])
    df['TenGV'] = fast_clean(df_raw[magv_idx + 2])
    df['LopHP'] = fast_clean(df_raw[magv_idx + 3])
    
    df['CauHoi'] = fast_clean(df_raw[magv_idx + 4])
    df['DanhGia'] = fast_clean(df_raw[magv_idx + 5])

    # 4. PIVOT NHANH
    # Tạo khóa định danh duy nhất cho mỗi SV-Môn học-GV
    df['GroupKey'] = df['MaSV'].fillna('') + "_" + df['MaHP'].fillna('') + "_" + df['MaGV'].fillna('')
    
    pivot_df = df.pivot_table(
        index=['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()

    # Đổi tên các cột Q1-Q12
    pivot_df.columns = [f'Q{int(float(c))}' if str(c).replace('.0','').isdigit() else c for c in pivot_df.columns]

    # 5. LƯU FILE (Sửa lỗi tên file cho bước load SQL)
    # Tên file phải khớp với load_to_sql.py đang chờ
    TEMP_FILE = "processed_data_temp.csv" 
    pivot_df.to_csv(TEMP_FILE, index=False, encoding='utf-8-sig')
    
    # Upload lên Azure (để lưu trữ lâu dài)
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_final.csv')}"
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists(): processed_container.create_container()
    
    with open(TEMP_FILE, "rb") as f:
        processed_container.upload_blob(name=output_path, data=f, overwrite=True)

    print(f"✅ SUCCESS: Exported {len(pivot_df):,} records to {TEMP_FILE}")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
