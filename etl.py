import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import numpy as np
from datetime import datetime
import ftfy

print("🚀 Starting Refined ETL Pipeline...")

# ==================== 1. CẤU HÌNH ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing environment variables!")
    sys.exit(1)

def clean_text(text):
    if pd.isna(text) or str(text).strip().lower() in ['null', 'nan', '', 'none']:
        return None
    return ftfy.fix_text(str(text)).strip()

try:
    # ==================== 2. TẢI DỮ LIỆU ====================
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    data = blob_client.download_blob().readall()
    
    # Thử đọc UTF-8 (65001), nếu lỗi dùng CP1258
    try:
        df_raw = pd.read_csv(io.BytesIO(data), sep='\t', header=None, dtype=str, encoding='utf-8-sig', low_memory=False)
    except:
        df_raw = pd.read_csv(io.BytesIO(data), sep='\t', header=None, dtype=str, encoding='cp1258', low_memory=False)

    print(f"✅ Loaded {len(df_raw):,} raw lines.")

    # ==================== 3. DÒ TÌM MỐC CỘT (ANCHORING) ====================
    # Tìm cột Ngày Sinh (dd/mm/yyyy)
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    date_mask = df_raw.apply(lambda s: s.str.contains(date_pattern, na=False)).any()
    date_idx = date_mask[date_mask == True].index[0]
    
    # Tìm cột Mã GV (Cột số sau MaHP)
    search_gv = df_raw.iloc[:, date_idx + 2 : date_idx + 10]
    magv_mask = search_gv.apply(lambda s: s.str.isdigit(), axis=0).any()
    magv_idx = magv_mask[magv_mask == True].index[0]

    # ==================== 4. TRÍCH XUẤT THÔNG TIN ====================
    df_processed = pd.DataFrame()
    
    # Thông tin Sinh viên
    df_processed['Lop'] = df_raw[0].apply(clean_text)
    df_processed['MaSV'] = df_raw[1].apply(clean_text)
    
    hoten_sv = df_raw.iloc[:, 2:date_idx].fillna('').agg(' '.join, axis=1).str.split()
    df_processed['Ten'] = hoten_sv.str[-1].apply(clean_text)
    df_processed['HoDem'] = hoten_sv.str[:-1].str.join(' ').apply(clean_text)
    df_processed['NgaySinh'] = df_raw[date_idx].apply(clean_text)
    
    # Thông tin Học phần & Giảng viên
    df_processed['MaHP'] = df_raw[date_idx + 1].apply(clean_text)
    df_processed['TenHP'] = df_raw.iloc[:, date_idx + 2 : magv_idx].fillna('').agg(' '.join, axis=1).apply(clean_text)
    
    df_processed['MaGV'] = df_raw[magv_idx].apply(clean_text)
    df_processed['HoDemGV'] = df_raw[magv_idx + 1].apply(clean_text)
    df_processed['TenGV'] = df_raw[magv_idx + 2].apply(clean_text)
    df_processed['LopHP'] = df_raw[magv_idx + 3].apply(clean_text)
    
    # Câu hỏi & Đánh giá (Q1-Q12 nằm ở cột dọc)
    df_processed['CauHoi'] = pd.to_numeric(df_raw[magv_idx + 4], errors='coerce')
    df_processed['DanhGia'] = df_raw[magv_idx + 5].apply(clean_text)

    # Feedback (Q13-Q16 nằm ở các cột ngang sau cột Đánh giá)
    for i, q_num in enumerate(range(13, 17)):
        fb_idx = magv_idx + 6 + i
        if fb_idx < len(df_raw.columns):
            df_processed[f'Q{q_num}'] = df_raw[fb_idx].apply(clean_text)

    # ==================== 5. TẠO ID & PIVOT DỮ LIỆU ====================
    # Tạo ID duy nhất cho bảng SINH_VIEN
    df_processed['ID'] = (
        df_processed['Lop'].fillna('') + '|' + 
        df_processed['MaSV'].fillna('') + '|' + 
        df_processed['HoDem'].fillna('') + '|' + 
        df_processed['Ten'].fillna('') + '|' + 
        df_processed['NgaySinh'].fillna('')
    )

    # Các cột giữ cố định khi Pivot
    index_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
                  'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
                  'Q13', 'Q14', 'Q15', 'Q16']
    
    # Thực hiện Pivot để đưa Q1-Q12 từ dòng thành cột
    df_pivot = df_processed.pivot_table(
        index=[c for c in index_cols if c in df_processed.columns],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()

    # Đổi tên cột 1.0, 2.0 -> Q1, Q2...
    df_pivot.columns = [f'Q{int(float(c))}' if str(c).replace('.0','').isdigit() else c for c in df_pivot.columns]

    # Bổ sung Metadata
    df_pivot['NamHoc'] = SEMESTER
    df_pivot['HocKy'] = 2 if "252" in SURVEY_FILE else 1

    # ==================== 6. XUẤT KẾT QUẢ ====================
    TEMP_FILE = "processed_data_temp.csv"
    df_pivot.to_csv(TEMP_FILE, index=False, encoding='utf-8-sig')
    
    print("\n--- 📋 Sample Data Preview ---")
    print(df_pivot[['ID', 'MaSV', 'MaHP', 'Q1', 'Q13']].head(3).to_string(index=False))
    print(f"\n✅ Total records: {len(df_pivot):,}")
    
    # Upload lên Azure (Tùy chọn lưu trữ lại)
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists(): processed_container.create_container()
    
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_final.csv')}"
    with open(TEMP_FILE, "rb") as f:
        processed_container.upload_blob(name=output_path, data=f, overwrite=True)

    print(f"🚀 ETL Process Finished. File {TEMP_FILE} is ready for SQL Load.")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
