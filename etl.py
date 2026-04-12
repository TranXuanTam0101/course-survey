# etl.py - TỐI ƯU TỐC ĐỘ CAO
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re
import numpy as np

print("🚀 Starting ETL Pipeline...")

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def clean_text_vectorized(series):
    """Clean text nhanh bằng vectorized operations"""
    if series is None or len(series) == 0:
        return series
    series = series.fillna('').astype(str)
    mask = (series != '') & (series != 'NULL')
    if mask.any():
        unique_vals = series[mask].unique()
        clean_map = {v: ftfy.fix_text(v).strip() for v in unique_vals}
        return series.map(lambda x: clean_map.get(x, x) if x != '' and x != 'NULL' else None)
    return series.replace('', None).replace('NULL', None)

def convert_masv_vectorized(series):
    """Chuyển đổi MaSV từ dạng E+ sang số - vectorized"""
    def convert_one(x):
        if pd.isna(x) or x == '':
            return None
        try:
            if 'E' in str(x).upper():
                return str(int(float(x)))
            return str(int(float(x)))
        except:
            return x
    return series.apply(convert_one)

def parse_lop_vectorized(series):
    """Lấy phần trước dấu cách hoặc tab"""
    def parse_one(x):
        if pd.isna(x):
            return None
        x_str = str(x).strip()
        if '\t' in x_str:
            return x_str.split('\t')[0]
        if ' ' in x_str:
            return x_str.split(' ')[0]
        return x_str
    return series.apply(parse_one)

try:
    # ==================== 1. ĐỌC FILE ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    raw_container = blob_service.get_container_client("rawdata")
    
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Reading: {blob_path}")
    
    blob_client = raw_container.get_blob_client(blob_path)
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. GIẢI MÃ ====================
    print("📊 Decoding file...")
    try:
        text_content = data.decode('cp1258')
    except:
        try:
            text_content = data.decode('utf-8')
        except:
            text_content = data.decode('latin1')
    
    # Fix encoding toàn bộ 1 lần
    text_content = ftfy.fix_text(text_content)
    
    # ==================== 3. ĐỌC CSV (CHỈ 2 CỘT) ====================
    print("📊 Reading CSV...")
    # Đọc chỉ 2 cột A và B
    df = pd.read_csv(
        io.StringIO(text_content),
        sep='\t',
        header=None,
        dtype=str,
        low_memory=False,
        usecols=[0, 1]  # Chỉ đọc 2 cột đầu
    )
    
    print(f"✅ Read {len(df):,} rows")
    
    # Đặt tên cột
    df.columns = ['Lop_raw', 'StudentData_raw']
    
    # ==================== 4. XỬ LÝ CỘT A (LOP) ====================
    print("🔄 Processing Lop...")
    df['Lop'] = parse_lop_vectorized(df['Lop_raw'])
    
    # ==================== 5. XỬ LÝ CỘT B (TÁCH CÁC TRƯỜNG) ====================
    print("🔄 Splitting column B...")
    
    # Tách cột B thành các phần bằng dấu cách
    # Dùng expand=True để tạo nhiều cột cùng lúc (rất nhanh)
    split_cols = df['StudentData_raw'].str.split(expand=True)
    
    # ===== MAP CÁC CỘT THEO ĐÚNG VỊ TRÍ =====
    # Dựa trên cấu trúc file raw:
    # 0: MaSV, 1: HoDem, 2: Ten, 3: NgaySinh, 
    # 4: MaHP? (có thể bỏ qua), 5: MaHP, 6: TenHP, 7: MaGV, 
    # 8: HoDemGV, 9: TenGV, 10: LopHP, 11: CauHoi, 12: DanhGia, 
    # 13: NULL, 14: FB1, 15: FB2, 16: FB3, 17: FB4
    
    # Khởi tạo dataframe kết quả
    result_df = pd.DataFrame(index=df.index)
    
    # MaSV (vị trí 0)
    if split_cols.shape[1] > 0:
        result_df['MaSV'] = convert_masv_vectorized(split_cols[0])
    
    # HoDem (vị trí 1)
    if split_cols.shape[1] > 1:
        result_df['HoDem'] = split_cols[1]
    
    # Ten (vị trí 2)
    if split_cols.shape[1] > 2:
        result_df['Ten'] = split_cols[2]
    
    # NgaySinh (vị trí 3)
    if split_cols.shape[1] > 3:
        result_df['NgaySinh_raw'] = split_cols[3]
        result_df['NgaySinh'] = pd.to_datetime(split_cols[3], errors='coerce', dayfirst=True)
    
    # MaHP (vị trí 5)
    if split_cols.shape[1] > 5:
        result_df['MaHP'] = split_cols[5]
    
    # TenHP (vị trí 6)
    if split_cols.shape[1] > 6:
        result_df['TenHP'] = split_cols[6]
    
    # MaGV (vị trí 7)
    if split_cols.shape[1] > 7:
        result_df['MaGV'] = split_cols[7]
    
    # HoDemGV (vị trí 8)
    if split_cols.shape[1] > 8:
        result_df['HoDemGV'] = split_cols[8]
    
    # TenGV (vị trí 9)
    if split_cols.shape[1] > 9:
        result_df['TenGV'] = split_cols[9]
    
    # LopHP (vị trí 10)
    if split_cols.shape[1] > 10:
        result_df['LopHP'] = split_cols[10]
    
    # CauHoi (vị trí 11)
    if split_cols.shape[1] > 11:
        result_df['CauHoi'] = pd.to_numeric(split_cols[11], errors='coerce')
    
    # DanhGia (vị trí 12)
    if split_cols.shape[1] > 12:
        result_df['DanhGia'] = pd.to_numeric(split_cols[12], errors='coerce')
    
    # Bỏ qua vị trí 13 (NULL)
    
    # FB1 (vị trí 14)
    if split_cols.shape[1] > 14:
        result_df['FB1'] = split_cols[14]
    
    # FB2 (vị trí 15)
    if split_cols.shape[1] > 15:
        result_df['FB2'] = split_cols[15]
    
    # FB3 (vị trí 16)
    if split_cols.shape[1] > 16:
        result_df['FB3'] = split_cols[16]
    
    # FB4 (vị trí 17)
    if split_cols.shape[1] > 17:
        result_df['FB4'] = split_cols[17]
    
    # ==================== 6. CLEAN TEXT (CHỈ CÁC CỘT CẦN) ====================
    print("🔄 Cleaning Vietnamese text...")
    
    text_columns = ['HoDem', 'Ten', 'TenHP', 'HoDemGV', 'TenGV', 'FB1', 'FB2', 'FB3', 'FB4']
    for col in text_columns:
        if col in result_df.columns:
            result_df[col] = clean_text_vectorized(result_df[col])
    
    # ==================== 7. LỌC DỮ LIỆU ====================
    print("🔄 Filtering valid records...")
    before = len(result_df)
    
    valid_mask = (
        result_df['MaSV'].notna() & 
        result_df['MaHP'].notna() & 
        result_df['CauHoi'].notna()
    )
    
    result_df = result_df[valid_mask]
    print(f"   Kept {len(result_df):,} / {before:,} rows ({len(result_df)/before*100:.1f}%)")
    
    if len(result_df) == 0:
        print("❌ ERROR: No valid records found!")
        sys.exit(1)
    
    # ==================== 8. THÊM METADATA ====================
    result_df['Lop'] = df.loc[result_df.index, 'Lop']
    result_df['HocKy'] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    result_df['NamHoc'] = SEMESTER
    result_df['ProcessedDate'] = datetime.now()
    
    # ==================== 9. SẮP XẾP CỘT ====================
    final_columns = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
        'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
        'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate'
    ]
    
    existing_columns = [col for col in final_columns if col in result_df.columns]
    result_df = result_df[existing_columns]
    
    # Sắp xếp
    result_df = result_df.sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # ==================== 10. UPLOAD ====================
    print("📤 Uploading to Azure...")
    output = result_df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 11. KẾT QUẢ ====================
    print(f"\n{'='*60}")
    print(f"✅ SUCCESS!")
    print(f"{'='*60}")
    print(f"📊 Total responses: {len(result_df):,}")
    print(f"👨‍🎓 Total students: {result_df['MaSV'].nunique():,}")
    print(f"📚 Total courses: {result_df['MaHP'].nunique():,}")
    print(f"⭐ Average rating: {result_df['DanhGia'].mean():.2f}/5")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample data (first 5 rows):")
    sample_cols = ['Lop', 'MaSV', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
    sample_cols = [c for c in sample_cols if c in result_df.columns]
    print(result_df[sample_cols].head(5).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
