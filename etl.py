# etl.py - XỬ LÝ ĐÚNG 17 CỘT RAW
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re

print("🚀 Starting ETL Pipeline...")

# Lấy environment variables
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def clean_text(text):
    """Sửa lỗi encoding tiếng Việt"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    if '?' in text or '??' in text:
        text = ftfy.fix_text(text)
    return text.strip()

def convert_masv(value):
    """Chuyển 91122E+11 -> 9112200000000000"""
    if pd.isna(value) or value == '':
        return None
    try:
        return str(int(float(str(value))))
    except:
        return value

def parse_lop(value):
    """Xử lý Lop: '45K15.1\t1' -> '45K15.1'"""
    if pd.isna(value) or value == '':
        return None
    value_str = str(value).strip()
    if '\t' in value_str:
        return value_str.split('\t')[0]
    return value_str

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
    
    # ==================== 2. ĐỌC CSV ====================
    print("📊 Reading CSV file...")
    
    # Đọc file với separator là tab
    df_raw = pd.read_csv(
        io.BytesIO(data),
        sep='\t',
        header=None,
        dtype=str,
        low_memory=False,
        encoding='cp1258'
    )
    
    print(f"✅ Read {len(df_raw):,} rows, {len(df_raw.columns)} columns")
    
    # ==================== 3. TẠO DATAFRAME KẾT QUẢ ====================
    print("🔄 Building result DataFrame...")
    
    result = pd.DataFrame()
    
    # Cột 0: Lop
    result['Lop'] = df_raw[0].apply(parse_lop)
    
    # Cột 1: MaSV
    result['MaSV'] = df_raw[1].apply(convert_masv) if 1 in df_raw.columns else None
    
    # Cột 2: HoDem
    result['HoDem'] = df_raw[2].apply(clean_text) if 2 in df_raw.columns else None
    
    # Cột 3: Ten (có thể bị thiếu)
    if 3 in df_raw.columns:
        result['Ten'] = df_raw[3].apply(clean_text)
    else:
        result['Ten'] = None
    
    # Cột 4: NgaySinh
    if 4 in df_raw.columns:
        result['NgaySinh'] = pd.to_datetime(df_raw[4], errors='coerce', dayfirst=True)
    else:
        result['NgaySinh'] = None
    
    # Cột 5: MaHP
    result['MaHP'] = df_raw[5] if 5 in df_raw.columns else None
    
    # Cột 6: TenHP
    result['TenHP'] = df_raw[6].apply(clean_text) if 6 in df_raw.columns else None
    
    # Cột 7: MaGV
    result['MaGV'] = df_raw[7] if 7 in df_raw.columns else None
    
    # Cột 8: HoDemGV
    result['HoDemGV'] = df_raw[8].apply(clean_text) if 8 in df_raw.columns else None
    
    # Cột 9: TenGV
    result['TenGV'] = df_raw[9].apply(clean_text) if 9 in df_raw.columns else None
    
    # Cột 10: LopHP
    result['LopHP'] = df_raw[10] if 10 in df_raw.columns else None
    
    # Cột 11: CauHoi
    result['CauHoi'] = pd.to_numeric(df_raw[11], errors='coerce') if 11 in df_raw.columns else None
    
    # Cột 12: DanhGia
    result['DanhGia'] = pd.to_numeric(df_raw[12], errors='coerce') if 12 in df_raw.columns else None
    
    # Cột 13: NULL (bỏ qua)
    
    # Cột 14: FB1
    result['FB1'] = df_raw[14].apply(clean_text) if 14 in df_raw.columns else None
    
    # Cột 15: FB2
    result['FB2'] = df_raw[15].apply(clean_text) if 15 in df_raw.columns else None
    
    # Cột 16: FB3
    result['FB3'] = df_raw[16].apply(clean_text) if 16 in df_raw.columns else None
    
    # Cột 17: FB4
    result['FB4'] = df_raw[17].apply(clean_text) if 17 in df_raw.columns else None
    
    # ==================== 4. XỬ LÝ TRƯỜNG HỢP TEN BỊ THIẾU ====================
    # Nếu Ten bị thiếu, thử lấy từ HoDem (trường hợp HoDem chứa cả họ và tên)
    if result['Ten'].isna().any():
        print("🔄 Handling missing Ten values...")
        # Một số dòng có Ten bị thiếu, HoDem có dạng "Lê Bùi ?ông	Th?o"
        # Cần tách HoDem nếu có tab
        for idx in result[result['Ten'].isna()].index:
            hodem_val = result.loc[idx, 'HoDem']
            if hodem_val and '\t' in str(hodem_val):
                parts = str(hodem_val).split('\t')
                result.loc[idx, 'HoDem'] = clean_text(parts[0]) if len(parts) > 0 else None
                result.loc[idx, 'Ten'] = clean_text(parts[1]) if len(parts) > 1 else None
    
    # ==================== 5. LỌC DỮ LIỆU ====================
    print("🔄 Filtering valid records...")
    before = len(result)
    
    valid_mask = (
        result['MaSV'].notna() & 
        result['MaHP'].notna() & 
        result['CauHoi'].notna()
    )
    
    result = result[valid_mask]
    print(f"   Kept {len(result):,} / {before:,} rows ({len(result)/before*100:.1f}%)")
    
    if len(result) == 0:
        print("❌ ERROR: No valid records found!")
        print("\n📋 Debug - First row raw data:")
        if len(df_raw) > 0:
            for i in range(min(len(df_raw.columns), 18)):
                val = df_raw[i].iloc[0]
                print(f"   Col {i}: {val[:80] if val else 'EMPTY'}")
        sys.exit(1)
    
    # ==================== 6. THÊM METADATA ====================
    result['HocKy'] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    result['NamHoc'] = SEMESTER
    result['ProcessedDate'] = datetime.now()
    
    # ==================== 7. SẮP XẾP CỘT ====================
    final_columns = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
        'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
        'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate'
    ]
    
    result = result[[col for col in final_columns if col in result.columns]]
    result = result.sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # ==================== 8. UPLOAD KẾT QUẢ ====================
    print("📤 Uploading to Azure...")
    output = result.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
        print("   Created container: processed-data")
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 9. KẾT QUẢ ====================
    print(f"\n{'='*60}")
    print(f"✅ SUCCESS!")
    print(f"{'='*60}")
    print(f"📊 Total responses: {len(result):,}")
    print(f"👨‍🎓 Total students: {result['MaSV'].nunique():,}")
    print(f"📚 Total courses: {result['MaHP'].nunique():,}")
    print(f"⭐ Average rating: {result['DanhGia'].mean():.2f}/5")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample data (first 5 rows):")
    sample_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
    sample_cols = [c for c in sample_cols if c in result.columns]
    print(result[sample_cols].head(5).to_string(index=False))
    
    print(f"\n📊 Rating by question:")
    if 'CauHoi' in result.columns and 'DanhGia' in result.columns:
        question_stats = result.groupby('CauHoi')['DanhGia'].agg(['mean', 'count'])
        for q in sorted(question_stats.index):
            if q <= 12:
                print(f"   Q{int(q):2d}: {question_stats.loc[q, 'mean']:.2f}/5 ({question_stats.loc[q, 'count']:,})")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
