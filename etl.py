# etl.py - XỬ LÝ NHANH FILE 2 CỘT (ĐÃ SỬA ENCODING)
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
    """Sửa lỗi encoding tiếng Việt nhanh"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    # Chỉ áp dụng ftfy nếu có dấu hiệu lỗi
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
    """Lấy phần đầu của Lop: '45K05\t1' -> '45K05'"""
    if pd.isna(value) or value == '':
        return None
    value_str = str(value).strip()
    # Tách theo tab hoặc space
    if '\t' in value_str:
        return value_str.split('\t')[0]
    if ' ' in value_str:
        return value_str.split(' ')[0]
    return value_str

try:
    # ==================== 1. ĐỌC FILE NHANH ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    raw_container = blob_service.get_container_client("rawdata")
    
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Reading: {blob_path}")
    
    blob_client = raw_container.get_blob_client(blob_path)
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. ĐỌC CSV (CHỈ 2 CỘT) - FIX ENCODING ====================
    print("📊 Reading CSV (2 columns)...")
    
    # 👇👇👇 SỬA Ở ĐÂY: THÊM encoding='cp1258' 👇👇👇
    df_raw = pd.read_csv(
        io.BytesIO(data),
        sep='\t',
        header=None,
        nrows=None,
        dtype=str,
        low_memory=False,
        encoding='cp1258'  # <-- THÊM DÒNG NÀY
    )
    
    print(f"✅ Read {len(df_raw):,} rows, {len(df_raw.columns)} columns")
    
    # Kiểm tra số cột
    if len(df_raw.columns) < 2:
        print(f"❌ Error: Expected 2 columns, found {len(df_raw.columns)}")
        sys.exit(1)
    
    # ==================== 3. TÁCH CỘT B (CỘT THỨ 2) ====================
    print("🔄 Splitting column B into multiple columns...")
    
    # Cột B là cột thứ 2 (index 1)
    # Tách các giá trị trong cột B bằng tab
    col_b_split = df_raw[1].str.split('\t', expand=True)
    
    print(f"   Split into {len(col_b_split.columns)} columns")
    
    # ==================== 4. TẠO DATAFRAME KẾT QUẢ ====================
    print("🔄 Building result DataFrame...")
    
    result = pd.DataFrame()
    
    # Cột A: Lop (chỉ lấy phần đầu)
    result['Lop'] = df_raw[0].apply(parse_lop)
    
    # Xử lý cột 0 (thông tin sinh viên) - tách tiếp
    if 0 in col_b_split.columns:
        student_info = col_b_split[0].str.split(' ', expand=True)
        if len(student_info.columns) >= 4:
            result['MaSV'] = student_info[0].apply(convert_masv)
            result['HoDem'] = student_info[1].apply(clean_text)
            result['Ten'] = student_info[2].apply(clean_text)
            result['NgaySinh'] = pd.to_datetime(student_info[3], errors='coerce', dayfirst=True)
        elif len(student_info.columns) >= 3:
            result['MaSV'] = student_info[0].apply(convert_masv)
            result['Ten'] = student_info[1].apply(clean_text)
            result['HoDem'] = None
            result['NgaySinh'] = pd.to_datetime(student_info[2], errors='coerce', dayfirst=True)
        else:
            result['MaSV'] = col_b_split[0].apply(convert_masv)
            result['HoDem'] = None
            result['Ten'] = None
            result['NgaySinh'] = None
    else:
        result['MaSV'] = None
        result['HoDem'] = None
        result['Ten'] = None
        result['NgaySinh'] = None
    
    # Các cột còn lại
    col_mapping = {
        4: 'MaHP',
        5: 'TenHP',
        6: 'MaGV',
        7: 'HoDemGV',
        8: 'TenGV',
        9: 'LopHP',
        10: 'CauHoi',
        11: 'DanhGia',
        13: 'FB1',
        14: 'FB2',
        15: 'FB3',
        16: 'FB4'
    }
    
    for idx, col_name in col_mapping.items():
        if idx in col_b_split.columns:
            if col_name in ['TenHP', 'HoDemGV', 'TenGV', 'FB1', 'FB2', 'FB3', 'FB4']:
                result[col_name] = col_b_split[idx].apply(clean_text)
            elif col_name in ['CauHoi', 'DanhGia']:
                result[col_name] = pd.to_numeric(col_b_split[idx], errors='coerce')
            else:
                result[col_name] = col_b_split[idx]
        else:
            result[col_name] = None
    
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
    sample_cols = ['Lop', 'MaSV', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
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
