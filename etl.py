# etl.py - ĐÃ FIX LỖI ENCODING
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime

print("🚀 Starting ETL Pipeline...")
print(f"Python version: {sys.version}")

# Lấy connection string từ environment variable
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

print(f"SEMESTER = {SEMESTER}")
print(f"SURVEY_FILE = {SURVEY_FILE}")
print(f"CONNECTION_STRING exists = {CONNECTION_STRING is not None}")

if not CONNECTION_STRING:
    print("❌ ERROR: Missing CONNECTION_STRING environment variable!")
    sys.exit(1)

if not SEMESTER or not SURVEY_FILE:
    print("❌ ERROR: Missing SEMESTER or SURVEY_FILE!")
    sys.exit(1)

try:
    # Kết nối Azure
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    raw_container = blob_service.get_container_client("rawdata")
    print("✅ Connected to container: rawdata")
    
    # Đường dẫn file
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Looking for file: {blob_path}")
    
    # Kiểm tra file tồn tại
    blob_client = raw_container.get_blob_client(blob_path)
    if not blob_client.exists():
        print(f"❌ File not found: {blob_path}")
        sys.exit(1)
    
    # Đọc file
    print("📥 Downloading file...")
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data)} bytes")
    
    # ===== FIX ENCODING: THỬ NHIỀU ENCODING =====
    print("📊 Parsing CSV with multiple encodings...")
    
    encodings_to_try = ['cp1258', 'latin1', 'utf-8', 'iso-8859-1']
    df = None
    used_encoding = None
    
    for encoding in encodings_to_try:
        try:
            print(f"   Trying encoding: {encoding}...")
            df = pd.read_csv(
                io.BytesIO(data), 
                encoding=encoding, 
                sep='\t', 
                header=None,
                dtype=str,
                low_memory=False
            )
            used_encoding = encoding
            print(f"   ✅ Success with encoding: {encoding}")
            break
        except UnicodeDecodeError as e:
            print(f"   ❌ Failed with {encoding}: {str(e)[:50]}...")
            continue
        except Exception as e:
            print(f"   ❌ Error with {encoding}: {str(e)[:50]}...")
            continue
    
    if df is None:
        print("❌ ERROR: Could not read file with any encoding!")
        sys.exit(1)
    
    print(f"✅ Parsed {len(df)} rows using {used_encoding} encoding")
    
    # Gán tên cột
    expected_cols = 17  # Số cột dự kiến
    if len(df.columns) < expected_cols:
        print(f"⚠️ Warning: Found {len(df.columns)} columns, expected {expected_cols}")
        # Thêm cột thiếu
        for i in range(len(df.columns), expected_cols):
            df[i] = None
    
    df.columns = [
        "Lop", "MaSV", "HoDem", "Ten", "NgaySinh", "MaHP", "TenHP",
        "MaGV", "HoDemGV", "TenGV", "LopHP", "CauHoi", "DanhGia",
        "FB1", "FB2", "FB3", "FB4"
    ][:len(df.columns)]
    
    # Fix MaSV - Xử lý dòng có nhiều thông tin
    def fix_masv_column(value):
        if pd.isna(value):
            return None
        value_str = str(value).strip()
        # Nếu có khoảng trắng, lấy phần đầu tiên
        if ' ' in value_str:
            value_str = value_str.split()[0]
        try:
            # Chuyển đổi số
            if 'E' in value_str or 'e' in value_str:
                return str(int(float(value_str)))
            return str(int(float(value_str)))
        except:
            return value_str
    
    df["MaSV"] = df["MaSV"].apply(fix_masv_column)
    
    # Xử lý tách họ tên nếu cần
    # Nếu cột Ten bị null, thử tách từ MaSV
    if 'Ten' in df.columns and df['Ten'].isna().all():
        print("   ⚠️ Detected raw format, parsing additional fields...")
        # Xử lý dòng đặc biệt: MaSV chứa nhiều thông tin
        for idx, row in df.iterrows():
            if pd.notna(row['MaSV']) and ' ' in str(row['MaSV']):
                parts = str(row['MaSV']).split()
                if len(parts) >= 4:
                    df.at[idx, 'MaSV'] = fix_masv_column(parts[0])
                    df.at[idx, 'HoDem'] = parts[1] if len(parts) > 1 else None
                    df.at[idx, 'Ten'] = parts[2] if len(parts) > 2 else None
                    df.at[idx, 'NgaySinh'] = parts[3] if len(parts) > 3 else None
    
    # Xử lý ngày sinh
    df["NgaySinh"] = pd.to_datetime(df["NgaySinh"], errors='coerce', dayfirst=True)
    
    # Chuyển đổi số
    df["CauHoi"] = pd.to_numeric(df["CauHoi"], errors='coerce')
    df["DanhGia"] = pd.to_numeric(df["DanhGia"], errors='coerce')
    
    # Thêm metadata
    df["HocKy"] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    df["NamHoc"] = SEMESTER
    df["ProcessedDate"] = datetime.now()
    
    # Lọc bỏ dòng null quan trọng
    before_filter = len(df)
    df = df.dropna(subset=['MaSV', 'MaHP', 'CauHoi'], how='all')
    print(f"   Filtered out {before_filter - len(df)} invalid rows")
    
    if len(df) == 0:
        print("❌ ERROR: No valid data after processing!")
        sys.exit(1)
    
    # Upload kết quả
    print("📤 Uploading processed data...")
    output = df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    # Tạo processed container nếu chưa có
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
        print("✅ Created container: processed-data")
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # In kết quả
    print(f"\n✅ SUCCESS!")
    print(f"📊 Total records: {len(df):,}")
    print(f"⭐ Average rating: {df['DanhGia'].mean():.2f}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    # Hiển thị mẫu dữ liệu
    print(f"\n📋 Sample data (first 3 rows):")
    sample_cols = ['Lop', 'MaSV', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
    sample_cols = [c for c in sample_cols if c in df.columns]
    print(df[sample_cols].head(3).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
