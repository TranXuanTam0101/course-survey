# etl.py
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

# Debug: Kiểm tra các biến môi trường
print(f"SEMESTER = {SEMESTER}")
print(f"SURVEY_FILE = {SURVEY_FILE}")
print(f"CONNECTION_STRING exists = {CONNECTION_STRING is not None}")

# Kiểm tra nếu thiếu connection string
if not CONNECTION_STRING:
    print("❌ ERROR: Missing CONNECTION_STRING environment variable!")
    print("Please add GitHub Secret: AZURE_CONNECTION_STRING")
    sys.exit(1)

# Kiểm tra nếu thiếu semester hoặc survey_file
if not SEMESTER or not SURVEY_FILE:
    print("❌ ERROR: Missing SEMESTER or SURVEY_FILE!")
    print("Please provide both when running workflow")
    sys.exit(1)

try:
    # Kết nối Azure
    print(f"📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    
    # Kiểm tra container
    raw_container = blob_service.get_container_client("rawdata")
    print(f"✅ Connected to container: rawdata")
    
    # Đường dẫn file
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Looking for file: {blob_path}")
    
    # Kiểm tra file tồn tại
    blob_client = raw_container.get_blob_client(blob_path)
    if not blob_client.exists():
        print(f"❌ File not found: {blob_path}")
        print(f"📁 Listing files in {SEMESTER}/:")
        blobs = raw_container.list_blobs(name_starts_with=f"{SEMESTER}/")
        for blob in blobs:
            print(f"   - {blob.name}")
        sys.exit(1)
    
    # Đọc file
    print(f"📥 Downloading file...")
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data)} bytes")
    
    # Đọc CSV
    print(f"📊 Parsing CSV...")
    df = pd.read_csv(io.BytesIO(data), encoding='utf-8', sep='\t', header=None)
    print(f"✅ Parsed {len(df)} rows")
    
    # Gán tên cột
    df.columns = [
        "Lop", "MaSV", "HoDem", "Ten", "NgaySinh", "MaHP", "TenHP",
        "MaGV", "HoDemGV", "TenGV", "LopHP", "CauHoi", "DanhGia",
        "FB1", "FB2", "FB3", "FB4"
    ]
    
    # Fix MaSV
    df["MaSV"] = df["MaSV"].apply(lambda x: str(int(float(x))) if pd.notna(x) else x)
    
    # Xử lý ngày sinh
    df["NgaySinh"] = pd.to_datetime(df["NgaySinh"], errors='coerce', dayfirst=True)
    
    # Thêm metadata
    df["HocKy"] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    df["NamHoc"] = SEMESTER
    df["ProcessedDate"] = datetime.now()
    
    # Upload kết quả
    print(f"📤 Uploading processed data...")
    output = df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    # Tạo processed container nếu chưa có
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
        print(f"✅ Created container: processed-data")
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # In kết quả
    print(f"\n✅ SUCCESS!")
    print(f"📊 Total records: {len(df):,}")
    print(f"⭐ Average rating: {df['DanhGia'].mean():.2f}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
