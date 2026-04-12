# etl.py
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime

# Lấy connection string từ GitHub Actions (không có trong code)
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")

# Kiểm tra nếu không có connection string
if not CONNECTION_STRING:
    print("❌ LỖI: Không tìm thấy CONNECTION_STRING")
    print("Vui lòng cấu hình GitHub Secret: AZURE_CONNECTION_STRING")
    sys.exit(1)

# Lấy thông tin từ GitHub Actions
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("❌ LỖI: Thiếu Semester hoặc Survey File")
    sys.exit(1)

print(f"🚀 Bắt đầu xử lý: {SEMESTER}/{SURVEY_FILE}")

# Kết nối Azure
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
raw_container = blob_service.get_container_client("rawdata")
processed_container = blob_service.get_container_client("processed-data")

# Phần còn lại giữ nguyên...
blob_path = f"{SEMESTER}/{SURVEY_FILE}"
blob_client = raw_container.get_blob_client(blob_path)
data = blob_client.download_blob().readall()

df = pd.read_csv(io.BytesIO(data), encoding='utf-8', sep='\t', header=None)

df.columns = [
    "Lop", "MaSV", "HoDem", "Ten", "NgaySinh", "MaHP", "TenHP",
    "MaGV", "HoDemGV", "TenGV", "LopHP", "CauHoi", "DanhGia",
    "FB1", "FB2", "FB3", "FB4"
]

df["MaSV"] = df["MaSV"].apply(lambda x: str(int(float(x))) if pd.notna(x) else x)
df["NgaySinh"] = pd.to_datetime(df["NgaySinh"], errors='coerce', dayfirst=True)
df["HocKy"] = 2 if "252" in SURVEY_FILE else 1
df["NamHoc"] = SEMESTER
df["ProcessedDate"] = datetime.now()

output = df.to_csv(index=False, encoding='utf-8-sig')
output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)

print(f"\n✅ XỬ LÝ THÀNH CÔNG!")
print(f"📊 Số records: {len(df):,}")
print(f"⭐ Điểm TB: {df['DanhGia'].mean():.2f}")
print(f"📤 Đã upload: processed-data/{output_path}")
