# etl.py - ĐƠN GIẢN NHẤT CÓ THỂ
import os
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime

# Lấy từ GitHub Secrets
CONNECTION_STRING = os.environ["CONNECTION_STRING"]
SEMESTER = os.environ["SEMESTER"]
SURVEY_FILE = os.environ["SURVEY_FILE"]

# Kết nối Azure
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
raw_container = blob_service.get_container_client("rawdata")
processed_container = blob_service.get_container_client("processed-data")

# Đọc file
blob_path = f"{SEMESTER}/{SURVEY_FILE}"
print(f"📥 Đang xử lý: {blob_path}")

blob_client = raw_container.get_blob_client(blob_path)
data = blob_client.download_blob().readall()

# Đọc CSV
df = pd.read_csv(io.BytesIO(data), encoding='utf-8', sep='\t', header=None)

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
df["HocKy"] = 2 if "252" in SURVEY_FILE else 1
df["NamHoc"] = SEMESTER
df["ProcessedDate"] = datetime.now()

# Upload kết quả
output = df.to_csv(index=False, encoding='utf-8-sig')
output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)

print(f"✅ Xử lý xong! {len(df)} records")
print(f"📤 Đã upload lên: processed-data/{output_path}")
print(f"⭐ Điểm TB: {df['DanhGia'].mean():.2f}")
