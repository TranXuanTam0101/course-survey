# etl.py - TỐI ƯU, NHANH, GỌN
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re

print("🚀 Starting ETL Pipeline...")

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def clean_text(text):
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    if '?' in text:
        text = ftfy.fix_text(text)
    return text.strip()

def convert_masv(value):
    if pd.isna(value) or value == '':
        return None
    try:
        return str(int(float(str(value))))
    except:
        return value

def extract_hodem_ten(full_name):
    if pd.isna(full_name) or full_name == '':
        return None, None
    parts = str(full_name).split()
    if len(parts) == 0:
        return None, None
    elif len(parts) == 1:
        return None, clean_text(parts[0])
    else:
        return clean_text(' '.join(parts[:-1])), clean_text(parts[-1])

def parse_row(fields):
    """Parse một dòng dữ liệu"""
    if len(fields) < 17:
        return None
    
    result = {}
    
    # Lop
    lop_raw = fields[0].strip()
    result['Lop'] = lop_raw.split(' ')[0] if ' ' in lop_raw else lop_raw.split('\t')[0] if '\t' in lop_raw else lop_raw
    
    # MaSV
    result['MaSV'] = convert_masv(fields[1].strip() if len(fields) > 1 else '')
    
    # Tìm NgaySinh
    ngaysinh_index = -1
    for i, field in enumerate(fields):
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', field.strip()):
            ngaysinh_index = i
            break
    
    if ngaysinh_index == -1:
        return None
    
    result['NgaySinh'] = pd.to_datetime(fields[ngaysinh_index].strip(), errors='coerce', dayfirst=True)
    
    # HoDem & Ten
    if ngaysinh_index > 1:
        hodem, ten = extract_hodem_ten(' '.join(fields[2:ngaysinh_index]))
        result['HoDem'] = hodem
        result['Ten'] = ten
    else:
        result['HoDem'] = result['Ten'] = None
    
    # MaHP
    result['MaHP'] = fields[ngaysinh_index + 1].strip() if ngaysinh_index + 1 < len(fields) else None
    
    # Tìm MaGV
    magv_index = -1
    for i in range(ngaysinh_index + 2, min(ngaysinh_index + 10, len(fields))):
        if re.match(r'^\d+$', fields[i].strip()):
            magv_index = i
            break
    
    # TenHP
    if magv_index > ngaysinh_index + 1:
        result['TenHP'] = clean_text(' '.join(fields[ngaysinh_index + 2:magv_index]))
    else:
        result['TenHP'] = None
    
    # MaGV
    result['MaGV'] = fields[magv_index].strip() if magv_index != -1 else None
    
    # Tìm LopHP
    lophp_index = -1
    for i in range(magv_index + 1, min(magv_index + 10, len(fields))):
        if '_' in fields[i] or re.match(r'^[A-Z0-9_]+$', fields[i].strip()):
            lophp_index = i
            break
    
    # HoDemGV & TenGV
    if lophp_index > magv_index + 1:
        hodem_gv, ten_gv = extract_hodem_ten(' '.join(fields[magv_index + 1:lophp_index]))
        result['HoDemGV'] = hodem_gv
        result['TenGV'] = ten_gv
    else:
        result['HoDemGV'] = result['TenGV'] = None
    
    # LopHP
    result['LopHP'] = fields[lophp_index].strip() if lophp_index != -1 else None
    
    # CauHoi, DanhGia
    result['CauHoi'] = pd.to_numeric(fields[lophp_index + 1], errors='coerce') if lophp_index + 1 < len(fields) else None
    result['DanhGia'] = pd.to_numeric(fields[lophp_index + 2], errors='coerce') if lophp_index + 2 < len(fields) else None
    
    # FB1-FB4
    fb_start = lophp_index + 3
    while fb_start < len(fields) and fields[fb_start].strip() == 'NULL':
        fb_start += 1
    
    result['FB1'] = clean_text(fields[fb_start]) if fb_start < len(fields) else None
    result['FB2'] = clean_text(fields[fb_start + 1]) if fb_start + 1 < len(fields) else None
    result['FB3'] = clean_text(fields[fb_start + 2]) if fb_start + 2 < len(fields) else None
    result['FB4'] = clean_text(fields[fb_start + 3]) if fb_start + 3 < len(fields) else None
    
    return result

try:
    # Đọc file
    print("📥 Connecting to Azure...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # Giải mã và xử lý
    print("📊 Processing...")
    text = ftfy.fix_text(data.decode('cp1258'))
    lines = text.split('\n')
    
    records = []
    for i, line in enumerate(lines):
        if i % 50000 == 0 and i > 0:
            print(f"   Processed {i:,}/{len(lines):,} lines")
        if line.strip():
            fields = line.split('\t')
            record = parse_row(fields)
            if record and record.get('MaSV') and record.get('MaHP') and record.get('CauHoi'):
                records.append(record)
    
    print(f"✅ Parsed {len(records):,} records")
    
    if not records:
        print("❌ No valid records!")
        sys.exit(1)
    
    # Tạo DataFrame
    df = pd.DataFrame(records)
    df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df['NamHoc'] = SEMESTER
    df['ProcessedDate'] = datetime.now()
    
    # Sắp xếp cột
    cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
            'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
            'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate']
    df = df[[c for c in cols if c in df.columns]].sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # Upload
    print("📤 Uploading...")
    output = df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    
    # Thống kê nhanh theo câu hỏi
    print(f"\n📊 RATING BY QUESTION:")
    q_stats = df.groupby('CauHoi')['DanhGia'].agg(['mean', 'count'])
    for q in range(1, 13):
        if q in q_stats.index:
            print(f"   Q{q:2d}: {q_stats.loc[q, 'mean']:.2f}/5 ({q_stats.loc[q, 'count']:,})")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    sys.exit(1)
# insert_to_sql.py - Chèn dữ liệu vào SQL Azure
import os
import pandas as pd
from sqlalchemy import create_engine, text
import urllib

# Cấu hình SQL Azure
SQL_SERVER = "your-server.database.windows.net"
SQL_DATABASE = "SurveyDB"
SQL_USERNAME = "your_username"
SQL_PASSWORD = "your_password"

# Tạo connection string
connection_string = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server=tcp:{SQL_SERVER},1433;"
    f"Database={SQL_DATABASE};"
    f"Uid={SQL_USERNAME};"
    f"Pwd={SQL_PASSWORD};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

# Tạo engine
params = urllib.parse.quote_plus(connection_string)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Đọc dữ liệu đã xử lý
df = pd.read_csv("processed_data.csv", encoding='utf-8-sig')

# Chuyển đổi kiểu dữ liệu
df['NgaySinh'] = pd.to_datetime(df['NgaySinh'], errors='coerce')
df['CauHoi'] = df['CauHoi'].astype(int)
df['DanhGia'] = df['DanhGia'].astype(int)
df['HocKy'] = df['HocKy'].astype(int)

# Insert vào SQL Azure
df.to_sql(
    'survey_responses',
    engine,
    if_exists='append',  # 'append' để thêm, 'replace' để thay thế
    index=False,
    method='multi',
    chunksize=1000
)

print(f"✅ Inserted {len(df)} records into SQL Azure")
