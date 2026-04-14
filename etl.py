import os
import sys
import io
import pandas as pd
import sqlalchemy as sa
import urllib.parse
import ftfy
from datetime import datetime
from azure.storage.blob import BlobServiceClient

print("🚀 Starting Professional ETL Pipeline (Fixed ParserError & MaSV PK)...")

# ==================== 1. CẤU HÌNH & BIẾN MÔI TRƯỜNG ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

sql_server = "course-survey.database.windows.net"
sql_db     = "course-survey-db"
sql_user   = "sqladmin"
sql_pass   = "Due@2026"

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing environment variables!")
    sys.exit(1)

# ==================== 2. HÀM TRỢ GIÚP (CLEANING) ====================
def clean_text(series, max_len=None):
    series = series.astype(str).str.strip()
    series = series.replace(['NULL', 'nan', 'None', ''], None)
    series = series.apply(lambda x: ftfy.fix_text(str(x)) if pd.notna(x) else x)
    if max_len:
        series = series.str[:max_len]
    return series

def convert_masv(series):
    def safe_convert(x):
        val = str(x).strip()
        if pd.isna(x) or val in ['', 'NULL', 'nan']:
            return None
        try:
            return str(int(float(val.replace(',', ''))))
        except:
            return val
    return series.map(safe_convert)

# ==================== 3. KẾT NỐI DATABASE & TẠO BẢNG ====================
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sql_server};DATABASE={sql_db};"
    f"UID={sql_user};PWD={sql_pass};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

schema_sql = """
-- Xóa bảng cũ theo thứ tự ràng buộc khóa ngoại
IF OBJECT_ID('PHIEU_KHAO_SAT', 'U') IS NOT NULL DROP TABLE PHIEU_KHAO_SAT;
IF OBJECT_ID('LOP_HOC_PHAN', 'U') IS NOT NULL DROP TABLE LOP_HOC_PHAN;
IF OBJECT_ID('SINH_VIEN', 'U') IS NOT NULL DROP TABLE SINH_VIEN;
IF OBJECT_ID('GIANG_VIEN', 'U') IS NOT NULL DROP TABLE GIANG_VIEN;
IF OBJECT_ID('HOC_PHAN', 'U') IS NOT NULL DROP TABLE HOC_PHAN;

CREATE TABLE SINH_VIEN (
    MaSV VARCHAR(50) PRIMARY KEY,
    Lop NVARCHAR(50),
    HoDem NVARCHAR(100),
    Ten NVARCHAR(50),
    NgaySinh DATE
);

CREATE TABLE HOC_PHAN (
    MaHP VARCHAR(50) PRIMARY KEY,
    TenHP NVARCHAR(200)
);

CREATE TABLE GIANG_VIEN (
    MaGV VARCHAR(50) PRIMARY KEY,
    HoDemGV NVARCHAR(100),
    TenGV NVARCHAR(100)
);

CREATE TABLE LOP_HOC_PHAN (
    MaLopHP VARCHAR(100) PRIMARY KEY,
    MaHP VARCHAR(50) REFERENCES HOC_PHAN(MaHP),
    MaGV VARCHAR(50) REFERENCES GIANG_VIEN(MaGV),
    TenLopHP NVARCHAR(200),
    HocKy INT,
    NamHoc NVARCHAR(20)
);

CREATE TABLE PHIEU_KHAO_SAT (
    MaSV VARCHAR(50) REFERENCES SINH_VIEN(MaSV),
    MaLopHP VARCHAR(100) REFERENCES LOP_HOC_PHAN(MaLopHP),
    HocKy INT,
    NamHoc NVARCHAR(20),
    Q1 INT, Q2 INT, Q3 INT, Q4 INT, Q5 INT, Q6 INT, 
    Q7 INT, Q8 INT, Q9 INT, Q10 INT, Q11 INT, Q12 INT,
    Q13 NVARCHAR(MAX), Q14 NVARCHAR(MAX), Q15 NVARCHAR(MAX), Q16 NVARCHAR(MAX),
    PRIMARY KEY (MaSV, MaLopHP)
);
"""

print("🛠 Re-creating database schema...")
with engine.connect() as conn:
    conn.execute(sa.text(schema_sql))
    conn.commit()

# ==================== 4. DOWNLOAD & XỬ LÝ DỮ LIỆU ====================
print("📥 Fetching raw data from Azure...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
data = blob_client.download_blob().readall()

print("📊 Reading CSV and handling potential bad lines...")
# Sử dụng engine='python' và on_bad_lines='skip' để xử lý dòng có dấu phẩy dư thừa
df_raw = pd.read_csv(
    io.BytesIO(data),
    sep=',',
    header=None,
    names=['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
           'LopHP', 'CauHoi', 'DanhGia', 'Col13', 'Q13', 'Q14', 'Q15', 'Q16'],
    dtype=str,
    encoding='utf-8',
    on_bad_lines='skip',
    engine='python'
)

print(f"✅ Loaded {len(df_raw):,} rows. Cleaning and Transforming...")

# Cleaning
df_raw['MaSV'] = convert_masv(df_raw['MaSV'])
df_raw['NgaySinh'] = pd.to_datetime(df_raw['NgaySinh'], format='%d/%m/%Y', errors='coerce')
df_raw['CauHoi'] = pd.to_numeric(df_raw['CauHoi'], errors='coerce')
df_raw['DanhGia'] = pd.to_numeric(df_raw['DanhGia'], errors='coerce')

# Xử lý Pivot
print("🔄 Pivoting survey answers (Q1-Q12)...")
df_pivot = df_raw[df_raw['CauHoi'].between(1, 12)].pivot_table(
    index=['MaSV', 'LopHP'],
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()

df_pivot.columns = ['MaSV', 'LopHP'] + [f'Q{int(i)}' for i in range(1, 13)]

# Lấy thông tin chung và câu tự luận
df_info = df_raw.groupby(['MaSV', 'LopHP']).first().reset_index()
df_final = df_info.merge(df_pivot, on=['MaSV', 'LopHP'], how='left')

df_final['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df_final['NamHoc'] = SEMESTER

# ==================== 5. CHÈN DỮ LIỆU VÀO SQL ====================
print(f"📤 Inserting {len(df_final):,} records into Azure SQL...")
try:
    with engine.connect() as conn:
        with conn.begin():
            # 1. SINH_VIEN
            df_sv = df_final[['MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].drop_duplicates('MaSV')
            df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False, chunksize=500)

            # 2. HOC_PHAN
            df_hp = df_final[['MaHP', 'TenHP']].drop_duplicates('MaHP')
            df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False)

            # 3. GIANG_VIEN
            df_gv = df_final[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
            df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False)

            # 4. LOP_HOC_PHAN
            df_lhp = df_final[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].drop_duplicates('LopHP')
            df_lhp = df_lhp.rename(columns={'LopHP': 'MaLopHP'})
            df_lhp['TenLopHP'] = df_lhp['MaLopHP']
            df_lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False)

            # 5. PHIEU_KHAO_SAT
            ks_cols = ['MaSV', 'LopHP', 'HocKy', 'NamHoc'] + [f'Q{i}' for i in range(1, 17)]
            df_ks = df_final[ks_cols].rename(columns={'LopHP': 'MaLopHP'})
            df_ks.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False, chunksize=1000)

    print("🎯 ETL PROCESS COMPLETED SUCCESSFULLY!")

except Exception as e:
    print(f"❌ SQL Insert Error: {str(e)}")
    sys.exit(1)
