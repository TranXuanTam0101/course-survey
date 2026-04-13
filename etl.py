import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import sqlalchemy as sa
import urllib
import numpy as np

print("🚀 Starting Ultra Fast ETL Pipeline...")

# ==================== ENVIRONMENT VARIABLES ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

# ==================== ULTRA FAST CLEAN ====================
def clean_text(series, max_len=None):
    series = series.astype(str).str.strip()
    series = series.replace(['NULL', 'nan', ''], np.nan)

    # vectorized ftfy (nhanh hơn apply)
    series = series.where(series.isna(), series.map(ftfy.fix_text))

    if max_len:
        series = series.str.slice(0, max_len)

    return series

def convert_masv(series):
    series = series.astype(str).str.replace(',', '', regex=False)
    return pd.to_numeric(series, errors='ignore').astype(str)

# ==================== DOWNLOAD ====================
print("📥 Connecting to Azure Storage...")

blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

data = blob_client.download_blob().readall()

print(f"✅ Downloaded {len(data)/1024/1024:.2f} MB")

# ==================== READ CSV ====================
print("📊 Reading CSV...")

df = pd.read_csv(
    io.BytesIO(data),
    sep='\t',
    header=None,
    dtype=str,
    encoding='cp1258',
    low_memory=False
)

print(f"✅ Read {len(df):,} rows")

# ==================== COLUMN NAMES ====================
df.columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
    'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
    'Col13','Q13','Q14','Q15','Q16',
    'Col18','Col19','Col20','Col21','Col22','Col23',
    'Col24','Col25','Col26','Col27',
    'Col28','Col29','Col30','Col31'
]

# ==================== CLEAN ====================
print("🧹 Cleaning...")

clean_map = {
    'Lop':50,
    'HoDem':100,
    'Ten':50,
    'MaHP':50,
    'TenHP':200,
    'MaGV':50,
    'HoDemGV':100,
    'TenGV':100,
    'LopHP':100
}

for col, length in clean_map.items():
    df[col] = clean_text(df[col], length)

df['MaSV'] = convert_masv(df['MaSV'])

df['CauHoi']  = pd.to_numeric(df['CauHoi'], errors='coerce')
df['DanhGia'] = pd.to_numeric(df['DanhGia'], errors='coerce')

for q in ['Q13','Q14','Q15','Q16']:
    df[q] = clean_text(df[q],1000)

print("✅ Clean done")

# ==================== FAST KEY ====================
print("⚡ Creating Student Key...")

df['StudentKey'] = pd.factorize(
    df[['Lop','MaSV','HoDem','Ten','NgaySinh']]
    .astype(str)
    .agg('|'.join, axis=1)
)[0]

df['ID'] = 'SV' + (df['StudentKey']+1).astype(str).str.zfill(6)

# ==================== BASIC ====================
df_basic = df.drop_duplicates('StudentKey')

# ==================== PIVOT ====================
print("🔄 Pivoting...")

df_q = df[df['CauHoi'].between(1,12)]

pivot = (
    df_q
    .pivot_table(
        index='StudentKey',
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    )
    .add_prefix('Q')
    .reset_index()
)

df_final = df_basic.merge(pivot, on='StudentKey', how='left')

# ==================== Q13-16 ====================
extra = df.groupby('StudentKey')[['Q13','Q14','Q15','Q16']].first()
df_final = df_final.merge(extra, on='StudentKey', how='left')

# ==================== FINAL ====================
df_final['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df_final['NamHoc'] = SEMESTER

final_cols = [
    'ID','Lop','MaSV','HoDem','Ten','NgaySinh',
    'MaHP','TenHP','MaGV','HoDemGV','TenGV','LopHP'
] + [f'Q{i}' for i in range(1,17)] + ['HocKy','NamHoc']

df_final = df_final[final_cols]

df_final['NgaySinh'] = pd.to_datetime(
    df_final['NgaySinh'],
    format='%d/%m/%Y',
    errors='coerce'
)

print(f"🎉 Final rows: {len(df_final):,}")

# ==================== SAVE ====================
local_filename = f"{SURVEY_FILE}_processed.csv"

df_final.to_csv(local_filename,index=False,encoding='utf-8-sig')

# ==================== SQL LOAD (FAST) ====================
print("🚀 Loading SQL...")

params = urllib.parse.quote_plus(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=course-survey.database.windows.net;"
    "DATABASE=course-survey-db;"
    "UID=sqladmin;"
    "PWD=Due@2026;"
    "Encrypt=yes;"
)

engine = sa.create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    fast_executemany=True
)

with engine.begin() as conn:

    print("Insert SV")
    df_final[['ID','MaSV','Lop','HoDem','Ten','NgaySinh']]\
        .drop_duplicates('ID')\
        .to_sql('SINH_VIEN',conn,index=False,chunksize=10000,if_exists='append')

    print("Insert HP")
    df_final[['MaHP','TenHP']]\
        .drop_duplicates()\
        .to_sql('HOC_PHAN',conn,index=False,chunksize=5000,if_exists='append')

    print("Insert GV")
    df_final[['MaGV','HoDemGV','TenGV']]\
        .drop_duplicates()\
        .to_sql('GIANG_VIEN',conn,index=False,chunksize=5000,if_exists='append')

print("🎯 DONE")
