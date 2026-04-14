import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import sqlalchemy as sa
import urllib

print("🚀 Starting Optimized ETL Pipeline for New Data Format...")

# ==================== ENVIRONMENT VARIABLES ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

# ==================== HELPER FUNCTIONS ====================
def clean_text_vectorized(series, max_len=500):
    series = series.astype(str).str.strip()
    series = series.replace(['NULL', 'nan', 'None', ''], None)
    # Sửa lỗi hiển thị tiếng Việt nếu có
    series = series.apply(lambda x: ftfy.fix_text(str(x)) if pd.notna(x) else x)
    if max_len:
        series = series.str[:max_len]
    return series

def convert_masv_vectorized(series):
    def safe_convert(x):
        if pd.isna(x) or str(x).strip() in ['', 'NULL', 'nan']:
            return None
        try:
            # Xử lý trường hợp số khoa học hoặc số thực
            return str(int(float(str(x).replace(',', ''))))
        except:
            return str(x).strip()
    return series.map(safe_convert)

# ==================== DOWNLOAD & READ ====================
print("📥 Connecting to Azure Storage...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
data = blob_client.download_blob().readall()

print("📊 Reading CSV (New Format: Comma Separated)...")
# Chuyển sang dùng sep=',' và encoding utf-8
df = pd.read_csv(
    io.BytesIO(data),
    sep=',', 
    header=None,
    dtype=str,
    encoding='utf-8', 
    on_bad_lines='skip',
    low_memory=False
)

# Mapping lại chính xác theo file mẫu bạn gửi
df.columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
    'LopHP', 'CauHoi', 'DanhGia', 'Col13', 'Q13', 'Q14', 'Q15', 'Q16'
]

print(f"✅ Read {len(df):,} rows. Sample: {df.iloc[0]['HoDem']} {df.iloc[0]['Ten']}")

# ==================== CLEANING ====================
print("🧹 Cleaning data...")
cols_to_clean = {
    'Lop': 50, 'HoDem': 100, 'Ten': 50, 'MaHP': 50, 'TenHP': 200, 
    'MaGV': 50, 'HoDemGV': 100, 'TenGV': 100, 'LopHP': 100
}

for col, length in cols_to_clean.items():
    df[col] = clean_text_vectorized(df[col], length)

df['MaSV'] = convert_masv_vectorized(df['MaSV'])
df['CauHoi'] = pd.to_numeric(df['CauHoi'], errors='coerce')
df['DanhGia'] = pd.to_numeric(df['DanhGia'], errors='coerce')

# Làm sạch các câu hỏi tự luận
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    df[q] = clean_text_vectorized(df[q], max_len=1000)

# ==================== ETL LOGIC ====================
df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df['NamHoc'] = SEMESTER

# Tạo Key duy nhất để gộp 12 dòng của 1 sinh viên thành 1 dòng ngang
df['StudentKey'] = (
    df['LopHP'].fillna('') + '|' + 
    df['MaSV'].fillna('') + '|' + 
    df['MaHP'].fillna('')
)

# Tạo ID duy nhất (SV000001...)
unique_keys = df['StudentKey'].unique()
key_to_id = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_keys)}
df['ID'] = df['StudentKey'].map(key_to_id)

print("🔄 Pivoting Q1-Q12...")
# Chỉ lấy các dòng từ câu hỏi 1-12 để pivot
df_pivot = df[df['CauHoi'].between(1, 12)].pivot_table(
    index='ID',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()

df_pivot.columns = ['ID'] + [f'Q{int(c)}' for c in df_pivot.columns[1:]]

# Lấy thông tin gốc (tên, mã môn...) bằng cách group by ID
df_info = df.groupby('ID').first().reset_index()
df_info = df_info.drop(columns=[f'Q{i}' for i in range(13, 17)] + ['CauHoi', 'DanhGia', 'Col13'], errors='ignore')

# Merge lại thành bảng cuối cùng
df_final = df_info.merge(df_pivot, on='ID', how='left')

# Đảm bảo các cột Q13-Q16 có dữ liệu
final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
              'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
             [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc']

df_final = df_final[[c for c in final_cols if c in df_final.columns]]
df_final['NgaySinh'] = pd.to_datetime(df_final['NgaySinh'], format='%d/%m/%Y', errors='coerce')

print(f"✅ Processing complete: {len(df_final)} survey records.")

# ==================== SQL LOAD ====================
print("\n🔄 Loading into Azure SQL...")
sql_params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;DATABASE=course-survey-db;"
    f"UID=sqladmin;PWD=Due@2026;"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)
engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={sql_params}", fast_executemany=True)

try:
    with engine.connect() as conn:
        # 1. Sinh Viên
        df_sv = df_final[['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].drop_duplicates('ID')
        df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False, chunksize=1000)
        
        # 2. Học Phần
        df_hp = df_final[['MaHP', 'TenHP']].drop_duplicates('MaHP')
        df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False)
        
        # 3. Giảng Viên
        df_gv = df_final[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
        df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False)
        
        # 4. Lớp Học Phần
        df_lhp = df_final[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].drop_duplicates('LopHP')
        df_lhp = df_lhp.rename(columns={'LopHP': 'MaLopHP'})
        df_lhp['TenLopHP'] = df_lhp['MaLopHP']
        df_lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False)
        
        # 5. Phiếu Khảo Sát
        df_ks = df_final.rename(columns={'ID': 'ID_SV', 'LopHP': 'MaLopHP'})
        ks_cols = ['ID_SV', 'MaLopHP', 'HocKy', 'NamHoc'] + [f'Q{i}' for i in range(1, 17)]
        df_ks[ks_cols].to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False, chunksize=1000)

    print("🎯 ALL DONE!")
except Exception as e:
    print(f"❌ Error: {e}")
