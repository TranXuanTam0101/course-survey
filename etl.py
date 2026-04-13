import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import sqlalchemy as sa
import urllib

print("🚀 Starting Optimized ETL Pipeline + SQL Load (Fast Version)...")

# ==================== ENVIRONMENT VARIABLES ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

# ==================== HÀM LÀM SẠCH (Vectorized) ====================
def clean_text_vectorized(series, max_len=None):
    """Làm sạch hàng loạt - nhanh hơn .apply rất nhiều"""
    series = series.astype(str).str.strip()
    series = series.replace(['NULL', 'nan', ''], None)
    
    # Sửa encoding tiếng Việt
    series = series.apply(lambda x: ftfy.fix_text(x) if pd.notna(x) and '?' in str(x) else x)
    series = series.str.strip()
    
    if max_len:
        series = series.str[:max_len]
    
    return series.where(series.notna() & (series != ''), None)

def convert_masv_vectorized(series):
    series = series.astype(str).str.replace(',', '', regex=False)
    # Chuyển scientific notation
    def safe_convert(x):
        if pd.isna(x) or str(x).strip() in ['', 'NULL']:
            return None
        try:
            return str(int(float(x)))
        except:
            return str(x).strip()
    return series.map(safe_convert)

# ==================== KẾT NỐI & DOWNLOAD ====================
print("📥 Connecting to Azure Storage...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

data = blob_client.download_blob().readall()
print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")

# ==================== ĐỌC FILE (Tối ưu) ====================
print("📊 Reading CSV...")
df = pd.read_csv(
    io.BytesIO(data),
    sep='\t',
    header=None,
    dtype=str,
    encoding='cp1258',
    on_bad_lines='skip',
    low_memory=False
)
print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")

# Gán tên cột
df.columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
    'LopHP', 'CauHoi', 'DanhGia', 'Col13', 'Q13', 'Q14', 'Q15', 'Q16',
    'Col18','Col19','Col20','Col21','Col22','Col23','Col24','Col25','Col26','Col27',
    'Col28','Col29','Col30','Col31'
]

# ==================== LÀM SẠCH DỮ LIỆU (VECTORIZED - NHANH) ====================
print("🧹 Cleaning data with vectorized operations...")

df['Lop']      = clean_text_vectorized(df['Lop'])
df['HoDem']    = clean_text_vectorized(df['HoDem'])
df['Ten']      = clean_text_vectorized(df['Ten'])
df['NgaySinh'] = clean_text_vectorized(df['NgaySinh'])
df['MaHP']     = clean_text_vectorized(df['MaHP'])
df['TenHP']    = clean_text_vectorized(df['TenHP'])
df['MaGV']     = clean_text_vectorized(df['MaGV'])
df['HoDemGV']  = clean_text_vectorized(df['HoDemGV'])
df['TenGV']    = clean_text_vectorized(df['TenGV'])
df['LopHP']    = clean_text_vectorized(df['LopHP'])

df['MaSV']     = convert_masv_vectorized(df['MaSV'])

df['CauHoi']   = pd.to_numeric(df['CauHoi'], errors='coerce')
df['DanhGia']  = pd.to_numeric(df['DanhGia'], errors='coerce')

# Feedback Q13-Q16
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    df[q] = clean_text_vectorized(df[q], max_len=1000)

print("✅ Data cleaning completed (vectorized).")

# ==================== ETL LOGIC (Tối ưu) ====================
df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df['NamHoc'] = SEMESTER
df['ProcessedDate'] = datetime.now()

# Tạo StudentKey nhanh hơn
df['StudentKey'] = (
    df['Lop'].fillna('') + '|' +
    df['MaSV'].fillna('') + '|' +
    df['HoDem'].fillna('') + '|' +
    df['Ten'].fillna('') + '|' +
    df['NgaySinh'].fillna('')
)

# Map ID sinh viên
unique_students = df['StudentKey'].unique()
student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
df['ID'] = df['StudentKey'].map(student_id_map)

# Lấy thông tin cơ bản một lần
df_basic = df.groupby('StudentKey', as_index=False).first()

print("🔄 Pivoting Q1-Q12 (optimized)...")

# Chỉ giữ dữ liệu cần cho pivot
df_questions = df[df['CauHoi'].between(1, 12)][['StudentKey', 'CauHoi', 'DanhGia']].copy()

# Tạo pivot nhanh
pivot_q = df_questions.pivot_table(
    index='StudentKey',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()

pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns[1:]]

# Merge
df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')

# Thêm Q13-Q16
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    if q in df.columns:
        q_map = df.groupby('StudentKey')[q].first()
        df_final[q] = df_final['StudentKey'].map(q_map)

# Chọn cột cuối
final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
              'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
             [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc']

df_final = df_final[[c for c in final_cols if c in df_final.columns]].copy()
df_final = df_final.sort_values('ID').reset_index(drop=True)

print(f"\n🎉 Hoàn tất xử lý! Final dataset: {len(df_final):,} sinh viên, {len(df_final.columns)} cột")

# ==================== LƯU & UPLOAD ====================
print("📤 Uploading to Azure Storage...")
output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.txt','').replace('.csv','')}_processed.csv"
processed_container = blob_service.get_container_client("processed-data")
if not processed_container.exists():
    processed_container.create_container()

processed_container.get_blob_client(output_path).upload_blob(
    df_final.to_csv(index=False, encoding='utf-8-sig'), 
    overwrite=True
)
print(f"✅ Uploaded to: processed-data/{output_path}")

# ==================== LOAD VÀO SQL ====================
print("\n🔄 Loading data into Azure SQL...")

sql_server = "course-survey.database.windows.net"
sql_db     = "course-survey-db"
sql_user   = "sqladmin"
sql_pass   = "Due@2026"

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sql_server};DATABASE={sql_db};"
    f"UID={sql_user};PWD={sql_pass};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
)

engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

try:
    with engine.connect() as conn:
        with conn.begin():
            # SINH_VIEN
            print("   - Inserting SINH_VIEN...")
            sv_cols = ['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']
            df_sv = df_final[[c for c in sv_cols if c in df_final.columns]].drop_duplicates(subset=['ID'])
            df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False, method='multi', chunksize=5000)

            # HOC_PHAN
            print("   - Inserting HOC_PHAN...")
            df_hp = df_final[['MaHP', 'TenHP']].drop_duplicates(subset=['MaHP']).dropna(subset=['MaHP'])
            df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False, method='multi', chunksize=1000)

            # GIANG_VIEN
            print("   - Inserting GIANG_VIEN...")
            df_gv = df_final[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).dropna(subset=['MaGV'])
            df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False, method='multi', chunksize=1000)

            # LOP_HOC_PHAN
            print("   - Inserting LOP_HOC_PHAN...")
            lhp = df_final[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
            lhp = lhp.rename(columns={'LopHP': 'MaLopHP'})
            lhp['TenLopHP'] = lhp['MaLopHP']
            lhp = lhp.drop_duplicates(subset=['MaLopHP']).dropna(subset=['MaLopHP'])
            lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False, method='multi', chunksize=5000)

            # PHIEU_KHAO_SAT
            print("   - Inserting PHIEU_KHAO_SAT...")
            fact_cols = ['ID', 'LopHP', 'HocKy', 'NamHoc']
            for i in range(1, 17):
                q = f'Q{i}'
                if q in df_final.columns:
                    fact_cols.append(q)
            
            df_fact = df_final[fact_cols].copy()
            df_fact = df_fact.rename(columns={'ID': 'ID_SV', 'LopHP': 'MaLopHP'})
            df_fact.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False, method='multi', chunksize=5000)

    print("✅ All data successfully loaded into Azure SQL!")

except Exception as e:
    print(f"❌ SQL Load Error: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n🎯 FULL OPTIMIZED PIPELINE COMPLETED SUCCESSFULLY!")
