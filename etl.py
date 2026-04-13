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
from concurrent.futures import ThreadPoolExecutor
import warnings
warnings.filterwarnings('ignore')

print("🚀 Starting Optimized ETL Pipeline + SQL Load (Ultra-Fast Version)...")

# ==================== ENVIRONMENT VARIABLES ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

# ==================== OPTIMIZED CLEANING FUNCTIONS ====================
def clean_text_optimized(df, columns, max_len=500):
    """Vectorized cleaning for multiple columns at once"""
    for col in columns:
        if col in df.columns:
            # Use numpy operations for speed
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['NULL', 'nan', ''], np.nan)
            
            # Batch ftfy operations (only for non-null values)
            mask = df[col].notna()
            if mask.any():
                df.loc[mask, col] = df.loc[mask, col].apply(lambda x: ftfy.fix_text(str(x))[:max_len] if max_len else ftfy.fix_text(str(x)))
    
    return df

def convert_masv_optimized(series):
    """Optimized MaSV conversion using numpy"""
    series = series.astype(str).str.replace(',', '', regex=False)
    series = series.replace(['nan', 'None', 'NULL', ''], np.nan)
    
    # Vectorized conversion
    mask = series.notna()
    if mask.any():
        try:
            # Try numeric conversion first
            numeric_vals = pd.to_numeric(series[mask], errors='coerce')
            mask_numeric = numeric_vals.notna()
            if mask_numeric.any():
                series.loc[mask & mask_numeric] = numeric_vals[mask_numeric].astype(int).astype(str)
        except:
            pass
    
    return series

# ==================== FAST DOWNLOAD & READ ====================
print("📥 Connecting to Azure Storage...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

# Download with streaming for large files
print("📥 Downloading file...")
data = blob_client.download_blob().readall()
print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")

# Optimized CSV reading
print("📊 Reading CSV with optimized settings...")
df = pd.read_csv(
    io.BytesIO(data),
    sep='\t',
    header=None,
    dtype=str,
    encoding='cp1258',
    on_bad_lines='skip',
    low_memory=False,
    engine='c'  # Use C engine for speed
)
print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")

# Column names
columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
    'LopHP', 'CauHoi', 'DanhGia', 'Col13', 'Q13', 'Q14', 'Q15', 'Q16',
    'Col18','Col19','Col20','Col21','Col22','Col23','Col24','Col25','Col26','Col27',
    'Col28','Col29','Col30','Col31'
]
df.columns = columns[:len(df.columns)]

# ==================== BATCH CLEANING ====================
print("🧹 Cleaning data (batch mode)...")

# Define column groups
text_cols_short = ['Lop', 'Ten', 'MaHP', 'MaGV']
text_cols_medium = ['HoDem', 'TenHP', 'HoDemGV', 'TenGV', 'LopHP']
text_cols_long = ['NgaySinh']

# Apply cleaning in batches
df = clean_text_optimized(df, text_cols_short, max_len=50)
df = clean_text_optimized(df, text_cols_medium, max_len=100)
df = clean_text_optimized(df, text_cols_long, max_len=None)

# Special handling for MaSV
df['MaSV'] = convert_masv_optimized(df['MaSV'])

# Convert numeric columns in one go
numeric_cols = ['CauHoi', 'DanhGia']
for col in numeric_cols:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Clean Q13-Q16
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    if q in df.columns:
        df[q] = df[q].astype(str).str.strip().replace(['NULL', 'nan', ''], np.nan)
        mask = df[q].notna()
        if mask.any():
            df.loc[mask, q] = df.loc[mask, q].apply(lambda x: ftfy.fix_text(str(x))[:1000])

print("✅ Data cleaning completed.")

# ==================== OPTIMIZED ETL LOGIC ====================
# Add metadata columns
df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df['NamHoc'] = SEMESTER
df['ProcessedDate'] = datetime.now()

# Create StudentKey efficiently
df['StudentKey'] = (
    df['Lop'].fillna('') + '|' +
    df['MaSV'].fillna('') + '|' +
    df['HoDem'].fillna('') + '|' +
    df['Ten'].fillna('') + '|' +
    df['NgaySinh'].fillna('')
)

# Generate IDs using pandas factorize (much faster)
unique_students = df['StudentKey'].unique()
student_id_map = pd.Series([f"SV{i+1:06d}" for i in range(len(unique_students))], index=unique_students)
df['ID'] = df['StudentKey'].map(student_id_map)

# Get basic info
df_basic = df.groupby('StudentKey', as_index=False).first()

# Optimized pivot for Q1-Q12
print("🔄 Pivoting Q1-Q12 (optimized)...")
df_questions = df[(df['CauHoi'] >= 1) & (df['CauHoi'] <= 12)][['StudentKey', 'CauHoi', 'DanhGia']].copy()

# Use pivot_table with numpy for speed
pivot_q = df_questions.pivot_table(
    index='StudentKey',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first',
    fill_value=np.nan
).reset_index()

# Rename columns efficiently
pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns[1:]]

# Merge using vectorized operations
df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')

# Add Q13-Q16 using vectorized mapping
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    if q in df.columns:
        q_dict = df.groupby('StudentKey')[q].first().to_dict()
        df_final[q] = df_final['StudentKey'].map(q_dict)

# Select final columns
final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
              'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
             [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc']

df_final = df_final[[c for c in final_cols if c in df_final.columns]].copy()
df_final = df_final.sort_values('ID').reset_index(drop=True)

# Convert NgaySinh in one operation
print("📅 Converting NgaySinh to datetime...")
df_final['NgaySinh'] = pd.to_datetime(df_final['NgaySinh'], format='%d/%m/%Y', errors='coerce')

print(f"\n🎉 Final dataset: {len(df_final):,} students, {len(df_final.columns)} columns")

# ==================== PARALLEL SAVE & UPLOAD ====================
def save_to_csv():
    local_filename = f"{SURVEY_FILE.replace('.txt','').replace('.csv','')}_processed.csv"
    df_final.to_csv(local_filename, index=False, encoding='utf-8-sig')
    return local_filename

def upload_to_azure():
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.txt','').replace('.csv','')}_processed.csv"
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(
        df_final.to_csv(index=False, encoding='utf-8-sig'), 
        overwrite=True
    )
    return output_path

# Parallel execution for save and upload
print("📤 Saving and uploading in parallel...")
with ThreadPoolExecutor(max_workers=2) as executor:
    save_future = executor.submit(save_to_csv)
    upload_future = executor.submit(upload_to_azure)
    
    local_file = save_future.result()
    blob_path = upload_future.result()
    
print(f"✅ Saved locally: {local_file}")
print(f"✅ Uploaded to: processed-data/{blob_path}")

# ==================== BULK SQL LOAD ====================
print("\n🔄 Loading data into Azure SQL (bulk optimized)...")

sql_server = "course-survey.database.windows.net"
sql_db     = "course-survey-db"
sql_user   = "sqladmin"
sql_pass   = "Due@2026"

# Optimized connection string with longer timeout
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={sql_server};DATABASE={sql_db};"
    f"UID={sql_user};PWD={sql_pass};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=300;"
)

# Use fast_executemany for better performance
engine = sa.create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}", 
    fast_executemany=True,
    pool_size=10,
    max_overflow=20
)

try:
    with engine.begin() as conn:
        
        # Prepare all dataframes first
        print("   - Preparing data for SQL...")
        
        # SINH_VIEN
        sv_cols = ['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']
        df_sv = df_final[[c for c in sv_cols if c in df_final.columns]].drop_duplicates(subset=['ID']).dropna(subset=['ID'])
        
        # HOC_PHAN
        df_hp = df_final[['MaHP', 'TenHP']].drop_duplicates(subset=['MaHP']).dropna(subset=['MaHP'])
        
        # GIANG_VIEN
        df_gv = df_final[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).dropna(subset=['MaGV'])
        
        # LOP_HOC_PHAN
        lhp = df_final[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
        lhp = lhp.rename(columns={'LopHP': 'MaLopHP'})
        lhp['TenLopHP'] = lhp['MaLopHP']
        lhp = lhp.drop_duplicates(subset=['MaLopHP']).dropna(subset=['MaLopHP'])
        
        # PHIEU_KHAO_SAT
        fact_cols = ['ID', 'LopHP', 'HocKy', 'NamHoc']
        for i in range(1, 17):
            q = f'Q{i}'
            if q in df_final.columns:
                fact_cols.append(q)
        df_fact = df_final[fact_cols].copy()
        df_fact = df_fact.rename(columns={'ID': 'ID_SV', 'LopHP': 'MaLopHP'})
        df_fact = df_fact.dropna(subset=['ID_SV', 'MaLopHP'])
        
        # Insert with larger chunks for better performance
        print("   - Inserting SINH_VIEN...")
        df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False, chunksize=10000, method='multi')
        
        print("   - Inserting HOC_PHAN...")
        df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False, chunksize=5000, method='multi')
        
        print("   - Inserting GIANG_VIEN...")
        df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False, chunksize=5000, method='multi')
        
        print("   - Inserting LOP_HOC_PHAN...")
        lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False, chunksize=10000, method='multi')
        
        print("   - Inserting PHIEU_KHAO_SAT...")
        df_fact.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False, chunksize=10000, method='multi')
        
    print("✅ All data successfully loaded into Azure SQL Database!")

except Exception as e:
    print(f"❌ SQL Load Error: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n🎯 FULL PIPELINE COMPLETED SUCCESSFULLY!")
print(f"⏱️ Total processing time: {datetime.now()}")
