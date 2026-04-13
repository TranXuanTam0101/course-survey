import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy

print("🚀 Starting ETL Pipeline (Optimized Version)...")

# Lấy environment variables
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)


def clean_text(text, max_len=None):
    """Sửa lỗi encoding tiếng Việt và làm sạch text"""
    if pd.isna(text) or str(text).strip() in ['', 'NULL', 'nan']:
        return None
    text = str(text).strip()
    if text.lower() == 'nan':
        return None
    
    # Sửa lỗi encoding tiếng Việt
    text = ftfy.fix_text(text)
    text = text.strip()
    
    if max_len and len(text) > max_len:
        text = text[:max_len]
    return text if text else None


def convert_masv(value):
    """Chuyển mã sinh viên dạng 1,91122E+11 thành string"""
    if pd.isna(value) or str(value).strip() in ['', 'NULL']:
        return None
    try:
        return str(int(float(str(value).replace(',', ''))))
    except:
        return str(value).strip()


# ==================== KẾT NỐI AZURE ====================
print("📥 Connecting to Azure Storage...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)

# Download raw file
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
data = blob_client.download_blob().readall()
print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")

# ==================== ĐỌC FILE ====================
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

# ==================== GÁN TÊN CỘT ====================
print("🔖 Assigning column names...")
df.columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
    'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
    'LopHP', 'CauHoi', 'DanhGia', 'Col13',
    'Q13', 'Q14', 'Q15', 'Q16',
    'Col18', 'Col19', 'Col20', 'Col21', 'Col22',
    'Col23', 'Col24', 'Col25', 'Col26', 'Col27',
    'Col28', 'Col29', 'Col30', 'Col31'
]

# ==================== LÀM SẠCH DỮ LIỆU ====================
print("🧹 Cleaning and fixing Vietnamese text...")

df['Lop']      = df['Lop'].apply(clean_text)
df['MaSV']     = df['MaSV'].apply(convert_masv)
df['HoDem']    = df['HoDem'].apply(clean_text)
df['Ten']      = df['Ten'].apply(clean_text)
df['NgaySinh'] = df['NgaySinh'].apply(clean_text)
df['MaHP']     = df['MaHP'].apply(clean_text)
df['TenHP']    = df['TenHP'].apply(clean_text)
df['MaGV']     = df['MaGV'].apply(clean_text)
df['HoDemGV']  = df['HoDemGV'].apply(clean_text)
df['TenGV']    = df['TenGV'].apply(clean_text)
df['LopHP']    = df['LopHP'].apply(clean_text)

df['CauHoi']   = pd.to_numeric(df['CauHoi'], errors='coerce')
df['DanhGia']  = pd.to_numeric(df['DanhGia'], errors='coerce')

for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    df[q] = df[q].apply(lambda x: clean_text(x, max_len=1000))

print("✅ Data cleaning completed.")

# ==================== XỬ LÝ LOGIC ETL ====================
df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df['NamHoc'] = SEMESTER
df['ProcessedDate'] = datetime.now()

df['StudentKey'] = (
    df['Lop'].fillna('') + '|' +
    df['MaSV'].fillna('') + '|' +
    df['HoDem'].fillna('') + '|' +
    df['Ten'].fillna('') + '|' +
    df['NgaySinh'].fillna('')
)

unique_students = df['StudentKey'].unique()
student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
df['ID'] = df['StudentKey'].map(student_id_map)

df_basic = df.groupby('StudentKey').first().reset_index()

# Pivot Q1-Q12
print("🔄 Pivoting Q1-Q12...")
df_questions = df[df['CauHoi'].between(1, 12)].copy()

all_students = df_basic['StudentKey'].unique()
complete_combinations = pd.DataFrame([
    {'StudentKey': s, 'CauHoi': q} for s in all_students for q in range(1, 13)
])

df_merged = complete_combinations.merge(
    df_questions[['StudentKey', 'CauHoi', 'DanhGia']],
    on=['StudentKey', 'CauHoi'], how='left'
)

pivot_q = df_merged.pivot_table(
    index='StudentKey',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()

pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns if col != 'StudentKey']

df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')

# Thêm Q13-Q16
for q in ['Q13', 'Q14', 'Q15', 'Q16']:
    if q in df.columns:
        q_values = df.groupby('StudentKey')[q].first()
        df_final[q] = df_final['StudentKey'].map(q_values)

# ==================== CHỌN CỘT CUỐI CÙNG ====================
final_cols = [
    'ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh',
    'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'
] + [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc']

final_cols_existing = [c for c in final_cols if c in df_final.columns]
df_final = df_final[final_cols_existing].copy()
df_final = df_final.sort_values('ID').reset_index(drop=True)

print(f"\n🎉 Hoàn tất xử lý! Final dataset: {len(df_final):,} sinh viên, {len(df_final.columns)} cột")

# ==================== XUẤT FILE KẾT QUẢ ====================
print("💾 Saving processed file...")

# 1. Lưu file local (để kiểm tra)
local_filename = f"{SURVEY_FILE.replace('.txt','').replace('.csv','')}_processed.csv"
df_final.to_csv(local_filename, index=False, encoding='utf-8-sig')
print(f"✅ Saved locally: {local_filename}")

# 2. Upload lên Azure Blob
print("📤 Uploading to Azure Storage...")
output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.txt','').replace('.csv','')}_processed.csv"

processed_container = blob_service.get_container_client("processed-data")
if not processed_container.exists():
    processed_container.create_container()
    print("✅ Created container: processed-data")

# Upload
processed_container.get_blob_client(output_path).upload_blob(
    df_final.to_csv(index=False, encoding='utf-8-sig'), 
    overwrite=True
)

print(f"✅ Uploaded successfully to: processed-data/{output_path}")
print("🎯 ETL Pipeline completed successfully!")
