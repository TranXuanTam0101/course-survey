import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re
import numpy as np

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
    if pd.isna(text) or text in ['NULL', '', 'nan', None]:
        return None
    text = str(text).strip()
    
    if text.lower() == 'nan':
        return None
    
    # Sửa lỗi encoding tiếng Việt bằng ftfy
    if '?' in text or any(ord(c) > 127 and ord(c) < 256 for c in text):
        text = ftfy.fix_text(text)
    
    text = text.strip()
    
    if max_len and len(text) > max_len:
        text = text[:max_len]
    
    return text if text else None


def convert_masv(value):
    """Chuyển đổi mã sinh viên dạng 1.91122E+11 thành string bình thường"""
    if pd.isna(value) or value in ['', 'NULL']:
        return None
    try:
        # Xử lý scientific notation
        return str(int(float(str(value).replace(',', ''))))
    except:
        return str(value).strip()


print("📥 Connecting to Azure Storage...")
blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

data = blob_client.download_blob().readall()
print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")

# ==================== ĐỌC FILE (rất quan trọng với encoding) ====================
print("📊 Reading CSV...")

df = pd.read_csv(
    io.BytesIO(data),
    sep='\t',           # Dữ liệu của bạn dùng tab
    header=None,
    dtype=str,
    encoding='cp1258',  # Windows-1258 thường dùng cho tiếng Việt cũ
    on_bad_lines='skip',
    low_memory=False
)

print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")

# ==================== XÁC ĐỊNH VỊ TRÍ CÁC CỘT ====================
# Dựa trên dữ liệu mẫu bạn đưa ra
col_names = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
    'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 
    'LopHP', 'CauHoi', 'DanhGia', 'NULL1', 'Q13', 'Q14', 'Q15', 'Q16'
]

# Gán tên cột (nếu số cột không khớp sẽ tự điều chỉnh)
if len(df.columns) >= len(col_names):
    df.columns = col_names[:len(df.columns)]
else:
    df.columns = [f'Col_{i}' for i in range(len(df.columns))]

print(f"Columns after naming: {df.columns.tolist()}")

# ==================== LÀM SẠCH DỮ LIỆU ====================
print("🧹 Cleaning data...")

df['Lop'] = df['Lop'].apply(clean_text)
df['MaSV'] = df['MaSV'].apply(convert_masv)
df['HoDem'] = df['HoDem'].apply(clean_text)
df['Ten'] = df['Ten'].apply(clean_text)
df['NgaySinh'] = df['NgaySinh'].apply(clean_text)
df['MaHP'] = df['MaHP'].apply(clean_text)
df['TenHP'] = df['TenHP'].apply(clean_text)
df['MaGV'] = df['MaGV'].apply(clean_text)
df['HoDemGV'] = df['HoDemGV'].apply(clean_text)
df['TenGV'] = df['TenGV'].apply(clean_text)
df['LopHP'] = df['LopHP'].apply(clean_text)

# Câu hỏi và đánh giá
df['CauHoi'] = pd.to_numeric(df.get('CauHoi'), errors='coerce')
df['DanhGia'] = pd.to_numeric(df.get('DanhGia'), errors='coerce')

# Feedback Q13 - Q16
for i, q in enumerate(['Q13', 'Q14', 'Q15', 'Q16'], start=14):
    if q in df.columns:
        df[q] = df[q].apply(lambda x: clean_text(x, max_len=1000))

# ==================== THÊM METADATA ====================
df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
df['NamHoc'] = SEMESTER
df['ProcessedDate'] = datetime.now()

# ==================== TẠO KHÓA SINH VIÊN ====================
df['StudentKey'] = (
    df['Lop'].fillna('') + '|' +
    df['MaSV'].fillna('') + '|' +
    df['HoDem'].fillna('') + '|' +
    df['Ten'].fillna('') + '|' +
    df['NgaySinh'].fillna('')
)

# Tạo ID sinh viên duy nhất
unique_students = df['StudentKey'].unique()
student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
df['ID'] = df['StudentKey'].map(student_id_map)

# Lấy thông tin cơ bản của sinh viên (lấy dòng đầu tiên)
df_basic = df.groupby('StudentKey').first().reset_index()

# ==================== PIVOT CÂU HỎI 1-12 ====================
print("🔄 Pivoting questions Q1-Q12...")

df_questions = df[df['CauHoi'].between(1, 12)].copy()

# Tạo tất cả các tổ hợp sinh viên - câu hỏi (để đảm bảo đủ 12 câu)
all_students = df_basic['StudentKey'].unique()
complete_combinations = pd.DataFrame([
    {'StudentKey': student, 'CauHoi': q} 
    for student in all_students 
    for q in range(1, 13)
])

if len(df_questions) > 0:
    df_merged = complete_combinations.merge(
        df_questions[['StudentKey', 'CauHoi', 'DanhGia']], 
        on=['StudentKey', 'CauHoi'], 
        how='left'
    )
else:
    df_merged = complete_combinations.copy()
    df_merged['DanhGia'] = None

# Pivot
pivot_q = df_merged.pivot_table(
    index='StudentKey',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()

pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns if col != 'StudentKey']

# Merge với thông tin cơ bản
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
] + [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc', 'ProcessedDate']

final_cols_existing = [c for c in final_cols if c in df_final.columns]
df_final = df_final[final_cols_existing]

df_final = df_final.sort_values('ID').reset_index(drop=True)

print(f"✅ Final dataset: {len(df_final):,} students, {len(df_final.columns)} columns")
print(df_final.head())

# Nếu muốn lưu để kiểm tra:
# df_final.to_excel(f"processed_{SURVEY_FILE.replace('.txt','.xlsx')}", index=False)
