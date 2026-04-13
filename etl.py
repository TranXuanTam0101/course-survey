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

def clean_text(text):
    """Sửa lỗi encoding tiếng Việt"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    if '?' in text:
        text = ftfy.fix_text(text)
    return text.strip()

def convert_masv(value):
    """Chuyển 91122E+11 -> số"""
    if not value or value == '':
        return None
    try:
        return str(int(float(value)))
    except:
        return value

try:
    # ==================== 1. ĐỌC FILE OPTIMIZED ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. ĐỌC CSV OPTIMIZED ====================
    print("📊 Reading CSV...")
    
    # Đọc toàn bộ với dtype tối ưu
    df = pd.read_csv(
        io.BytesIO(data),
        sep='\t',
        header=None,
        dtype=str,
        encoding='cp1258',
        low_memory=False
    )
    
    print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")
    
    # ==================== 3-9. XỬ LÝ THÔNG TIN SINH VIÊN ====================
    # Tìm vị trí các cột quan trọng một lần
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    
    # Vectorized operations thay vì loop
    df['Lop'] = df[0].astype(str).str.split(' ').str[0]
    
    df['MaSV_raw'] = df[1].astype(str).str.split(' ').str[0]
    df['MaSV'] = df['MaSV_raw'].apply(convert_masv)
    
    # Tìm cột ngày sinh
    ngaysinh_col = None
    for col in range(2, min(10, len(df.columns))):
        if df[col].astype(str).str.match(date_pattern, na=False).any():
            df['NgaySinh'] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            ngaysinh_col = col
            break
    
    if ngaysinh_col is None:
        df['NgaySinh'] = None
        ngaysinh_col = 2
    
    # Xử lý họ tên SV vectorized
    if ngaysinh_col > 2:
        hoten_sv = df[list(range(2, ngaysinh_col))].astype(str).agg(' '.join, axis=1)
        name_parts = hoten_sv.str.split()
        df['Ten'] = name_parts.str[-1].apply(clean_text)
        df['HoDem'] = name_parts.str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['Ten'] = None
        df['HoDem'] = None
    
    # Mã HP
    maHP_col = ngaysinh_col + 1 if ngaysinh_col is not None else 5
    df['MaHP'] = df[maHP_col] if maHP_col < len(df.columns) else None
    
    # Tìm mã GV
    magv_col = None
    for col in range(maHP_col + 1, min(maHP_col + 10, len(df.columns))):
        if df[col].astype(str).str.match(r'^\d+$', na=False).any():
            magv_col = col
            break
    
    # Tên HP
    if magv_col is not None and magv_col > maHP_col + 1:
        tenhp_cols = list(range(maHP_col + 1, magv_col))
        df['TenHP'] = df[tenhp_cols].astype(str).agg(' '.join, axis=1).apply(clean_text)
    else:
        df['TenHP'] = None
    
    # Mã GV
    df['MaGV'] = df[magv_col] if magv_col is not None else None
    
    # Tìm LopHP
    lophp_col = None
    if magv_col is not None:
        for col in range(magv_col + 1, min(magv_col + 10, len(df.columns))):
            if df[col].astype(str).str.contains('_', na=False).any():
                lophp_col = col
                break
    
    # Tên GV
    if lophp_col is not None and lophp_col > magv_col + 1:
        hoten_gv_cols = list(range(magv_col + 1, lophp_col))
        hoten_gv = df[hoten_gv_cols].astype(str).agg(' '.join, axis=1)
        gv_parts = hoten_gv.str.split()
        df['TenGV'] = gv_parts.str[-1].apply(clean_text)
        df['HoDemGV'] = gv_parts.str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['TenGV'] = None
        df['HoDemGV'] = None
    
    # LopHP
    df['LopHP'] = df[lophp_col] if lophp_col is not None else None
    
    # ==================== 10. CÂU HỎI VÀ ĐÁNH GIÁ ====================
    cauhoi_col = lophp_col + 1 if lophp_col is not None else 11
    df['CauHoi'] = pd.to_numeric(df[cauhoi_col], errors='coerce') if cauhoi_col < len(df.columns) else None
    
    danhgia_col = cauhoi_col + 1
    df['DanhGia'] = pd.to_numeric(df[danhgia_col], errors='coerce') if danhgia_col < len(df.columns) else None
    
    # ==================== 11. XỬ LÝ CÂU HỎI Q1-Q16 ====================
    # Chuyển FB1->FB4 thành Q13->Q16
    fb_start = 14
    
    # Tạo dictionary cho mapping câu hỏi
    question_cols = {}
    for i in range(1, 13):  # Q1-Q12 từ các dòng CauHoi khác nhau
        question_cols[f'Q{i}'] = None
    
    # Q13-Q16 từ FB1-FB4
    if fb_start < len(df.columns):
        df['Q13'] = df[fb_start].apply(clean_text)
    if fb_start + 1 < len(df.columns):
        df['Q14'] = df[fb_start + 1].apply(clean_text)
    if fb_start + 2 < len(df.columns):
        df['Q15'] = df[fb_start + 2].apply(clean_text)
    if fb_start + 3 < len(df.columns):
        df['Q16'] = df[fb_start + 3].apply(clean_text)
    
    # ==================== 12. THÊM METADATA ====================
    df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df['NamHoc'] = SEMESTER
    df['ProcessedDate'] = datetime.now()
    
    # ==================== 13. TẠO ID DUY NHẤT VÀ GỘP DÒNG ====================
    print("🔄 Grouping by student (12 questions per student)...")
    
    # Tạo ID duy nhất cho mỗi sinh viên dựa trên Lop, MaSV, HoDem, Ten, NgaySinh, LopHP
    df['StudentKey'] = df['Lop'].fillna('') + '|' + \
                       df['MaSV'].fillna('') + '|' + \
                       df['HoDem'].fillna('') + '|' + \
                       df['Ten'].fillna('') + '|' + \
                       df['NgaySinh'].astype(str).fillna('') + '|' + \
                       df['LopHP'].fillna('')
    
    # Tạo ID cho mỗi sinh viên (hash hoặc sequential)
    unique_students = df['StudentKey'].unique()
    student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
    df['ID'] = df['StudentKey'].map(student_id_map)
    
    # Pivot để chuyển 12 dòng CauHoi thành 12 cột Q1-Q12
    # Chỉ lấy các dòng có CauHoi từ 1-12
    df_questions = df[df['CauHoi'].between(1, 12)].copy()
    
    # Tạo pivot table cho câu hỏi
    if len(df_questions) > 0:
        pivot_q = df_questions.pivot_table(
            index='StudentKey',
            columns='CauHoi',
            values='DanhGia',
            aggfunc='first'
        ).reset_index()
        
        # Đổi tên cột thành Q1-Q12
        pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns if col != 'StudentKey']
        
        # Merge với dữ liệu cơ bản (lấy 1 dòng đại diện cho mỗi student)
        df_basic = df.groupby('StudentKey').first().reset_index()
        
        # Merge tất cả lại
        df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')
        
        # Thêm Q13-Q16 (các cột này giống nhau cho tất cả dòng của cùng student)
        for q in ['Q13', 'Q14', 'Q15', 'Q16']:
            if q in df.columns:
                q_values = df.groupby('StudentKey')[q].first()
                df_final[q] = df_final['StudentKey'].map(q_values)
    else:
        df_final = df.groupby('StudentKey').first().reset_index()
        # Khởi tạo các cột Q1-Q12 rỗng
        for i in range(1, 13):
            df_final[f'Q{i}'] = None
    
    # Điền giá trị mặc định cho Q1-Q12 nếu thiếu
    for i in range(1, 13):
        if f'Q{i}' not in df_final.columns:
            df_final[f'Q{i}'] = None
    
    # ==================== 14. CHỌN CỘT THEO YÊU CẦU ====================
    final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
                 [f'Q{i}' for i in range(1, 17)] + \
                 ['HocKy', 'NamHoc']
    
    # Chỉ lấy các cột tồn tại
    final_cols_existing = [c for c in final_cols if c in df_final.columns]
    df_final = df_final[final_cols_existing]
    
    
    # ==================== 15. UPLOAD ====================
    print("📤 Uploading to Azure...")
    output = df_final.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 16. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ SUCCESS!")
    print(f"📊 Original rows: {len(df):,}")
    print(f"📊 Student rows after grouping: {len(df_final):,}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample (first 3 rows):")
    sample_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                   'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'Q1', 'Q2', 'Q3', 'Q13', 'Q14', 'Q15', 'Q16']
    sample_cols = [c for c in sample_cols if c in df_final.columns]
    print(df_final[sample_cols].head(3).to_string(index=False))
    
    print(f"\n📊 Statistics:")
    print(f"   - Students have Q1-Q12: {df_final[[f'Q{i}' for i in range(1,13)]].notna().any(axis=1).sum():,}")
    print(f"   - Students have Q13-Q16: {df_final[['Q13','Q14','Q15','Q16']].notna().any(axis=1).sum():,}")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
