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
    """Sửa lỗi encoding tiếng Việt và xử lý độ dài"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    
    # Xử lý string 'nan' (phòng trường hợp)
    if text.lower() == 'nan':
        return None
    
    # Sửa lỗi encoding
    if '?' in text:
        text = ftfy.fix_text(text)
    
    # Cắt khoảng trắng
    text = text.strip()
    
    # Cắt ngắn nếu có max_len
    if max_len and len(text) > max_len:
        text = text[:max_len]
    
    return text if text else None

def safe_join_no_nan(df_columns):
    """Join các cột nhưng loại bỏ giá trị NaN/None trước khi join"""
    def join_row(row):
        # Lọc bỏ các giá trị NaN, None, 'nan', '' 
        valid_values = []
        for val in row:
            if pd.notna(val) and val is not None:
                val_str = str(val).strip()
                if val_str and val_str.lower() != 'nan':
                    valid_values.append(val_str)
        return ' '.join(valid_values) if valid_values else None
    return df_columns.apply(join_row, axis=1)

def convert_masv(value):
    """Chuyển 91122E+11 -> số"""
    if not value or value == '':
        return None
    try:
        return str(int(float(value)))
    except:
        return value

try:
    # ==================== 1. ĐỌC FILE ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. ĐỌC CSV ====================
    print("📊 Reading CSV...")
    
    df = pd.read_csv(
        io.BytesIO(data),
        sep='\t',
        header=None,
        dtype=str,
        encoding='cp1258',
        low_memory=False
    )
    
    print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")
    
    # ==================== 3. XỬ LÝ THÔNG TIN SINH VIÊN ====================
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    
    # Lớp
    df['Lop'] = df[0].astype(str).str.split(' ').str[0]
    df['Lop'] = df['Lop'].apply(lambda x: clean_text(x, max_len=50))
    
    # Mã SV
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
    
    # Xử lý họ tên SV (loại bỏ NaN trước khi join)
    if ngaysinh_col > 2:
        hoten_sv_cols = list(range(2, ngaysinh_col))
        hoten_sv = safe_join_no_nan(df[hoten_sv_cols])
        
        # Tách họ đệm và tên
        df['Ten'] = None
        df['HoDem'] = None
        
        for idx, fullname in hoten_sv.items():
            if fullname:
                name_parts = fullname.split()
                if len(name_parts) > 0:
                    df.at[idx, 'Ten'] = clean_text(name_parts[-1], max_len=50)
                    if len(name_parts) > 1:
                        df.at[idx, 'HoDem'] = clean_text(' '.join(name_parts[:-1]), max_len=100)
    else:
        df['Ten'] = None
        df['HoDem'] = None
    
    # Mã HP
    maHP_col = ngaysinh_col + 1 if ngaysinh_col is not None else 5
    df['MaHP'] = df[maHP_col].apply(lambda x: clean_text(x, max_len=20)) if maHP_col < len(df.columns) else None
    
    # Tìm mã GV
    magv_col = None
    for col in range(maHP_col + 1, min(maHP_col + 10, len(df.columns))):
        if df[col].astype(str).str.match(r'^\d+$', na=False).any():
            magv_col = col
            break
    
    # Tên HP (loại bỏ NaN trước khi join)
    if magv_col is not None and magv_col > maHP_col + 1:
        tenhp_cols = list(range(maHP_col + 1, magv_col))
        df['TenHP'] = safe_join_no_nan(df[tenhp_cols])
        df['TenHP'] = df['TenHP'].apply(lambda x: clean_text(x, max_len=200))
    else:
        df['TenHP'] = None
    
    # Mã GV
    df['MaGV'] = df[magv_col].apply(lambda x: clean_text(x, max_len=20)) if magv_col is not None else None
    
    # Tìm LopHP
    lophp_col = None
    if magv_col is not None:
        for col in range(magv_col + 1, min(magv_col + 20, len(df.columns))):
            if df[col].isna().all() or (df[col].astype(str).str.strip().isin(['', 'NULL', 'nan', 'None']).all()):
                lophp_col = col - 1
                print(f"   ℹ️ Found NULL column at position {col}, using LopHP from column {lophp_col}")
                break
    
    # Tên GV (xử lý ĐÚNG - không tạo ra string 'nan')
    if lophp_col is not None and lophp_col > magv_col + 1:
        hoten_gv_cols = list(range(magv_col + 1, lophp_col))
        
        # SỬA QUAN TRỌNG: Loại bỏ NaN trước khi join
        hoten_gv = safe_join_no_nan(df[hoten_gv_cols])
        
        # Tách họ đệm và tên GV
        df['TenGV'] = None
        df['HoDemGV'] = None
        
        for idx, fullname in hoten_gv.items():
            if fullname:
                name_parts = fullname.split()
                if len(name_parts) > 0:
                    df.at[idx, 'TenGV'] = clean_text(name_parts[-1], max_len=50)
                    if len(name_parts) > 1:
                        df.at[idx, 'HoDemGV'] = clean_text(' '.join(name_parts[:-1]), max_len=100)
    else:
        df['TenGV'] = None
        df['HoDemGV'] = None
    
    # LopHP
    df['LopHP'] = df[lophp_col].apply(lambda x: clean_text(x, max_len=50)) if lophp_col is not None else None
    
    # ==================== 4. CÂU HỎI VÀ ĐÁNH GIÁ ====================
    cauhoi_col = lophp_col + 1 if lophp_col is not None else 11
    df['CauHoi'] = pd.to_numeric(df[cauhoi_col], errors='coerce') if cauhoi_col < len(df.columns) else None
    
    danhgia_col = cauhoi_col + 1
    df['DanhGia'] = pd.to_numeric(df[danhgia_col], errors='coerce') if danhgia_col < len(df.columns) else None
    
    # ==================== 5. XỬ LÝ CÂU HỎI Q13-Q16 ====================
    fb_start = 14
    
    if fb_start < len(df.columns):
        df['Q13'] = df[fb_start].apply(lambda x: clean_text(x, max_len=500))
    if fb_start + 1 < len(df.columns):
        df['Q14'] = df[fb_start + 1].apply(lambda x: clean_text(x, max_len=500))
    if fb_start + 2 < len(df.columns):
        df['Q15'] = df[fb_start + 2].apply(lambda x: clean_text(x, max_len=500))
    if fb_start + 3 < len(df.columns):
        df['Q16'] = df[fb_start + 3].apply(lambda x: clean_text(x, max_len=500))
    
    # ==================== 6. THÊM METADATA ====================
    df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df['NamHoc'] = SEMESTER
    df['ProcessedDate'] = datetime.now()
    
    # ==================== 7. TẠO ID DUY NHẤT ====================
    print("🔄 Grouping by student (12 questions per student)...")
    
    # Tạo StudentKey
    df['StudentKey'] = (
        df['Lop'].fillna('') + '|' +
        df['MaSV'].fillna('') + '|' +
        df['HoDem'].fillna('') + '|' +
        df['Ten'].fillna('') + '|' +
        df['NgaySinh'].astype(str).fillna('')
    )
    
    # Tạo ID
    unique_students = df['StudentKey'].unique()
    student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
    df['ID'] = df['StudentKey'].map(student_id_map)
    
    # Lấy thông tin cơ bản
    df_basic = df.groupby('StudentKey').first().reset_index()
    
    # ==================== 8. PIVOT CHO Q1-Q12 ====================
    df_questions = df[df['CauHoi'].between(1, 12)].copy()
    
    all_students = df_basic['StudentKey'].unique()
    all_questions = list(range(1, 13))
    
    complete_combinations = []
    for student in all_students:
        for q in all_questions:
            complete_combinations.append({'StudentKey': student, 'CauHoi': q})
    
    df_complete = pd.DataFrame(complete_combinations)
    
    if len(df_questions) > 0:
        df_merged = df_complete.merge(
            df_questions[['StudentKey', 'CauHoi', 'DanhGia']], 
            on=['StudentKey', 'CauHoi'], 
            how='left'
        )
    else:
        df_merged = df_complete.copy()
        df_merged['DanhGia'] = None
    
    pivot_q = df_merged.pivot_table(
        index='StudentKey',
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()
    
    pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns if col != 'StudentKey']
    
    # Merge
    df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')
    
    # Thêm Q13-Q16
    for q in ['Q13', 'Q14', 'Q15', 'Q16']:
        if q in df.columns:
            q_values = df.groupby('StudentKey')[q].first()
            df_final[q] = df_final['StudentKey'].map(q_values)
    
    # ==================== 9. CHỌN CỘT ====================
    final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
                 [f'Q{i}' for i in range(1, 17)] + \
                 ['HocKy', 'NamHoc']
    
    final_cols_existing = [c for c in final_cols if c in df_final.columns]
    df_final = df_final[final_cols_existing]
    df_final = df_final.sort_values('ID').reset_index(drop=True)
    
    # ==================== 10. KIỂM TRA TRƯỚC KHI LƯU ====================
    # Kiểm tra xem còn string 'nan' nào không
    print("\n🔍 Checking for 'nan' strings in final data...")
    for col in ['HoDemGV', 'TenGV']:
        if col in df_final.columns:
            nan_count = df_final[col].astype(str).str.lower().eq('nan').sum()
            if nan_count > 0:
                print(f"   ⚠️ Found {nan_count} 'nan' strings in {col}, converting to None")
                df_final[col] = df_final[col].apply(lambda x: None if pd.notna(x) and str(x).lower() == 'nan' else x)
    
    # Kiểm tra độ dài
    print("\n📏 Checking max length for GIANG_VIEN columns:")
    if 'HoDemGV' in df_final.columns:
        max_len = df_final['HoDemGV'].dropna().astype(str).str.len().max() if len(df_final['HoDemGV'].dropna()) > 0 else 0
        print(f"   - HoDemGV max length: {max_len}")
        if max_len > 100:
            print(f"   ⚠️ WARNING: HoDemGV exceeds 100 chars! Will truncate in SQL loader")
    
    if 'TenGV' in df_final.columns:
        max_len = df_final['TenGV'].dropna().astype(str).str.len().max() if len(df_final['TenGV'].dropna()) > 0 else 0
        print(f"   - TenGV max length: {max_len}")
        if max_len > 50:
            print(f"   ⚠️ WARNING: TenGV exceeds 50 chars! Will truncate in SQL loader")
    
    # ==================== 11. UPLOAD ====================
    print("\n📤 Uploading to Azure...")
    output = df_final.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # Lưu local để SQL loader dùng
    df_final.to_csv("processed_data_temp.csv", index=False, encoding='utf-8-sig')
    
    # ==================== 12. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ SUCCESS!")
    print(f"📊 Original rows: {len(df):,}")
    print(f"📊 Student rows after grouping: {len(df_final):,}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    print(f"💾 Saved to: processed_data_temp.csv")
    
    print(f"\n📋 Sample (first 3 rows):")
    sample_cols = ['ID', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP']
    sample_cols = [c for c in sample_cols if c in df_final.columns]
    print(df_final[sample_cols].head(3).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
