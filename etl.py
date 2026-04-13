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
    
    # Tạo ID cho mỗi sinh viên
    unique_students = df['StudentKey'].unique()
    student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
    df['ID'] = df['StudentKey'].map(student_id_map)
    
    # Lấy thông tin cơ bản của mỗi student (1 dòng đại diện)
    df_basic = df.groupby('StudentKey').first().reset_index()
    
    # ==================== SỬA LỖI PIVOT CHO Q1-Q12 ====================
    # Lấy tất cả các dòng có CauHoi từ 1-12
    df_questions = df[df['CauHoi'].between(1, 12)].copy()
    
    # Tạo đầy đủ các cặp (StudentKey, CauHoi) cho tất cả student và 12 câu hỏi
    all_students = df_basic['StudentKey'].unique()
    all_questions = list(range(1, 13))
    
    # Tạo dataframe đầy đủ các combinations
    complete_combinations = []
    for student in all_students:
        for q in all_questions:
            complete_combinations.append({'StudentKey': student, 'CauHoi': q})
    
    df_complete = pd.DataFrame(complete_combinations)
    
    # Merge với dữ liệu có sẵn (giữ nguyên giá trị gốc, nếu thiếu thì để NaN)
    if len(df_questions) > 0:
        df_merged = df_complete.merge(
            df_questions[['StudentKey', 'CauHoi', 'DanhGia']], 
            on=['StudentKey', 'CauHoi'], 
            how='left'
        )
    else:
        df_merged = df_complete.copy()
        df_merged['DanhGia'] = None
    
    # Pivot để có 12 cột Q1-Q12
    pivot_q = df_merged.pivot_table(
        index='StudentKey',
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()
    
    # Đổi tên cột thành Q1-Q12
    pivot_q.columns = ['StudentKey'] + [f'Q{int(col)}' for col in pivot_q.columns if col != 'StudentKey']
    
    # Merge với thông tin cơ bản
    df_final = df_basic.merge(pivot_q, on='StudentKey', how='left')
    
    # Thêm Q13-Q16 (các cột này giống nhau cho tất cả dòng của cùng student)
    for q in ['Q13', 'Q14', 'Q15', 'Q16']:
        if q in df.columns:
            q_values = df.groupby('StudentKey')[q].first()
            df_final[q] = df_final['StudentKey'].map(q_values)
    
    # ==================== 14. CHỌN CỘT THEO YÊU CẦU ====================
    final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
                 [f'Q{i}' for i in range(1, 17)] + \
                 ['HocKy', 'NamHoc']
    
    # Chỉ lấy các cột tồn tại
    final_cols_existing = [c for c in final_cols if c in df_final.columns]
    df_final = df_final[final_cols_existing]
    
    # Sắp xếp theo ID
    df_final = df_final.sort_values('ID').reset_index(drop=True)
    
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
    
    # ==================== SỬA LỖI THỐNG KÊ ====================
    print(f"\n📊 Statistics:")
    # Đếm số sinh viên có ít nhất 1 câu trả lời (không null) cho Q1-Q12
    students_with_q1_q12 = 0
    students_with_q13_q16 = 0
    
    for idx, row in df_final.iterrows():
        # Kiểm tra Q1-Q12: có bất kỳ câu nào không null không
        has_any_q1_q12 = False
        for i in range(1, 13):
            val = row.get(f'Q{i}')
            if pd.notna(val) and val is not None and str(val).strip() != '':
                has_any_q1_q12 = True
                break
        
        # Kiểm tra Q13-Q16: có bất kỳ câu nào không null không
        has_any_q13_q16 = False
        for i in range(13, 17):
            val = row.get(f'Q{i}')
            if pd.notna(val) and val is not None and str(val).strip() != '':
                has_any_q13_q16 = True
                break
        
        if has_any_q1_q12:
            students_with_q1_q12 += 1
        if has_any_q13_q16:
            students_with_q13_q16 += 1
    
    print(f"   - Students have Q1-Q12: {students_with_q1_q12:,}")
    print(f"   - Students have Q13-Q16: {students_with_q13_q16:,}")
    
    # Cảnh báo nếu không bằng nhau
    if students_with_q1_q12 != students_with_q13_q16:
        print(f"   ⚠️ WARNING: Mismatch detected! Difference: {abs(students_with_q1_q12 - students_with_q13_q16)} students")
    else:
        print(f"   ✅ PERFECT: All students have consistent responses!")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
