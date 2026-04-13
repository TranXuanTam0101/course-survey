import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re
import numpy as np
import pyodbc
from sqlalchemy import create_engine, text
import urllib

print("🚀 Starting ETL Pipeline (Optimized Version)...")

# Lấy environment variables
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

# SQL Azure connection variables
SQL_SERVER = os.environ.get("course-survey.database.windows.net")
SQL_DATABASE = os.environ.get("course-survey-db")
SQL_USERNAME = os.environ.get("dqladmin")
SQL_PASSWORD = os.environ.get("Due@2026")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables for Blob Storage!")
    sys.exit(1)

if not SQL_SERVER or not SQL_DATABASE or not SQL_USERNAME or not SQL_PASSWORD:
    print("❌ Missing required environment variables for SQL Azure!")
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

def create_sql_engine():
    """Tạo kết nối đến SQL Azure"""
    try:
        connection_string = f"mssql+pyodbc://{SQL_USERNAME}:{SQL_PASSWORD}@{SQL_SERVER}:1433/{SQL_DATABASE}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
        engine = create_engine(connection_string)
        return engine
    except Exception as e:
        print(f"❌ Failed to create SQL connection: {str(e)}")
        return None

def insert_into_sql(df_final, engine):
    """Chèn dữ liệu vào các bảng SQL Azure"""
    print("\n📊 Inserting data into SQL Azure...")
    
    try:
        # 1. Chuẩn bị dữ liệu cho bảng SINH_VIEN
        sinh_vien_data = df_final[['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].copy()
        sinh_vien_data = sinh_vien_data.drop_duplicates(subset=['ID'])
        sinh_vien_data['NgaySinh'] = pd.to_datetime(sinh_vien_data['NgaySinh'], errors='coerce').dt.date
        
        # 2. Chuẩn bị dữ liệu cho bảng HOC_PHAN
        hoc_phan_data = df_final[['MaHP', 'TenHP']].copy()
        hoc_phan_data = hoc_phan_data.drop_duplicates(subset=['MaHP'])
        hoc_phan_data = hoc_phan_data[hoc_phan_data['MaHP'].notna()]
        
        # 3. Chuẩn bị dữ liệu cho bảng GIANG_VIEN
        giang_vien_data = df_final[['MaGV', 'HoDemGV', 'TenGV']].copy()
        giang_vien_data = giang_vien_data.drop_duplicates(subset=['MaGV'])
        giang_vien_data = giang_vien_data[giang_vien_data['MaGV'].notna()]
        
        # 4. Chuẩn bị dữ liệu cho bảng LOP_HOC_PHAN
        lop_hoc_phan_data = df_final[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
        lop_hoc_phan_data = lop_hoc_phan_data.drop_duplicates(subset=['LopHP'])
        lop_hoc_phan_data = lop_hoc_phan_data[lop_hoc_phan_data['LopHP'].notna()]
        lop_hoc_phan_data['TenLopHP'] = lop_hoc_phan_data['LopHP']  # Tên lớp HP = Mã lớp HP
        
        # 5. Chuẩn bị dữ liệu cho bảng PHIEU_KHAO_SAT
        phieu_khao_sat_data = df_final[['ID', 'LopHP'] + [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc', 'ProcessedDate']].copy()
        phieu_khao_sat_data = phieu_khao_sat_data.rename(columns={'ID': 'ID_SV', 'LopHP': 'MaLopHP'})
        
        # Chuyển đổi kiểu dữ liệu cho Q1-Q12 sang float
        for i in range(1, 13):
            phieu_khao_sat_data[f'Q{i}'] = pd.to_numeric(phieu_khao_sat_data[f'Q{i}'], errors='coerce')
        
        # Thêm ProcessedDate nếu chưa có
        if 'ProcessedDate' not in phieu_khao_sat_data.columns:
            phieu_khao_sat_data['ProcessedDate'] = datetime.now()
        
        # Bắt đầu transaction
        with engine.begin() as connection:
            # Xóa dữ liệu cũ (tùy chọn - comment nếu muốn giữ lại)
            print("   Clearing old data...")
            connection.execute(text("DELETE FROM PHIEU_KHAO_SAT"))
            connection.execute(text("DELETE FROM LOP_HOC_PHAN"))
            connection.execute(text("DELETE FROM GIANG_VIEN"))
            connection.execute(text("DELETE FROM HOC_PHAN"))
            connection.execute(text("DELETE FROM SINH_VIEN"))
            
            # Chèn vào bảng SINH_VIEN
            print(f"   Inserting {len(sinh_vien_data):,} records into SINH_VIEN...")
            sinh_vien_data.to_sql('SINH_VIEN', con=connection, if_exists='append', index=False)
            
            # Chèn vào bảng HOC_PHAN
            print(f"   Inserting {len(hoc_phan_data):,} records into HOC_PHAN...")
            hoc_phan_data.to_sql('HOC_PHAN', con=connection, if_exists='append', index=False)
            
            # Chèn vào bảng GIANG_VIEN
            print(f"   Inserting {len(giang_vien_data):,} records into GIANG_VIEN...")
            giang_vien_data.to_sql('GIANG_VIEN', con=connection, if_exists='append', index=False)
            
            # Chèn vào bảng LOP_HOC_PHAN
            print(f"   Inserting {len(lop_hoc_phan_data):,} records into LOP_HOC_PHAN...")
            lop_hoc_phan_data[['LopHP', 'MaHP', 'MaGV', 'TenLopHP', 'HocKy', 'NamHoc']].to_sql(
                'LOP_HOC_PHAN', con=connection, if_exists='append', index=False
            )
            
            # Chèn vào bảng PHIEU_KHAO_SAT
            print(f"   Inserting {len(phieu_khao_sat_data):,} records into PHIEU_KHAO_SAT...")
            phieu_khao_sat_data.to_sql('PHIEU_KHAO_SAT', con=connection, if_exists='append', index=False)
        
        print("✅ Successfully inserted all data into SQL Azure!")
        return True
        
    except Exception as e:
        print(f"❌ Error inserting into SQL: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

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
    
    # Tạo ID duy nhất cho mỗi sinh viên dựa trên Lop, MaSV, HoDem, Ten, NgaySinh
    df['StudentKey'] = df['Lop'].fillna('') + '|' + \
                       df['MaSV'].fillna('') + '|' + \
                       df['HoDem'].fillna('') + '|' + \
                       df['Ten'].fillna('') + '|' + \
                       df['NgaySinh'].astype(str).fillna('')
    
    # Tạo ID cho mỗi sinh viên
    unique_students = df['StudentKey'].unique()
    student_id_map = {key: f"SV{idx+1:06d}" for idx, key in enumerate(unique_students)}
    df['ID'] = df['StudentKey'].map(student_id_map)
    
    # Lấy thông tin cơ bản của mỗi student (1 dòng đại diện)
    df_basic = df.groupby('StudentKey').first().reset_index()
    
    # ==================== XỬ LÝ Q1-Q12 ====================
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
    
    # Merge với dữ liệu có sẵn
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
    
    # Thêm Q13-Q16
    for q in ['Q13', 'Q14', 'Q15', 'Q16']:
        if q in df.columns:
            q_values = df.groupby('StudentKey')[q].first()
            df_final[q] = df_final['StudentKey'].map(q_values)
    
    # Xử lý LopHP (gom nhiều lớp thành 1 dòng)
    lophp_grouped = df.groupby('StudentKey')['LopHP'].apply(lambda x: ' | '.join(x.dropna().unique())).reset_index()
    lophp_grouped.columns = ['StudentKey', 'LopHP']
    df_final = df_final.merge(lophp_grouped, on='StudentKey', how='left')
    
    # ==================== 14. CHỌN CỘT THEO YÊU CẦU ====================
    final_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + \
                 [f'Q{i}' for i in range(1, 17)] + \
                 ['HocKy', 'NamHoc', 'ProcessedDate']
    
    # Chỉ lấy các cột tồn tại
    final_cols_existing = [c for c in final_cols if c in df_final.columns]
    df_final = df_final[final_cols_existing]
    
    # Sắp xếp theo ID
    df_final = df_final.sort_values('ID').reset_index(drop=True)
    
    # ==================== 15. UPLOAD TO BLOB STORAGE ====================
    print("📤 Uploading to Azure Blob Storage...")
    output = df_final.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    print(f"✅ Uploaded to blob: processed-data/{output_path}")
    
    # ==================== 16. INSERT INTO SQL AZURE ====================
    # Tạo kết nối SQL
    engine = create_sql_engine()
    if engine:
        # Chèn dữ liệu vào SQL
        success = insert_into_sql(df_final, engine)
        if not success:
            print("⚠️ Warning: SQL insertion failed, but blob storage upload succeeded")
    else:
        print("❌ Failed to connect to SQL Azure, skipping database insertion")
    
    # ==================== 17. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ ETL PIPELINE COMPLETED!")
    print(f"📊 Original rows: {len(df):,}")
    print(f"📊 Student rows after grouping: {len(df_final):,}")
    print(f"📤 Blob storage: processed-data/{output_path}")
    
    if engine:
        print(f"💾 SQL Azure: Data inserted into 5 tables")
        print(f"   - SINH_VIEN: {df_final['ID'].nunique():,} records")
        print(f"   - HOC_PHAN: {df_final['MaHP'].nunique():,} records")
        print(f"   - GIANG_VIEN: {df_final['MaGV'].nunique():,} records")
        print(f"   - LOP_HOC_PHAN: {df_final['LopHP'].nunique():,} records")
        print(f"   - PHIEU_KHAO_SAT: {len(df_final):,} records")
    
    print(f"\n📋 Sample (first 3 rows):")
    sample_cols = ['ID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                   'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'Q1', 'Q2', 'Q3', 'Q13', 'Q14', 'Q15', 'Q16']
    sample_cols = [c for c in sample_cols if c in df_final.columns]
    print(df_final[sample_cols].head(3).to_string(index=False))
    
    print(f"\n📊 Statistics:")
    students_with_q1_q12 = 0
    students_with_q13_q16 = 0
    
    for idx, row in df_final.iterrows():
        has_any_q1_q12 = False
        for i in range(1, 13):
            val = row.get(f'Q{i}')
            if pd.notna(val) and val is not None and str(val).strip() != '':
                has_any_q1_q12 = True
                break
        
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
    
    if students_with_q1_q12 == students_with_q13_q16:
        print(f"   ✅ PERFECT: All students have consistent responses!")
    else:
        print(f"   ⚠️ WARNING: Mismatch detected! Difference: {abs(students_with_q1_q12 - students_with_q13_q16)} students")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
