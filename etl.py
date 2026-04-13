# etl.py - XỬ LÝ VÀ PIVOT (MỖI SINH VIÊN 1 DÒNG)
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re

print("🚀 Starting ETL Pipeline...")

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
    
    # ==================== 3. XỬ LÝ LOP ====================
    df['Lop'] = df[0].astype(str).str.split(' ').str[0]
    
    # ==================== 4. XỬ LÝ MASV ====================
    df['MaSV_raw'] = df[1].astype(str).str.split(' ').str[0]
    df['MaSV'] = df['MaSV_raw'].apply(convert_masv)
    
    # ==================== 5. TÌM NGÀY SINH ====================
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    ngaysinh_col = -1
    for col in range(2, min(10, len(df.columns))):
        if df[col].astype(str).str.match(date_pattern, na=False).any():
            df['NgaySinh'] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            ngaysinh_col = col
            break
    
    if ngaysinh_col == -1:
        df['NgaySinh'] = None
    
    # ==================== 6. XỬ LÝ HỌ TÊN SINH VIÊN ====================
    if ngaysinh_col > 2:
        hoten_sv = df[list(range(2, ngaysinh_col))].astype(str).apply(lambda x: ' '.join(x), axis=1)
        df['Ten'] = hoten_sv.str.split().str[-1].apply(clean_text)
        df['HoDem'] = hoten_sv.str.split().str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['Ten'] = None
        df['HoDem'] = None
    
    # ==================== 7. MÃ HỌC PHẦN ====================
    maHP_col = ngaysinh_col + 1 if ngaysinh_col > 0 else 5
    df['MaHP'] = df[maHP_col] if maHP_col < len(df.columns) else None
    
    # ==================== 8. TÌM MÃ GIẢNG VIÊN ====================
    magv_col = -1
    for col in range(maHP_col + 1, min(maHP_col + 10, len(df.columns))):
        if df[col].astype(str).str.match(r'^\d+$', na=False).any():
            magv_col = col
            break
    
    # Tên học phần
    if magv_col > maHP_col + 1:
        tenhp_cols = list(range(maHP_col + 1, magv_col))
        df['TenHP'] = df[tenhp_cols].astype(str).apply(lambda x: ' '.join(x), axis=1).apply(clean_text)
    else:
        df['TenHP'] = None
    
    # Mã GV
    df['MaGV'] = df[magv_col] if magv_col > 0 else None
    
    # ==================== 9. TÌM LỚP HỌC PHẦN ====================
    lophp_col = -1
    for col in range(magv_col + 1, min(magv_col + 10, len(df.columns))):
        if df[col].astype(str).str.contains('_', na=False).any():
            lophp_col = col
            break
    
    # Tên giảng viên
    if lophp_col > magv_col + 1:
        hoten_gv_cols = list(range(magv_col + 1, lophp_col))
        hoten_gv = df[hoten_gv_cols].astype(str).apply(lambda x: ' '.join(x), axis=1)
        df['TenGV'] = hoten_gv.str.split().str[-1].apply(clean_text)
        df['HoDemGV'] = hoten_gv.str.split().str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['TenGV'] = None
        df['HoDemGV'] = None
    
    # Lớp HP
    df['LopHP'] = df[lophp_col] if lophp_col > 0 else None
    
    # ==================== 10. CÂU HỎI VÀ ĐÁNH GIÁ ====================
    cauhoi_col = lophp_col + 1 if lophp_col > 0 else 11
    df['CauHoi'] = pd.to_numeric(df[cauhoi_col], errors='coerce') if cauhoi_col < len(df.columns) else None
    
    danhgia_col = cauhoi_col + 1
    df['DanhGia'] = pd.to_numeric(df[danhgia_col], errors='coerce') if danhgia_col < len(df.columns) else None
    
    # ==================== 11. PHẢN HỒI FB1-FB4 ====================
    fb_start = 14
    
    df['FB1'] = df[fb_start].apply(clean_text) if fb_start < len(df.columns) else None
    df['FB2'] = df[fb_start + 1].apply(clean_text) if fb_start + 1 < len(df.columns) else None
    df['FB3'] = df[fb_start + 2].apply(clean_text) if fb_start + 2 < len(df.columns) else None
    df['FB4'] = df[fb_start + 3].apply(clean_text) if fb_start + 3 < len(df.columns) else None
    
    # ==================== 12. THÊM METADATA ====================
    df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df['NamHoc'] = SEMESTER
    df['ProcessedDate'] = datetime.now()
    
    # ==================== 13. PIVOT DỮ LIỆU: MỖI SINH VIÊN 1 DÒNG ====================
    print("🔄 Pivoting data: 12 rows per student -> 1 row per student...")
    
    # Chỉ lấy các câu hỏi từ 1-12
    df_questions = df[df['CauHoi'].between(1, 12)].copy()
    
    # Pivot câu hỏi
    question_pivot = df_questions.pivot_table(
        index=['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 
               'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'HocKy', 'NamHoc', 'ProcessedDate'],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()
    
    # Đổi tên cột Q1-Q12
    question_pivot.columns = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 
                               'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'HocKy', 'NamHoc', 'ProcessedDate'] + \
                              [f'Q{i}' for i in range(1, 13)]
    
    # Lấy phản hồi FB (chỉ 1 dòng mỗi sinh viên)
    feedback_df = df[['Lop', 'MaSV', 'MaHP', 'FB1', 'FB2', 'FB3', 'FB4']].drop_duplicates(
        subset=['Lop', 'MaSV', 'MaHP']
    )
    
    # Merge
    result_df = question_pivot.merge(
        feedback_df[['Lop', 'MaSV', 'MaHP', 'FB1', 'FB2', 'FB3', 'FB4']],
        on=['Lop', 'MaSV', 'MaHP'],
        how='left'
    )
    
    # Đổi tên FB thành Q13-Q16
    result_df = result_df.rename(columns={
        'FB1': 'Q13',
        'FB2': 'Q14',
        'FB3': 'Q15',
        'FB4': 'Q16'
    })
    
    # Sắp xếp cột
    final_columns = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                     'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
                     'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12',
                     'Q13', 'Q14', 'Q15', 'Q16', 'HocKy', 'NamHoc', 'ProcessedDate']
    
    result_df = result_df[[c for c in final_columns if c in result_df.columns]]
    
    print(f"✅ After pivot: {len(result_df):,} rows (each row = 1 student)")
    
    # ==================== 14. UPLOAD ====================
    print("📤 Uploading to Azure...")
    output = result_df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 15. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ SUCCESS!")
    print(f"📊 Before pivot: {len(df):,} rows (12 rows per student)")
    print(f"📊 After pivot: {len(result_df):,} rows (1 row per student)")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample (first 3 rows):")
    sample_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'MaHP', 'TenHP', 
                   'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Q6', 'Q7', 'Q8', 'Q9', 'Q10', 'Q11', 'Q12',
                   'Q13', 'Q14', 'Q15', 'Q16']
    sample_cols = [c for c in sample_cols if c in result_df.columns]
    print(result_df[sample_cols].head(3).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
