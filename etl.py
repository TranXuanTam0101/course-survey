import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re
import numpy as np

print("🚀 Starting ETL Pipeline (Optimized)")

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
    # ==================== 1. ĐỌC FILE TỐI ƯU ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. ĐỌC CSV TỐI ƯU ====================
    print("📊 Reading CSV...")
    # Đọc toàn bộ với dtype object để tránh chậm
    df = pd.read_csv(
        io.BytesIO(data),
        sep='\t',
        header=None,
        dtype=str,
        encoding='cp1258',
        low_memory=False
    )
    
    print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")
    
    # ==================== 3-11. XỬ LÝ CÁC CỘT CƠ BẢN ====================
    # Vectorized operations thay vì apply
    
    # Lop
    df['Lop'] = df[0].astype(str).str.split(' ').str[0]
    
    # MaSV
    df['MaSV_raw'] = df[1].astype(str).str.split(' ').str[0]
    df['MaSV'] = df['MaSV_raw'].apply(convert_masv)
    
    # Tìm ngày sinh
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    ngaysinh_col = -1
    for col in range(2, min(10, len(df.columns))):
        if df[col].astype(str).str.match(date_pattern, na=False).any():
            df['NgaySinh'] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            ngaysinh_col = col
            break
    
    if ngaysinh_col == -1:
        df['NgaySinh'] = None
    
    # Họ tên SV
    if ngaysinh_col > 2:
        hoten_sv = df[list(range(2, ngaysinh_col))].astype(str).agg(' '.join, axis=1)
        df['Ten'] = hoten_sv.str.split().str[-1].apply(clean_text)
        df['HoDem'] = hoten_sv.str.split().str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['Ten'] = None
        df['HoDem'] = None
    
    # Mã HP
    maHP_col = ngaysinh_col + 1 if ngaysinh_col > 0 else 5
    df['MaHP'] = df[maHP_col] if maHP_col < len(df.columns) else None
    
    # Tìm mã GV
    magv_col = -1
    for col in range(maHP_col + 1, min(maHP_col + 10, len(df.columns))):
        if df[col].astype(str).str.match(r'^\d+$', na=False).any():
            magv_col = col
            break
    
    # Tên HP
    if magv_col > maHP_col + 1:
        tenhp_cols = list(range(maHP_col + 1, magv_col))
        df['TenHP'] = df[tenhp_cols].astype(str).agg(' '.join, axis=1).apply(clean_text)
    else:
        df['TenHP'] = None
    
    # Mã GV
    df['MaGV'] = df[magv_col] if magv_col > 0 else None
    
    # Tìm lớp HP
    lophp_col = -1
    for col in range(magv_col + 1, min(magv_col + 10, len(df.columns))):
        if df[col].astype(str).str.contains('_', na=False).any():
            lophp_col = col
            break
    
    # Tên GV
    if lophp_col > magv_col + 1:
        hoten_gv_cols = list(range(magv_col + 1, lophp_col))
        hoten_gv = df[hoten_gv_cols].astype(str).agg(' '.join, axis=1)
        df['TenGV'] = hoten_gv.str.split().str[-1].apply(clean_text)
        df['HoDemGV'] = hoten_gv.str.split().str[:-1].apply(lambda x: ' '.join(x) if len(x) > 0 else None).apply(clean_text)
    else:
        df['TenGV'] = None
        df['HoDemGV'] = None
    
    # Lớp HP
    df['LopHP'] = df[lophp_col] if lophp_col > 0 else None
    
    # ==================== 12. LẤY CÂU HỎI & ĐÁNH GIÁ ====================
    cauhoi_col = lophp_col + 1 if lophp_col > 0 else 11
    df['CauHoi'] = pd.to_numeric(df[cauhoi_col], errors='coerce') if cauhoi_col < len(df.columns) else None
    
    danhgia_col = cauhoi_col + 1
    df['DanhGia'] = pd.to_numeric(df[danhgia_col], errors='coerce') if danhgia_col < len(df.columns) else None
    
    # ==================== 13. ĐỔI TÊN FB1-FB4 THÀNH Q13-Q16 ====================
    fb_start = 14
    
    df['Q13'] = df[fb_start].apply(clean_text) if fb_start < len(df.columns) else None
    df['Q14'] = df[fb_start + 1].apply(clean_text) if fb_start + 1 < len(df.columns) else None
    df['Q15'] = df[fb_start + 2].apply(clean_text) if fb_start + 2 < len(df.columns) else None
    df['Q16'] = df[fb_start + 3].apply(clean_text) if fb_start + 3 < len(df.columns) else None
    
    # ==================== 14. THÊM METADATA ====================
    df['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df['NamHoc'] = SEMESTER
    df['ProcessedDate'] = datetime.now()
    
    # ==================== 15. GỘP 12 DÒNG/SINH VIÊN (QUAN TRỌNG) ====================
    print("🔄 Merging 12 questions per student...")
    
    # Lọc chỉ lấy các câu hỏi 1-12
    df_q1_q12 = df[(df['CauHoi'] >= 1) & (df['CauHoi'] <= 12)].copy()
    
    # Tạo cột Q{so_cau_hoi} từ DanhGia
    df_q1_q12['Q_col'] = 'Q' + df_q1_q12['CauHoi'].astype(int).astype(str)
    
    # Pivot để chuyển 12 dòng thành cột
    pivot_q = df_q1_q12.pivot_table(
        index=['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 
               'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'HocKy', 'NamHoc'],
        columns='Q_col',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()
    
    # Lấy các cột Q13-Q16 (chỉ 1 dòng mỗi SV, lấy giá trị đầu tiên)
    df_q13_q16 = df.groupby(['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                              'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'HocKy', 'NamHoc'])[['Q13', 'Q14', 'Q15', 'Q16']].first().reset_index()
    
    # Merge lại
    df_final = pivot_q.merge(df_q13_q16, on=['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                                               'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'HocKy', 'NamHoc'], how='left')
    
    # Đảm bảo có đủ các cột Q1-Q12 (nếu thiếu thì thêm NaN)
    for i in range(1, 13):
        col_name = f'Q{i}'
        if col_name not in df_final.columns:
            df_final[col_name] = np.nan
    
    # Sắp xếp cột theo đúng thứ tự yêu cầu
    final_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + [f'Q{i}' for i in range(1, 17)] + ['HocKy', 'NamHoc']
    
    df_final = df_final[final_cols]
    
    # ==================== 16. UPLOAD ====================
    print("📤 Uploading to Azure...")
    output = df_final.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 17. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ SUCCESS!")
    print(f"📊 Original rows: {len(df):,}")
    print(f"📊 Final rows (unique students): {len(df_final):,}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample (first 3 students):")
    print(df_final.head(3).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
