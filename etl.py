# etl.py - TỐI ƯU VECTORIZED (CHẠY NHANH)
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy
import re

print("🚀 Starting ETL Pipeline...")

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def fast_clean_text(series):
    """Clean text nhanh trên toàn bộ series"""
    if series is None:
        return series
    series = series.fillna('').astype(str)
    series = series.replace('NULL', '')
    # Chỉ xử lý các giá trị có dấu hiệu lỗi
    mask = series.str.contains('\?', na=False)
    if mask.any():
        unique_vals = series[mask].unique()
        clean_map = {v: ftfy.fix_text(v).strip() for v in unique_vals}
        series = series.map(lambda x: clean_map.get(x, x))
    return series.replace('', None)

try:
    # ==================== 1. ĐỌC FILE ====================
    print("📥 Reading from Azure...")
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
    df['Lop'] = df[0].str.split(r'[ \t]', expand=True)[0]
    
    # ==================== 4. XỬ LÝ MASV ====================
    df['MaSV_raw'] = df[1]
    df['MaSV'] = pd.to_numeric(df[1], errors='coerce').fillna(0).astype('int64').astype(str)
    df['MaSV'] = df['MaSV'].apply(lambda x: '1' + x if len(x) == 11 else x)
    
    # ==================== 5. TÌM NGAY SINH ====================
    # Tìm cột chứa ngày sinh (định dạng dd/mm/yyyy)
    date_pattern = r'\d{1,2}/\d{1,2}/\d{4}'
    for col in range(2, min(8, len(df.columns))):
        if df[col].str.match(date_pattern).any():
            df['NgaySinh'] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)
            ngaysinh_col = col
            break
    else:
        df['NgaySinh'] = None
        ngaysinh_col = 4
    
    # ==================== 6. TÁCH HỌ TÊN SINH VIÊN ====================
    # Lấy các cột từ 2 đến ngaysinh_col
    name_cols = [str(i) for i in range(2, ngaysinh_col)]
    if name_cols:
        df['HoTenSV'] = df[name_cols].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
        # Tách họ đệm và tên
        df['Ten'] = df['HoTenSV'].str.split().str[-1]
        df['HoDem'] = df['HoTenSV'].str.split().str[:-1].apply(lambda x: ' '.join(x) if x else None)
        df['HoDem'] = fast_clean_text(df['HoDem'])
        df['Ten'] = fast_clean_text(df['Ten'])
    else:
        df['HoDem'] = None
        df['Ten'] = None
    
    # ==================== 7. MÃ HỌC PHẦN ====================
    maHP_col = ngaysinh_col + 1
    df['MaHP'] = df[maHP_col] if maHP_col in df.columns else None
    
    # ==================== 8. TÌM MÃ GIẢNG VIÊN ====================
    # Tìm cột chứa mã GV (toàn số)
    magv_col = -1
    for col in range(maHP_col + 1, min(maHP_col + 10, len(df.columns))):
        if df[col].astype(str).str.match(r'^\d+$').all():
            magv_col = col
            break
    
    if magv_col > maHP_col + 1:
        # Tên học phần ở giữa
        tenhp_cols = [str(i) for i in range(maHP_col + 1, magv_col)]
        if tenhp_cols:
            df['TenHP'] = df[tenhp_cols].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
            df['TenHP'] = fast_clean_text(df['TenHP'])
    
    df['MaGV'] = df[magv_col] if magv_col != -1 else None
    
    # ==================== 9. TÌM LỚP HỌC PHẦN ====================
    lophp_col = -1
    for col in range(magv_col + 1, min(magv_col + 10, len(df.columns))):
        if df[col].astype(str).str.contains('_', na=False).any():
            lophp_col = col
            break
    
    if lophp_col > magv_col + 1:
        # Tên giảng viên ở giữa
        gv_cols = [str(i) for i in range(magv_col + 1, lophp_col)]
        if gv_cols:
            df['HoTenGV'] = df[gv_cols].apply(lambda row: ' '.join(row.dropna().astype(str)), axis=1)
            df['TenGV'] = df['HoTenGV'].str.split().str[-1]
            df['HoDemGV'] = df['HoTenGV'].str.split().str[:-1].apply(lambda x: ' '.join(x) if x else None)
            df['HoDemGV'] = fast_clean_text(df['HoDemGV'])
            df['TenGV'] = fast_clean_text(df['TenGV'])
    
    df['LopHP'] = df[lophp_col] if lophp_col != -1 else None
    
    # ==================== 10. CÂU HỎI VÀ ĐÁNH GIÁ ====================
    df['CauHoi'] = pd.to_numeric(df[lophp_col + 1] if lophp_col + 1 in df.columns else None, errors='coerce')
    df['DanhGia'] = pd.to_numeric(df[lophp_col + 2] if lophp_col + 2 in df.columns else None, errors='coerce')
    
    # ==================== 11. PHẢN HỒI FB1-FB4 ====================
    fb_start = lophp_col + 3
    # Bỏ qua cột NULL
    while fb_start < len(df.columns) and df[fb_start].astype(str).str.strip().eq('NULL').all():
        fb_start += 1
    
    df['FB1'] = df[fb_start] if fb_start < len(df.columns) else None
    df['FB2'] = df[fb_start + 1] if fb_start + 1 < len(df.columns) else None
    df['FB3'] = df[fb_start + 2] if fb_start + 2 < len(df.columns) else None
    df['FB4'] = df[fb_start + 3] if fb_start + 3 < len(df.columns) else None
    
    # Clean FB columns
    for col in ['FB1', 'FB2', 'FB3', 'FB4']:
        if col in df.columns:
            df[col] = fast_clean_text(df[col])
    
    # ==================== 12. LỌC DỮ LIỆU ====================
    print("🔄 Filtering...")
    valid = df['MaSV'].notna() & df['MaHP'].notna() & df['CauHoi'].notna()
    result = df[valid].copy()
    print(f"   Kept {len(result):,} / {len(df):,} rows")
    
    if len(result) == 0:
        print("❌ No valid records!")
        sys.exit(1)
    
    # ==================== 13. THÊM METADATA ====================
    result['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    result['NamHoc'] = SEMESTER
    result['ProcessedDate'] = datetime.now()
    
    # ==================== 14. CHỌN CỘT ====================
    final_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
                  'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
                  'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate']
    
    result = result[[c for c in final_cols if c in result.columns]]
    result = result.sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # ==================== 15. UPLOAD ====================
    print("📤 Uploading...")
    output = result.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 16. KẾT QUẢ ====================
    print(f"\n{'='*50}")
    print(f"✅ SUCCESS! ({len(result):,} records)")
    print(f"👨‍🎓 Students: {result['MaSV'].nunique():,}")
    print(f"⭐ Avg rating: {result['DanhGia'].mean():.2f}/5")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample:")
    print(result[['Lop', 'MaSV', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']].head(5).to_string(index=False))
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
