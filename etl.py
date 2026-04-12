# etl.py - SỬ DỤNG FTFY
import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy  # Thư viện fix encoding
import unicodedata

print("🚀 Starting ETL Pipeline...")

# Lấy connection string từ environment variable
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def clean_text(text):
    """Fix lỗi encoding tự động"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    # ftfy tự động sửa lỗi
    text = ftfy.fix_text(text)
    # Chuẩn hóa Unicode
    text = unicodedata.normalize('NFC', text)
    return text.strip()

def parse_student_info(raw_text):
    """Parse thông tin sinh viên"""
    if pd.isna(raw_text):
        return None, None, None, None
    
    parts = str(raw_text).split('\t')
    if len(parts) >= 4:
        maSV = parts[0].strip()
        hoDem = clean_text(parts[1])
        ten = clean_text(parts[2])
        ngaySinh = parts[3].strip()
        return maSV, hoDem, ten, ngaySinh
    return None, None, None, None

def fix_masv(masv):
    """Chuyển đổi mã số sinh viên"""
    if pd.isna(masv):
        return None
    try:
        if 'E' in str(masv) or 'e' in str(masv):
            num = float(masv)
            result = str(int(num))
            if len(result) == 11:
                result = '1' + result
            return result
        return str(int(float(masv)))
    except:
        return masv

def fix_lop(lop):
    """Xử lý Lop: 45K05\t1 -> 45K05.1"""
    if pd.isna(lop):
        return None
    parts = str(lop).split('\t')
    if len(parts) >= 2:
        return f"{parts[0].strip()}.{parts[1].strip()}"
    return lop

try:
    # Kết nối Azure
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    raw_container = blob_service.get_container_client("rawdata")
    
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Reading: {blob_path}")
    
    blob_client = raw_container.get_blob_client(blob_path)
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data)} bytes")
    
    # Đọc CSV với xử lý encoding
    print("📊 Parsing CSV with encoding fix...")
    
    # Thử decode với cp1258 trước
    try:
        text_content = data.decode('cp1258')
    except:
        try:
            text_content = data.decode('utf-8')
        except:
            text_content = data.decode('latin1')
    
    # Fix toàn bộ lỗi encoding
    text_content = ftfy.fix_text(text_content)
    
    # Đọc vào pandas
    df_raw = pd.read_csv(
        io.StringIO(text_content),
        sep='\t',
        header=None,
        dtype=str,
        low_memory=False
    )
    
    print(f"✅ Read {len(df_raw)} rows, {len(df_raw.columns)} columns")
    
    # Xử lý từng dòng
    print("🔄 Processing data...")
    records = []
    
    for idx, row in df_raw.iterrows():
        if idx % 10000 == 0:
            print(f"   Progress: {idx:,}/{len(df_raw):,}")
        
        # Lấy dữ liệu từ các cột
        raw_lop = row[0] if len(row) > 0 else None
        raw_student = row[1] if len(row) > 1 else None
        raw_ma_hp = row[5] if len(row) > 5 else None
        raw_ten_hp = row[6] if len(row) > 6 else None
        raw_ma_gv = row[7] if len(row) > 7 else None
        raw_ho_dem_gv = row[8] if len(row) > 8 else None
        raw_ten_gv = row[9] if len(row) > 9 else None
        raw_lop_hp = row[10] if len(row) > 10 else None
        raw_cau_hoi = row[11] if len(row) > 11 else None
        raw_danh_gia = row[12] if len(row) > 12 else None
        raw_fb1 = row[13] if len(row) > 13 else None
        raw_fb2 = row[14] if len(row) > 14 else None
        raw_fb3 = row[15] if len(row) > 15 else None
        raw_fb4 = row[16] if len(row) > 16 else None
        
        # Parse thông tin
        ma_sv, ho_dem, ten, ngay_sinh = parse_student_info(raw_student)
        ma_sv = fix_masv(ma_sv)
        lop = fix_lop(raw_lop)
        
        if ma_sv and raw_ma_hp:  # Chỉ giữ record hợp lệ
            records.append({
                'Lop': lop,
                'MaSV': ma_sv,
                'HoDem': ho_dem,
                'Ten': ten,
                'NgaySinh': ngay_sinh,
                'MaHP': raw_ma_hp.strip() if raw_ma_hp else None,
                'TenHP': clean_text(raw_ten_hp),
                'MaGV': raw_ma_gv.strip() if raw_ma_gv else None,
                'HoDemGV': clean_text(raw_ho_dem_gv),
                'TenGV': clean_text(raw_ten_gv),
                'LopHP': raw_lop_hp.strip() if raw_lop_hp else None,
                'CauHoi': raw_cau_hoi,
                'DanhGia': raw_danh_gia,
                'FB1': raw_fb1 if raw_fb1 not in ['NULL', None] else None,
                'FB2': raw_fb2 if raw_fb2 not in ['NULL', None] else None,
                'FB3': raw_fb3 if raw_fb3 not in ['NULL', None] else None,
                'FB4': raw_fb4 if raw_fb4 not in ['NULL', None] else None,
            })
    
    df_result = pd.DataFrame(records)
    print(f"✅ Processed {len(df_result)} valid records")
    
    # Chuyển đổi kiểu dữ liệu
    df_result['CauHoi'] = pd.to_numeric(df_result['CauHoi'], errors='coerce')
    df_result['DanhGia'] = pd.to_numeric(df_result['DanhGia'], errors='coerce')
    df_result['NgaySinh'] = pd.to_datetime(df_result['NgaySinh'], errors='coerce', dayfirst=True)
    
    # Thêm metadata
    df_result['HocKy'] = 2 if "252" in SURVEY_FILE else 1
    df_result['NamHoc'] = SEMESTER
    df_result['ProcessedDate'] = datetime.now()
    
    # Upload kết quả
    print("📤 Uploading to Azure...")
    output = df_result.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    print(f"\n✅ SUCCESS!")
    print(f"📊 Records: {len(df_result):,}")
    print(f"⭐ Avg rating: {df_result['DanhGia'].mean():.2f}")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
