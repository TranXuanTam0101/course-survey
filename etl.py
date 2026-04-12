# etl.py - TIỀN XỬ LÝ DỮ LIỆU KHẢO SÁT
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

def clean_vietnamese(text):
    """Sửa lỗi encoding tiếng Việt"""
    if pd.isna(text) or text == 'NULL' or text == '':
        return None
    text = str(text)
    # Dùng ftfy để sửa lỗi encoding
    text = ftfy.fix_text(text)
    # Loại bỏ khoảng trắng thừa
    text = re.sub(r'\s+', ' ', text).strip()
    return text if text else None

def convert_masv(value):
    """
    Chuyển đổi MaSV từ dạng 91122E+11 thành số bình thường
    91122E+11 = 91122 * 10^11 = 9112200000000000
    """
    if pd.isna(value) or value == '':
        return None
    
    value_str = str(value).strip()
    
    try:
        # Nếu có dạng E+ (khoa học)
        if 'E' in value_str.upper():
            # Chuyển thành số float rồi thành int
            num = float(value_str)
            # Chuyển thành số nguyên không có dấu thập phân
            result = str(int(num))
            return result
        else:
            # Nếu đã là số bình thường
            return str(int(float(value_str)))
    except:
        # Nếu không chuyển được, giữ nguyên
        return value_str

def parse_lop(value):
    """
    Xử lý cột Lop: "45K05\t1" -> "45K05"
    Chỉ lấy phần trước dấu cách hoặc tab
    """
    if pd.isna(value) or value == '':
        return None
    
    value_str = str(value).strip()
    
    # Nếu có tab, lấy phần đầu
    if '\t' in value_str:
        return value_str.split('\t')[0]
    # Nếu có space, lấy phần đầu
    if ' ' in value_str:
        return value_str.split(' ')[0]
    
    return value_str

def parse_student_info(value):
    """
    Parse cột thông tin sinh viên
    Input: "91122E+11 Hoàng Quýc 1/5/2001"
    Output: (maSV, hoDem, ten, ngaySinh)
    """
    if pd.isna(value) or value == '':
        return None, None, None, None
    
    value_str = str(value).strip()
    parts = value_str.split()
    
    maSV = None
    hoDem = None
    ten = None
    ngaySinh = None
    
    if len(parts) >= 4:
        # 91122E+11 Hoàng Quýc 1/5/2001
        maSV = convert_masv(parts[0])
        hoDem = clean_vietnamese(parts[1])
        ten = clean_vietnamese(parts[2])
        ngaySinh = parts[3]
    elif len(parts) >= 3:
        maSV = convert_masv(parts[0])
        hoDem = clean_vietnamese(parts[1])
        ten = None
        ngaySinh = parts[2] if len(parts) > 2 else None
    elif len(parts) >= 2:
        maSV = convert_masv(parts[0])
        ten = clean_vietnamese(parts[1])
    
    return maSV, hoDem, ten, ngaySinh

try:
    # ==================== 1. ĐỌC FILE ====================
    print("📥 Connecting to Azure Storage...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    raw_container = blob_service.get_container_client("rawdata")
    
    blob_path = f"{SEMESTER}/{SURVEY_FILE}"
    print(f"📄 Reading: {blob_path}")
    
    blob_client = raw_container.get_blob_client(blob_path)
    data = blob_client.download_blob().readall()
    print(f"✅ Downloaded {len(data) / 1024 / 1024:.2f} MB")
    
    # ==================== 2. GIẢI MÃ ENCODING ====================
    print("📊 Decoding file...")
    try:
        text_content = data.decode('cp1258')
        print("   Using encoding: cp1258")
    except:
        try:
            text_content = data.decode('utf-8')
            print("   Using encoding: utf-8")
        except:
            text_content = data.decode('latin1')
            print("   Using encoding: latin1")
    
    # ==================== 3. ĐỌC CSV ====================
    print("📊 Reading CSV...")
    df = pd.read_csv(
        io.StringIO(text_content),
        sep='\t',
        header=None,
        dtype=str,
        low_memory=False
    )
    
    print(f"✅ Read {len(df):,} rows, {len(df.columns)} columns")
    
    # ==================== 4. TẠO DATAFRAME KẾT QUẢ ====================
    result_df = pd.DataFrame()
    
    # Cột 0: Lop - Chỉ lấy phần trước dấu cách/tab
    print("🔄 Processing Lop column...")
    result_df['Lop'] = df[0].apply(parse_lop)
    
    # Cột 1: Thông tin sinh viên (tách thành MaSV, HoDem, Ten, NgaySinh)
    print("🔄 Processing student info...")
    student_info = df[1].apply(parse_student_info)
    result_df['MaSV'] = student_info.apply(lambda x: x[0])
    result_df['HoDem'] = student_info.apply(lambda x: x[1])
    result_df['Ten'] = student_info.apply(lambda x: x[2])
    result_df['NgaySinh'] = student_info.apply(lambda x: x[3])
    
    # Chuyển đổi NgaySinh thành datetime
    result_df['NgaySinh'] = pd.to_datetime(result_df['NgaySinh'], errors='coerce', dayfirst=True)
    
    # Các cột từ vị trí 5 đến 17
    print("🔄 Processing course and evaluation data...")
    
    # Cột 5: MaHP
    result_df['MaHP'] = df[5] if 5 in df.columns else None
    
    # Cột 6: TenHP
    result_df['TenHP'] = df[6].apply(clean_vietnamese) if 6 in df.columns else None
    
    # Cột 7: MaGV
    result_df['MaGV'] = df[7] if 7 in df.columns else None
    
    # Cột 8: HoDemGV
    result_df['HoDemGV'] = df[8].apply(clean_vietnamese) if 8 in df.columns else None
    
    # Cột 9: TenGV
    result_df['TenGV'] = df[9].apply(clean_vietnamese) if 9 in df.columns else None
    
    # Cột 10: LopHP
    result_df['LopHP'] = df[10] if 10 in df.columns else None
    
    # Cột 11: CauHoi
    result_df['CauHoi'] = pd.to_numeric(df[11], errors='coerce') if 11 in df.columns else None
    
    # Cột 12: DanhGia
    result_df['DanhGia'] = pd.to_numeric(df[12], errors='coerce') if 12 in df.columns else None
    
    # Cột 13: NULL - Bỏ qua (không lấy)
    
    # Cột 14: FB1
    result_df['FB1'] = df[14].apply(clean_vietnamese) if 14 in df.columns else None
    
    # Cột 15: FB2
    result_df['FB2'] = df[15].apply(clean_vietnamese) if 15 in df.columns else None
    
    # Cột 16: FB3
    result_df['FB3'] = df[16].apply(clean_vietnamese) if 16 in df.columns else None
    
    # Cột 17: FB4
    result_df['FB4'] = df[17].apply(clean_vietnamese) if 17 in df.columns else None
    
    # ==================== 5. LỌC DỮ LIỆU ====================
    print("🔄 Filtering valid records...")
    before = len(result_df)
    
    # Chỉ giữ các dòng có MaSV, MaHP, CauHoi
    valid_mask = (
        result_df['MaSV'].notna() & 
        result_df['MaHP'].notna() & 
        result_df['CauHoi'].notna()
    )
    
    result_df = result_df[valid_mask]
    print(f"   Kept {len(result_df):,} / {before:,} rows ({len(result_df)/before*100:.1f}%)")
    
    if len(result_df) == 0:
        print("❌ ERROR: No valid records found!")
        sys.exit(1)
    
    # ==================== 6. THÊM METADATA ====================
    result_df['HocKy'] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    result_df['NamHoc'] = SEMESTER
    result_df['ProcessedDate'] = datetime.now()
    
    # ==================== 7. SẮP XẾP CỘT ====================
    final_columns = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
        'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
        'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate'
    ]
    
    result_df = result_df[final_columns]
    
    # Sắp xếp theo MaSV, MaHP, CauHoi
    result_df = result_df.sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # ==================== 8. UPLOAD KẾT QUẢ ====================
    print("📤 Uploading to Azure...")
    output = result_df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
        print("   Created container: processed-data")
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 9. KẾT QUẢ ====================
    print(f"\n{'='*60}")
    print(f"✅ SUCCESS!")
    print(f"{'='*60}")
    print(f"📊 Total responses: {len(result_df):,}")
    print(f"👨‍🎓 Total students: {result_df['MaSV'].nunique():,}")
    print(f"📚 Total courses: {result_df['MaHP'].nunique():,}")
    print(f"⭐ Average rating: {result_df['DanhGia'].mean():.2f}/5")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    # Hiển thị mẫu dữ liệu
    print(f"\n📋 Sample data (first 5 rows):")
    sample_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
    print(result_df[sample_cols].head(5).to_string(index=False))
    
    # Thống kê theo câu hỏi
    print(f"\n📊 Rating by question:")
    question_stats = result_df.groupby('CauHoi')['DanhGia'].agg(['mean', 'count'])
    for q in sorted(question_stats.index):
        if q <= 12:
            mean_val = question_stats.loc[q, 'mean']
            count_val = question_stats.loc[q, 'count']
            print(f"   Question {int(q):2d}: {mean_val:.2f}/5 ({count_val:,} responses)")
    
    # Kiểm tra MaSV
    print(f"\n📋 Sample MaSV values (converted from E+):")
    sample_masv = result_df['MaSV'].dropna().unique()[:5]
    for sv in sample_masv:
        print(f"   {sv}")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
