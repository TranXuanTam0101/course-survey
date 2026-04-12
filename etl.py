# etl.py - XỬ LÝ DỮ LIỆU THEO ĐẶC ĐIỂM NHẬN DẠNG
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
    if '?' in text or '??' in text:
        text = ftfy.fix_text(text)
    return text.strip()

def convert_masv(value):
    """Chuyển 91122E+11 -> 9112200000000000"""
    if pd.isna(value) or value == '':
        return None
    try:
        return str(int(float(str(value))))
    except:
        return value

def parse_lop(value):
    """Lop: Đầu dòng (trước dấu cách)"""
    if pd.isna(value) or value == '':
        return None
    value_str = str(value).strip()
    # Lấy phần trước dấu cách hoặc tab
    if ' ' in value_str:
        return value_str.split(' ')[0]
    if '\t' in value_str:
        return value_str.split('\t')[0]
    return value_str

def extract_hodem_ten(full_name):
    """
    Tách HoDem và Ten từ chuỗi họ tên
    Ten: Từ cuối cùng
    HoDem: Tất cả các từ còn lại
    """
    if pd.isna(full_name) or full_name == '':
        return None, None
    
    full_name = str(full_name).strip()
    parts = full_name.split()
    
    if len(parts) == 0:
        return None, None
    elif len(parts) == 1:
        return None, clean_text(parts[0])
    else:
        ten = clean_text(parts[-1])
        hodem = clean_text(' '.join(parts[:-1]))
        return hodem, ten

def parse_row_advanced(row_text):
    """
    Parse một dòng dữ liệu dựa trên đặc điểm nhận dạng
    Input: chuỗi text đã được tách bằng tab
    """
    if pd.isna(row_text) or row_text == '':
        return None
    
    fields = str(row_text).split('\t')
    
    if len(fields) < 17:
        return None
    
    result = {}
    
    # 1. Lop: Đầu dòng (trước dấu cách)
    lop_raw = fields[0].strip()
    if ' ' in lop_raw:
        result['Lop'] = lop_raw.split(' ')[0]
    elif '\t' in lop_raw:
        result['Lop'] = lop_raw.split('\t')[0]
    else:
        result['Lop'] = lop_raw
    
    # 2. MaSV: Sau Lop, chuỗi số 12 ký tự (có thể ở dạng E+)
    masv_raw = fields[1].strip() if len(fields) > 1 else ''
    result['MaSV'] = convert_masv(masmv_raw)
    
    # 3-5. Tìm NgaySinh để xác định vị trí
    ngaysinh_value = None
    ngaysinh_index = -1
    
    # Tìm NgaySinh (định dạng dd/mm/yyyy)
    for i, field in enumerate(fields):
        if re.match(r'\d{1,2}/\d{1,2}/\d{4}', field.strip()):
            ngaysinh_value = field.strip()
            ngaysinh_index = i
            break
    
    result['NgaySinh'] = pd.to_datetime(ngaysinh_value, errors='coerce', dayfirst=True) if ngaysinh_value else None
    
    # HoDem & Ten: Lấy phần giữa MaSV và NgaySinh
    if ngaysinh_index > 1:
        hoten_sv = ' '.join(fields[2:ngaysinh_index]).strip()
        hodem, ten = extract_hodem_ten(hoten_sv)
        result['HoDem'] = hodem
        result['Ten'] = ten
    else:
        result['HoDem'] = None
        result['Ten'] = None
    
    # 6. MaHP: Mã học phần, sau ngày sinh
    if ngaysinh_index + 1 < len(fields):
        result['MaHP'] = fields[ngaysinh_index + 1].strip()
    else:
        result['MaHP'] = None
    
    # 7. TenHP: Phần giữa MaHP và MaGV
    # Tìm MaGV (chuỗi số)
    magv_index = -1
    for i in range(ngaysinh_index + 2, min(ngaysinh_index + 10, len(fields))):
        if re.match(r'^\d+$', fields[i].strip()):
            magv_index = i
            break
    
    if magv_index > ngaysinh_index + 1:
        tenhp = ' '.join(fields[ngaysinh_index + 2:magv_index]).strip()
        result['TenHP'] = clean_text(tenhp)
    else:
        result['TenHP'] = None
    
    # 8. MaGV
    if magv_index != -1:
        result['MaGV'] = fields[magv_index].strip()
    else:
        result['MaGV'] = None
    
    # 9-10. HoDemGV & TenGV: Phần giữa MaGV và LopHP
    # Tìm LopHP (mã lớp học phần)
    lophp_index = -1
    for i in range(magv_index + 1, min(magv_index + 10, len(fields))):
        if '_' in fields[i] or (re.match(r'^[A-Z0-9_]+$', fields[i].strip())):
            lophp_index = i
            break
    
    if lophp_index > magv_index + 1:
        hoten_gv = ' '.join(fields[magv_index + 1:lophp_index]).strip()
        hodem_gv, ten_gv = extract_hodem_ten(hoten_gv)
        result['HoDemGV'] = hodem_gv
        result['TenGV'] = ten_gv
    else:
        result['HoDemGV'] = None
        result['TenGV'] = None
    
    # 11. LopHP
    if lophp_index != -1:
        result['LopHP'] = fields[lophp_index].strip()
    else:
        result['LopHP'] = None
    
    # 12. CauHoi
    if lophp_index + 1 < len(fields):
        result['CauHoi'] = pd.to_numeric(fields[lophp_index + 1], errors='coerce')
    else:
        result['CauHoi'] = None
    
    # 13. DanhGia
    if lophp_index + 2 < len(fields):
        result['DanhGia'] = pd.to_numeric(fields[lophp_index + 2], errors='coerce')
    else:
        result['DanhGia'] = None
    
    # 14-17. FB1, FB2, FB3, FB4 (bỏ qua cột NULL)
    fb_start = lophp_index + 3
    # Bỏ qua các giá trị NULL
    while fb_start < len(fields) and fields[fb_start].strip() == 'NULL':
        fb_start += 1
    
    result['FB1'] = clean_text(fields[fb_start]) if fb_start < len(fields) else None
    result['FB2'] = clean_text(fields[fb_start + 1]) if fb_start + 1 < len(fields) else None
    result['FB3'] = clean_text(fields[fb_start + 2]) if fb_start + 2 < len(fields) else None
    result['FB4'] = clean_text(fields[fb_start + 3]) if fb_start + 3 < len(fields) else None
    
    return result

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
    
    # ==================== 2. GIẢI MÃ VÀ ĐỌC DÒNG ====================
    print("📊 Decoding and parsing rows...")
    
    # Giải mã
    try:
        text_content = data.decode('cp1258')
    except:
        text_content = data.decode('utf-8')
    
    # Fix encoding
    text_content = ftfy.fix_text(text_content)
    
    # Đọc từng dòng
    lines = text_content.split('\n')
    print(f"   Total lines: {len(lines):,}")
    
    # Parse từng dòng
    records = []
    for i, line in enumerate(lines):
        if i % 50000 == 0 and i > 0:
            print(f"   Processing line {i:,}/{len(lines):,}")
        
        if line.strip():
            record = parse_row_advanced(line)
            if record and record.get('MaSV') and record.get('MaHP') and record.get('CauHoi'):
                records.append(record)
    
    print(f"✅ Parsed {len(records):,} valid records")
    
    if len(records) == 0:
        print("❌ ERROR: No valid records found!")
        sys.exit(1)
    
    # Tạo DataFrame
    result = pd.DataFrame(records)
    
    # ==================== 3. THÊM METADATA ====================
    result['HocKy'] = 2 if "252" in SURVEY_FILE else 1 if "251" in SURVEY_FILE else None
    result['NamHoc'] = SEMESTER
    result['ProcessedDate'] = datetime.now()
    
    # ==================== 4. SẮP XẾP CỘT ====================
    final_columns = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP',
        'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 'CauHoi', 'DanhGia',
        'FB1', 'FB2', 'FB3', 'FB4', 'HocKy', 'NamHoc', 'ProcessedDate'
    ]
    
    result = result[[col for col in final_columns if col in result.columns]]
    result = result.sort_values(['MaSV', 'MaHP', 'CauHoi'])
    
    # ==================== 5. UPLOAD KẾT QUẢ ====================
    print("📤 Uploading to Azure...")
    output = result.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    processed_container = blob_service.get_container_client("processed-data")
    if not processed_container.exists():
        processed_container.create_container()
        print("   Created container: processed-data")
    
    processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
    
    # ==================== 6. KẾT QUẢ ====================
    print(f"\n{'='*60}")
    print(f"✅ SUCCESS!")
    print(f"{'='*60}")
    print(f"📊 Total responses: {len(result):,}")
    print(f"👨‍🎓 Total students: {result['MaSV'].nunique():,}")
    print(f"📚 Total courses: {result['MaHP'].nunique():,}")
    print(f"⭐ Average rating: {result['DanhGia'].mean():.2f}/5")
    print(f"📤 Uploaded to: processed-data/{output_path}")
    
    print(f"\n📋 Sample data (first 5 rows):")
    sample_cols = ['Lop', 'MaSV', 'HoDem', 'Ten', 'MaHP', 'CauHoi', 'DanhGia']
    sample_cols = [c for c in sample_cols if c in result.columns]
    print(result[sample_cols].head(5).to_string(index=False))
    
    print(f"\n📊 Rating by question:")
    if 'CauHoi' in result.columns and 'DanhGia' in result.columns:
        question_stats = result.groupby('CauHoi')['DanhGia'].agg(['mean', 'count'])
        for q in sorted(question_stats.index):
            if q <= 12:
                print(f"   Q{int(q):2d}: {question_stats.loc[q, 'mean']:.2f}/5 ({question_stats.loc[q, 'count']:,})")
    
except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
