import os
import sys
import logging
from datetime import datetime
import pandas as pd
import numpy as np
from azure.storage.blob import BlobServiceClient

# ==================== CẤU HÌNH LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ==================== BIẾN MÔI TRƯỜNG ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    logging.error("❌ Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

logging.info(f"Semester    : {SEMESTER}")
logging.info(f"Survey File : {SURVEY_FILE}")
logging.info(f"File Name   : {FILE_NAME}")

# ==================== DOWNLOAD FILE TỪ AZURE BLOB ====================
def download_from_blob():
    print("📥 Connecting to Azure Storage...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

        logging.info(f"📤 Downloading blob: {SEMESTER}/{SURVEY_FILE}")

        data = blob_client.download_blob().readall()

        # Lưu file vào thư mục hiện tại
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)

        logging.info(f"✅ Download thành công: {SURVEY_FILE} ({len(data)/1024:.1f} KB)")
        return blob_service

    except Exception as e:
        logging.error(f"❌ Lỗi download từ Azure Blob: {e}")
        sys.exit(1)

# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    logging.info("🔄 Bắt đầu xử lý dữ liệu raw...")
    
    # Đọc file CSV với encoding phù hợp
    df_raw = pd.read_csv(file_path, encoding='utf-8-sig', header=None)
    
    logging.info(f"📊 Đọc được {len(df_raw)} dòng dữ liệu raw")
    
    # Dictionary để lưu dữ liệu của mỗi sinh viên
    student_data = {}
    
    for idx, row in df_raw.iterrows():
        try:
            # Chuyển row thành list và loại bỏ các giá trị NaN
            parts = [str(x).strip() if pd.notna(x) else '' for x in row.values]
            
            # Bỏ qua dòng trống
            if len(parts) < 10 or not parts[0]:
                continue
            
            # Lấy Lop
            Lop = parts[0] if len(parts) > 0 else ''
            
            MaSV = parts[1]
            
            # Tìm NgaySinh (tìm phần tử có chứa /)
            ngay_sinh_idx = None
            for i, val in enumerate(parts):
                if '/' in str(val) and len(str(val).strip()) >= 8:
                    ngay_sinh_idx = i
                    break
            
            if ngay_sinh_idx is None:
                continue
            
            # Họ tên Sinh viên
            HoDem = parts[2] if len(parts) > 2 else ''
            Ten = parts[3] if len(parts) > 3 else ''
            NgaySinh = parts[ngay_sinh_idx]
            
            # Mã học phần
            MaHP = parts[ngay_sinh_idx + 1] if ngay_sinh_idx + 1 < len(parts) else ''
            
            # Tìm MaGV (tìm số có 7 chữ số)
            MaGV_idx = None
            for i in range(ngay_sinh_idx + 2, min(ngay_sinh_idx + 15, len(parts))):
                val = str(parts[i]).strip()
                if val.isdigit() and len(val) >= 6:
                    MaGV_idx = i
                    break
            
            if MaGV_idx is None:
                continue
            
            TenHP = parts[ngay_sinh_idx + 2] if ngay_sinh_idx + 2 < len(parts) else ''
            MaGV = parts[MaGV_idx]
            
            # Tìm LopHP (tìm phần tử có chứa _ )
            LopHP_idx = None
            for i in range(MaGV_idx + 1, min(MaGV_idx + 10, len(parts))):
                val = str(parts[i]).strip()
                if '_' in val and any(c.isdigit() for c in val):
                    LopHP_idx = i
                    break
            
            if LopHP_idx is None:
                continue
            
            HoDemGV = parts[MaGV_idx + 1] if MaGV_idx + 1 < len(parts) else ''
            TenGV = parts[MaGV_idx + 2] if MaGV_idx + 2 < len(parts) else ''
            LopHP = parts[LopHP_idx]
            
            # Số thứ tự câu hỏi (cột sau LopHP)
            cauhoi_idx = LopHP_idx + 1
            if cauhoi_idx >= len(parts):
                continue
                
            try:
                CauHoi = int(float(parts[cauhoi_idx])) if parts[cauhoi_idx] and parts[cauhoi_idx] != '' else None
            except:
                CauHoi = None
            
            # Đánh giá (cột tiếp theo)
            danhgia_idx = cauhoi_idx + 1
            try:
                DanhGia = int(float(parts[danhgia_idx])) if danhgia_idx < len(parts) and parts[danhgia_idx] and parts[danhgia_idx] != '' else None
            except:
                DanhGia = None
            
            # 4 câu hỏi mở (câu 13-16) - lấy 4 giá trị cuối cùng
            gopy_values = []
            gopy_start = len(parts) - 4
            for i in range(gopy_start, len(parts)):
                if i >= 0 and i < len(parts):
                    val = parts[i] if parts[i] and parts[i] != 'NULL' else None
                    gopy_values.append(val)
                else:
                    gopy_values.append(None)
            
            # Đảm bảo có đúng 4 giá trị
            while len(gopy_values) < 4:
                gopy_values.append(None)
            gopy_values = gopy_values[:4]
            
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"
            
            # Khởi tạo hoặc cập nhật dữ liệu sinh viên
            if SubmissionID not in student_data:
                student_data[SubmissionID] = {
                    'SubmissionID': SubmissionID,
                    'Lop': Lop,
                    'MaSV': MaSV,
                    'HoDem': HoDem,
                    'Ten': Ten,
                    'NgaySinh': NgaySinh,
                    'MaHP': MaHP,
                    'TenHP': TenHP,
                    'MaGV': MaGV,
                    'HoDemGV': HoDemGV,
                    'TenGV': TenGV,
                    'LopHP': LopHP,
                    'Semester': SEMESTER,
                    # Khởi tạo 12 câu hỏi đánh giá
                    'CauHoi1': None, 'CauHoi2': None, 'CauHoi3': None, 'CauHoi4': None,
                    'CauHoi5': None, 'CauHoi6': None, 'CauHoi7': None, 'CauHoi8': None,
                    'CauHoi9': None, 'CauHoi10': None, 'CauHoi11': None, 'CauHoi12': None,
                    # Khởi tạo 4 câu hỏi mở
                    'CauHoi13': None, 'CauHoi14': None, 'CauHoi15': None, 'CauHoi16': None
                }
            
            # Gán giá trị cho câu hỏi tương ứng
            if CauHoi and 1 <= CauHoi <= 12:
                student_data[SubmissionID][f'CauHoi{CauHoi}'] = DanhGia
            elif CauHoi and 13 <= CauHoi <= 16:
                student_data[SubmissionID][f'CauHoi{CauHoi}'] = gopy_values[CauHoi - 13]
                
        except Exception as e:
            logging.warning(f"Bỏ qua dòng {idx + 1} do lỗi: {e}")
            continue
    
    # Chuyển dictionary thành DataFrame
    result_df = pd.DataFrame(list(student_data.values()))
    
    # Kiểm tra nếu DataFrame rỗng
    if len(result_df) == 0:
        logging.error("❌ Không có dữ liệu nào được xử lý!")
        return pd.DataFrame()
    
    # Sắp xếp lại thứ tự các cột
    column_order = [
        'SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 
        'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP', 
        'Semester',
        'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 
        'CauHoi9', 'CauHoi10', 'CauHoi11', 'CauHoi12',
        'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ lấy các cột có tồn tại
    existing_columns = [col for col in column_order if col in result_df.columns]
    result_df = result_df[existing_columns]
    
    # Chuyển đổi kiểu dữ liệu cho các câu hỏi 1-12 sang số
    for i in range(1, 13):
        col = f'CauHoi{i}'
        if col in result_df.columns:
            result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
    
    logging.info(f"✅ Hoàn tất xử lý: {len(result_df)} sinh viên (mỗi sinh viên 1 hàng dữ liệu)")
    logging.info(f"📊 Các cột dữ liệu: {list(result_df.columns)}")
    
    return result_df

# ==================== UPLOAD TO AZURE ====================
def upload_to_blob(blob_service, df):
    print("📤 Uploading to Azure...")
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
        
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
            logging.info(f"✅ Đã tạo container 'processed-data'")
        
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        logging.info(f"✅ Upload thành công: {output_path}")
        
    except Exception as e:
        logging.error(f"❌ Lỗi upload lên Azure Blob: {e}")
        sys.exit(1)

# ==================== MAIN ====================
if __name__ == "__main__":
    logging.info("=" * 90)
    logging.info("     BẮT ĐẦU SURVEY ETL PIPELINE")
    logging.info("=" * 90)

    # 1. Download file từ Azure Blob
    blob_service = download_from_blob()

    # 2. Xử lý ETL
    result_df = extract_and_transform_survey(SURVEY_FILE)
    
    if len(result_df) == 0:
        logging.error("❌ Không có dữ liệu để xử lý!")
        sys.exit(1)

    # 3. Upload lên Azure
    upload_to_blob(blob_service, result_df)

    # 4. In 10 dòng mẫu để kiểm tra
    print("\n" + "="*130)
    print("📋 10 DÒNG MẪU - KẾT QUẢ XỬ LÝ (1 HÀNG/SINH VIÊN)")
    print("="*130)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(result_df.head(10).to_string(index=False))

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
