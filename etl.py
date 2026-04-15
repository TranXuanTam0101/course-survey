import os
import sys
import logging
from datetime import datetime
import pandas as pd
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
    
    # Dictionary để lưu dữ liệu của mỗi sinh viên
    student_data = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # Lấy Lop từ cột đầu tiên
            Lop = parts[0] if len(parts) > 0 else ''
            
            MaSV = parts[1]

            # Tìm NgaySinh
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None:
                continue

            # Họ tên Sinh viên
            ho_ten_sv = parts[2:ngay_sinh_idx]
            HoDem = ho_ten_sv[0] if ho_ten_sv else ''
            Ten = ','.join(ho_ten_sv[1:]) if len(ho_ten_sv) > 1 else ''

            # Tên học phần
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''

            # Tìm MaGV
            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Họ tên Giảng viên
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                continue

            ho_ten_gv = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv[0] if ho_ten_gv else ''
            TenGV = ','.join(ho_ten_gv[1:]) if len(ho_ten_gv) > 1 else ''

            LopHP = parts[LopHP_idx]

            # Cột sau LopHP
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and str(parts[cau_hoi_idx]).isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and str(parts[cau_hoi_idx + 1]).isdigit() else None

            # 4 cột góp ý mở (câu 13-16) được lấy theo thứ tự cuối dòng dữ liệu
            gopy_start = cau_hoi_idx + 3
            gopy_values = []
            for i in range(4):
                if gopy_start + i < len(parts):
                    gopy_values.append(parts[gopy_start + i])
                else:
                    gopy_values.append(None)

            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo key cho student_data nếu chưa có
            if SubmissionID not in student_data:
                student_data[SubmissionID] = {
                    'SubmissionID': SubmissionID,
                    'Lop': Lop,
                    'MaSV': MaSV,
                    'HoDem': HoDem,
                    'Ten': Ten,
                    'NgaySinh': parts[ngay_sinh_idx],
                    'MaHP': MaHP,
                    'TenHP': TenHP,
                    'MaGV': MaGV,
                    'HoDemGV': HoDemGV,
                    'TenGV': TenGV,
                    'LopHP': LopHP,
                    'Semester': SEMESTER,
                    # Khởi tạo 12 câu hỏi
                    'CauHoi1': None, 'CauHoi2': None, 'CauHoi3': None, 'CauHoi4': None,
                    'CauHoi5': None, 'CauHoi6': None, 'CauHoi7': None, 'CauHoi8': None,
                    'CauHoi9': None, 'CauHoi10': None, 'CauHoi11': None, 'CauHoi12': None,
                    # Khởi tạo 4 câu hỏi mở được lấy theo thứ tự cuối dòng dữ liệu
                    'CauHoi13': None, 'CauHoi14': None, 'CauHoi15': None, 'CauHoi16': None
                }
            
            # Gán giá trị cho câu hỏi tương ứng
            if CauHoi and 1 <= CauHoi <= 12:
                student_data[SubmissionID][f'CauHoi{CauHoi}'] = DanhGia
            elif CauHoi and 13 <= CauHoi <= 16:
                student_data[SubmissionID][f'CauHoi{CauHoi}'] = gopy_values[CauHoi - 13]

        except Exception as e:
            logging.warning(f"Bỏ qua dòng {line_num} do lỗi định dạng: {e}")

    # Chuyển dictionary thành DataFrame
    result_df = pd.DataFrame(list(student_data.values()))
    
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
    
    result_df = result_df[column_order]

    logging.info(f"✅ Hoàn tất xử lý: {len(result_df)} sinh viên (mỗi sinh viên 1 hàng dữ liệu)")
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

    # 3. Upload lên Azure
    upload_to_blob(blob_service, result_df)

    # 4. In 10 dòng mẫu để kiểm tra
    print("\n" + "="*130)
    print("📋 10 DÒNG MẪU - KẾT QUẢ XỬ LÝ (1 HÀNG/SINH VIÊN)")
    print("="*130)
    print(result_df.head(10).to_string(index=False))

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
