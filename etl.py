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
def download_from_blob(blob_service):
    print("📥 Connecting to Azure Storage...")
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

        logging.info(f"📤 Downloading blob: {SEMESTER}/{SURVEY_FILE}")

        data = blob_client.download_blob().readall()

        # Lưu file vào thư mục hiện tại
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)

        logging.info(f"✅ Download thành công: {SURVEY_FILE} ({len(data)/1024:.1f} KB)")

    except Exception as e:
        logging.error(f"❌ Lỗi download từ Azure Blob: {e}")
        sys.exit(1)

# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    logging.info("🔄 Bắt đầu xử lý dữ liệu raw...")
    
    # Dictionary lưu dữ liệu theo SubmissionID
    survey_data = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # Lấy Lop từ phần tử đầu tiên của dòng
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

            # BỎ QUA CỘT NULL SAU LopHP (cột toàn giá trị NULL)
            # Tìm vị trí bắt đầu của câu hỏi (sau cột NULL)
            null_col_idx = LopHP_idx + 1
            # Kiểm tra nếu cột đó là NULL thì bỏ qua, chuyển sang cột tiếp theo
            if null_col_idx < len(parts) and parts[null_col_idx].upper() == 'NULL':
                cau_hoi_idx = null_col_idx + 1
            else:
                cau_hoi_idx = null_col_idx
            
            # Xử lý CauHoi và DanhGia
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and str(parts[cau_hoi_idx]).isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and str(parts[cau_hoi_idx + 1]).isdigit() else None

            # 4 cột góp ý mở (giữ nguyên - KHÔNG XỬ LÝ)
            gopy_start = cau_hoi_idx + 3
            gopy_values = []
            for i in range(4):
                if gopy_start + i < len(parts):
                    gopy_values.append(parts[gopy_start + i])
                else:
                    gopy_values.append(None)

            # Tạo SubmissionID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo hoặc cập nhật dữ liệu cho SubmissionID
            if SubmissionID not in survey_data:
                survey_data[SubmissionID] = {
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
                    'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    # Khởi tạo các cột câu hỏi (dùng integer để tránh .0)
                    'CauHoi1': None, 'CauHoi2': None, 'CauHoi3': None, 'CauHoi4': None,
                    'CauHoi5': None, 'CauHoi6': None, 'CauHoi7': None, 'CauHoi8': None,
                    'CauHoi9': None, 'CauHoi10': None, 'CauHoi11': None, 'CauHoi12': None,
                    # CauHoi13-16 lưu dưới dạng string (giữ nguyên không xử lý)
                    'CauHoi13': None, 'CauHoi14': None, 'CauHoi15': None, 'CauHoi16': None,
                    'Col13': None
                }
            
            # Điền giá trị cho câu hỏi tương ứng
            if CauHoi is not None:
                if 1 <= CauHoi <= 12:
                    # Đảm bảo DanhGia là integer, không phải float
                    if DanhGia is not None:
                        survey_data[SubmissionID][f'CauHoi{CauHoi}'] = int(DanhGia)
                elif CauHoi == 13:
                    # CHỈ LẤY KẾT QUẢ, KHÔNG XỬ LÝ - giữ nguyên giá trị gốc
                    raw_value = gopy_values[0] if len(gopy_values) > 0 and gopy_values[0] is not None else None
                    survey_data[SubmissionID]['CauHoi13'] = raw_value
                    survey_data[SubmissionID]['Col13'] = raw_value  # Giữ nguyên Col13
                elif CauHoi == 14:
                    # CHỈ LẤY KẾT QUẢ, KHÔNG XỬ LÝ
                    survey_data[SubmissionID]['CauHoi14'] = gopy_values[1] if len(gopy_values) > 1 and gopy_values[1] is not None else None
                elif CauHoi == 15:
                    # CHỈ LẤY KẾT QUẢ, KHÔNG XỬ LÝ
                    survey_data[SubmissionID]['CauHoi15'] = gopy_values[2] if len(gopy_values) > 2 and gopy_values[2] is not None else None
                elif CauHoi == 16:
                    # CHỈ LẤY KẾT QUẢ, KHÔNG XỬ LÝ
                    survey_data[SubmissionID]['CauHoi16'] = gopy_values[3] if len(gopy_values) > 3 and gopy_values[3] is not None else None

        except Exception as e:
            logging.warning(f"Bỏ qua dòng {line_num} do lỗi định dạng: {e}")

    # Chuyển đổi thành DataFrame
    survey_df = pd.DataFrame(list(survey_data.values()))
    
    # Sắp xếp lại các cột theo đúng thứ tự yêu cầu (Lop ở vị trí đầu tiên)
    column_order = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 
        'HoDemGV', 'TenGV', 'LopHP', 'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10', 
        'CauHoi11', 'CauHoi12', 'Col13', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ giữ các cột có trong DataFrame
    existing_columns = [col for col in column_order if col in survey_df.columns]
    survey_df = survey_df[existing_columns]
    
    # Đảm bảo các cột CauHoi1-12 là integer (không có .0)
    for i in range(1, 13):
        col_name = f'CauHoi{i}'
        if col_name in survey_df.columns:
            # Chuyển đổi sang Int64 (nullable integer) để tránh .0
            survey_df[col_name] = pd.to_numeric(survey_df[col_name], errors='coerce').astype('Int64')
    
    # CauHoi13-16 giữ nguyên kiểu string (không xử lý)

    logging.info(f"✅ Hoàn tất xử lý: {len(survey_df)} hàng dữ liệu survey")
    
    # In thông tin về kiểu dữ liệu
    logging.info("📊 Kiểu dữ liệu các cột:")
    for i in range(1, 17):
        col_name = f'CauHoi{i}'
        if col_name in survey_df.columns:
            sample_val = survey_df[col_name].iloc[0] if len(survey_df) > 0 else None
            logging.info(f"   {col_name}: {type(sample_val).__name__} - Ví dụ: {sample_val}")
    
    return survey_df

# ==================== MAIN ====================
if __name__ == "__main__":
    logging.info("=" * 90)
    logging.info("     BẮT ĐẦU SURVEY ETL PIPELINE")
    logging.info("=" * 90)

    # Khởi tạo BlobServiceClient
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        logging.info("✅ Kết nối Azure Storage thành công")
    except Exception as e:
        logging.error(f"❌ Lỗi kết nối Azure Storage: {e}")
        sys.exit(1)

    # 1. Download file từ Azure Blob
    download_from_blob(blob_service)

    # 2. Xử lý ETL
    survey_df = extract_and_transform_survey(SURVEY_FILE)

    # 3. Xuất file CSV và Upload lên Azure
    output_file = f"survey_cleaned_{FILE_NAME}.csv"
    survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logging.info(f"📁 Đã xuất file local: {output_file}")

    # ==================== 4. UPLOAD ====================
    print("📤 Uploading to Azure...")
    output = survey_df.to_csv(index=False, encoding='utf-8-sig')
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    
    try:
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
            logging.info("📦 Đã tạo container 'processed-data'")
        
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        logging.info(f"✅ Upload thành công lên Azure: {output_path}")
    except Exception as e:
        logging.error(f"❌ Lỗi upload lên Azure: {e}")
        sys.exit(1)

    # 5. In 10 dòng mẫu để kiểm tra
    print("\n" + "="*130)
    print("📋 10 DÒNG MẪU - SURVEY DATA (PIVOTED)")
    print("="*130)
    print(survey_df.head(10).to_string(index=False))
    
    # Kiểm tra giá trị của CauHoi5 và CauHoi13-16
    print("\n" + "="*130)
    print("🔍 KIỂM TRA DỮ LIỆU CÂU HỎI")
    print("="*130)
    if 'CauHoi5' in survey_df.columns:
        print(f"CauHoi5 - Kiểu: {survey_df['CauHoi5'].dtype}, Giá trị mẫu: {survey_df['CauHoi5'].iloc[0] if len(survey_df) > 0 else 'N/A'}")
    for i in range(13, 17):
        col = f'CauHoi{i}'
        if col in survey_df.columns:
            print(f"{col} - Kiểu: {survey_df[col].dtype}, Giá trị mẫu: {survey_df[col].iloc[0] if len(survey_df) > 0 else 'N/A'}")

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
