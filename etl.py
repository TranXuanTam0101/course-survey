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

            # Tìm NgaySinh (có định dạng dd/mm/yyyy)
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None:
                continue

            # Họ tên Sinh viên
            ho_ten_sv = parts[2:ngay_sinh_idx]
            HoDem = ho_ten_sv[0] if ho_ten_sv else ''
            Ten = ' '.join(ho_ten_sv[1:]) if len(ho_ten_sv) > 1 else ''

            # Mã học phần
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''

            # Tìm MaGV (số có 7 chữ số)
            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                continue

            # Tên học phần (các phần tử giữa MaHP và MaGV)
            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Tìm LopHP (có dạng xxx_xxx chứa dấu _ và số)
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                continue

            # Họ tên Giảng viên (các phần tử giữa MaGV và LopHP)
            ho_ten_gv = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv[0] if ho_ten_gv else ''
            TenGV = ' '.join(ho_ten_gv[1:]) if len(ho_ten_gv) > 1 else ''

            LopHP = parts[LopHP_idx]

            # Tìm vị trí bắt đầu của câu hỏi (sau LopHP)
            # Bỏ qua cột NULL nếu có
            current_idx = LopHP_idx + 1
            if current_idx < len(parts) and parts[current_idx].upper() == 'NULL':
                current_idx += 1
            
            # Đọc số thứ tự câu hỏi (1-16)
            cauhoi_num = None
            if current_idx < len(parts) and parts[current_idx].isdigit():
                cauhoi_num = int(parts[current_idx])
                current_idx += 1
            
            # Đọc điểm đánh giá (nếu có)
            danhgia = None
            if current_idx < len(parts) and parts[current_idx] and parts[current_idx] != 'NULL':
                if parts[current_idx].isdigit():
                    danhgia = int(parts[current_idx])
                else:
                    danhgia = parts[current_idx]  # Có thể là text
                current_idx += 1
            
            # Bỏ qua cột NULL tiếp theo nếu có
            if current_idx < len(parts) and parts[current_idx].upper() == 'NULL':
                current_idx += 1
            
            # Đọc 4 cột góp ý (cauhoi 13-16)
            gopy_values = []
            for i in range(4):
                if current_idx + i < len(parts):
                    val = parts[current_idx + i]
                    gopy_values.append(val if val and val != 'NULL' else None)
                else:
                    gopy_values.append(None)

            # Tạo SubmissionID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo hoặc cập nhật dữ liệu cho SubmissionID
            if SubmissionID not in survey_data:
                survey_data[SubmissionID] = {
                    'SubmissionID': SubmissionID,  # Thêm cột SubmissionID
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
                    # Khởi tạo các cột câu hỏi
                    'CauHoi1': None, 'CauHoi2': None, 'CauHoi3': None, 'CauHoi4': None,
                    'CauHoi5': None, 'CauHoi6': None, 'CauHoi7': None, 'CauHoi8': None,
                    'CauHoi9': None, 'CauHoi10': None, 'CauHoi11': None, 'CauHoi12': None,
                    'CauHoi13': None, 'CauHoi14': None, 'CauHoi15': None, 'CauHoi16': None,
                    'Col13': None
                }
            
            # Điền giá trị cho câu hỏi tương ứng
            if cauhoi_num is not None:
                if 1 <= cauhoi_num <= 12:
                    # Câu hỏi 1-12: lấy điểm đánh giá
                    if danhgia is not None and str(danhgia).isdigit():
                        survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] = int(danhgia)
                    else:
                        survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] = danhgia
                elif 13 <= cauhoi_num <= 16:
                    # Câu hỏi 13-16: lấy từ gopy_values
                    idx = cauhoi_num - 13
                    if idx < len(gopy_values):
                        survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] = gopy_values[idx]
                        if cauhoi_num == 13:
                            survey_data[SubmissionID]['Col13'] = gopy_values[idx]

        except Exception as e:
            logging.warning(f"Bỏ qua dòng {line_num} do lỗi định dạng: {e}")
            continue

    # Chuyển đổi thành DataFrame
    survey_df = pd.DataFrame(list(survey_data.values()))
    
    # Sắp xếp lại các cột theo đúng thứ tự yêu cầu
    column_order = [
        'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 
        'HoDemGV', 'TenGV', 'LopHP', 'SubmissionID', 'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10', 
        'CauHoi11', 'CauHoi12', 'Col13', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ giữ các cột có trong DataFrame
    existing_columns = [col for col in column_order if col in survey_df.columns]
    survey_df = survey_df[existing_columns]
    
    # Đảm bảo các cột CauHoi1-12 là integer nếu có thể
    for i in range(1, 13):
        col_name = f'CauHoi{i}'
        if col_name in survey_df.columns:
            survey_df[col_name] = pd.to_numeric(survey_df[col_name], errors='ignore')
    
    logging.info(f"✅ Hoàn tất xử lý: {len(survey_df)} hàng dữ liệu survey")
    
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
    print("\n" + "="*150)
    print("📋 DỮ LIỆU MẪU - SURVEY DATA (PIVOTED)")
    print("="*150)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(survey_df.head(10).to_string(index=False))
    
    # Kiểm tra dữ liệu cho một sinh viên cụ thể
    if len(survey_df) > 0:
        print("\n" + "="*150)
        print("🔍 KIỂM TRA DỮ LIỆU SINH VIÊN ĐẦU TIÊN")
        print("="*150)
        first_row = survey_df.iloc[0]
        print(f"SubmissionID: {first_row.get('SubmissionID', 'N/A')}")
        print(f"Lop: {first_row.get('Lop', 'N/A')}")
        print(f"MaSV: {first_row.get('MaSV', 'N/A')}")
        print(f"HoDem: {first_row.get('HoDem', 'N/A')}")
        print(f"Ten: {first_row.get('Ten', 'N/A')}")
        print("\nĐiểm đánh giá các câu hỏi:")
        for i in range(1, 17):
            col = f'CauHoi{i}'
            val = first_row.get(col, 'N/A')
            print(f"  {col}: {val}")

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
