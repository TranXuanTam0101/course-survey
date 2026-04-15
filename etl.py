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

    except Exception as e:
        logging.error(f"❌ Lỗi download từ Azure Blob: {e}")
        sys.exit(1)


# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    logging.info("🔄 Bắt đầu xử lý dữ liệu (Pivot 12 dòng thành 1 hàng)...")

    # Dictionary để nhóm dữ liệu theo cặp (Sinh viên, Lớp học phần)
    data_map = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # Nhận diện các thông tin cố định
            Lop = parts[0]
            MaSV = parts[1]
            
            # Tìm NgaySinh để xác định vị trí các cột động
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None:
                continue

            HoDem = parts[2]
            Ten = " ".join(parts[3:ngay_sinh_idx])
            NgaySinh = parts[ngay_sinh_idx]

            MaHP = parts[ngay_sinh_idx + 1]
            
            # Tìm MaGV (số >= 6 chữ số)
            MaGV_idx = next((i for i in range(ngay_sinh_idx + 2, len(parts)) 
                           if parts[i].isdigit() and len(parts[i]) >= 6), None)
            TenHP = " ".join(parts[ngay_sinh_idx + 2:MaGV_idx])
            MaGV = parts[MaGV_idx]

            # Tìm LopHP
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in parts[i]), None)
            HoDemGV = parts[MaGV_idx + 1]
            TenGV = " ".join(parts[MaGV_idx + 2:LopHP_idx])
            LopHP = parts[LopHP_idx]

            # Thông tin câu hỏi likert (1-12)
            cau_hoi_id = parts[LopHP_idx + 1] # Giá trị 1, 2, ..., 12
            danh_gia_val = parts[LopHP_idx + 2] # Điểm đánh giá (5, 4, ...)
            
            # 4 câu hỏi mở cuối cùng (lặp lại ở mỗi dòng)
            gopy_start = LopHP_idx + 4
            gopy_values = parts[gopy_start:]

            # Tạo key định danh duy nhất cho mỗi bài khảo sát của 1 SV cho 1 GV
            group_key = f"{MaSV}_{LopHP}_{MaGV}"

            if group_key not in data_map:
                data_map[group_key] = {
                    'SubmissionID': f"{group_key}_{FILE_NAME}",
                    'Lop': Lop, 'MaSV': MaSV, 'HoDem': HoDem, 'Ten': Ten, 'NgaySinh': NgaySinh,
                    'MaHP': MaHP, 'TenHP': TenHP, 'MaGV': MaGV, 'HoDemGV': HoDemGV, 'TenGV': TenGV,
                    'LopHP': LopHP, 'Col13': 'NULL',
                    'CauHoi13': gopy_values[0] if len(gopy_values) > 0 else '',
                    'CauHoi14': gopy_values[1] if len(gopy_values) > 1 else '',
                    'CauHoi15': gopy_values[2] if len(gopy_values) > 2 else '',
                    'CauHoi16': gopy_values[3] if len(gopy_values) > 3 else ''
                }
                # Khởi tạo các cột CauHoi1 -> CauHoi12 là rỗng
                for i in range(1, 13):
                    data_map[group_key][f'CauHoi{i}'] = None

            # Điền giá trị đánh giá vào cột tương ứng
            data_map[group_key][f'CauHoi{cau_hoi_id}'] = danh_gia_val

        except Exception as e:
            logging.warning(f"Bỏ qua dòng {line_num} do lỗi định dạng: {e}")

    # Chuyển từ dictionary sang DataFrame
    df = pd.DataFrame(data_map.values())

    # Sắp xếp lại thứ tự cột đúng yêu cầu
    ordered_columns = [
        'SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP',
        'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10', 'CauHoi11', 'CauHoi12',
        'Col13', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ lấy các cột có trong ordered_columns (tránh lỗi nếu dữ liệu thiếu cột)
    df = df.reindex(columns=ordered_columns)

    logging.info(f"✅ Hoàn tất xử lý: {len(df)} hàng dữ liệu sau khi pivot.")
    return df


# ==================== MAIN ====================
if __name__ == "__main__":
    logging.info("=" * 90)
    logging.info("     BẮT ĐẦU SURVEY ETL PIPELINE (PIVOT MODE)")
    logging.info("=" * 90)

    # 1. Download file từ Azure Blob
    download_from_blob()

    # 2. Xử lý ETL (Trả về 1 dataframe duy nhất đã pivot)
    final_df = extract_and_transform_survey(SURVEY_FILE)

    # 3. Xuất file CSV
    output_file = f"survey_cleaned_pivot_{FILE_NAME}.csv"
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')

    logging.info(f"📁 Đã xuất file: {output_file}")

    # 4. In 5 dòng mẫu để kiểm tra
    print("\n" + "="*110)
    print("📋 5 DÒNG MẪU DỮ LIỆU ĐÃ PIVOT")
    print("="*110)
    if not final_df.empty:
        print(final_df.head(5).to_string(index=False))

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
