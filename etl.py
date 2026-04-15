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
SURVEY_FILE = os.environ.get("SURVEY_FILE")   # Ví dụ: khaosat_252.csv

if not SURVEY_FILE or not SEMESTER:
    logging.error("Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

logging.info(f"Semester         : {SEMESTER}")
logging.info(f"Survey file      : {SURVEY_FILE}")
logging.info(f"File name for ID : {FILE_NAME}")

# ==================== DOWNLOAD FILE TỪ AZURE BLOB ====================
def download_from_blob():
    if not CONNECTION_STRING:
        logging.error("Thiếu CONNECTION_STRING")
        sys.exit(1)

    logging.info(f"Đang tải file từ Azure Blob: {SURVEY_FILE}")

    try:
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        
        # Container thường là "processed-data" hoặc bạn cần chỉnh nếu khác
        container_name = "processed-data"
        # Nếu file nằm trong thư mục con theo semester
        blob_path = f"{SEMESTER}/{SURVEY_FILE}" if SEMESTER else SURVEY_FILE

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

        # Tải về thư mục hiện tại
        with open(SURVEY_FILE, "wb") as download_file:
            download_stream = blob_client.download_blob()
            download_file.write(download_stream.readall())

        logging.info(f"✅ Tải thành công: {SURVEY_FILE} ({os.path.getsize(SURVEY_FILE)/1024:.1f} KB)")
        
    except Exception as e:
        logging.error(f"❌ Lỗi tải file từ Blob: {e}")
        logging.error("Kiểm tra: Container name, đường dẫn blob, và quyền truy cập.")
        sys.exit(1)

# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    logging.info("Bắt đầu xử lý dữ liệu raw...")

    submissions = []
    responses = []

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            MaSV = parts[1]

            # Tìm NgaySinh
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None:
                continue

            # Họ tên SV
            ho_ten_sv = parts[2:ngay_sinh_idx]
            HoDem = ho_ten_sv[0] if ho_ten_sv else ''
            Ten = ','.join(ho_ten_sv[1:]) if len(ho_ten_sv) > 1 else ''

            # Tên học phần
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''

            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Họ tên GV
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                continue

            ho_ten_gv = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv[0] if ho_ten_gv else ''
            TenGV = ','.join(ho_ten_gv[1:]) if len(ho_ten_gv) > 1 else ''

            LopHP = parts[LopHP_idx]

            # Cột sau
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and str(parts[cau_hoi_idx]).isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and str(parts[cau_hoi_idx + 1]).isdigit() else None

            gopy_start = cau_hoi_idx + 3
            gopy_values = parts[gopy_start:gopy_start+4]
            CauHoi13, CauHoi14, CauHoi15, CauHoi16 = (gopy_values + [None]*4)[:4]

            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            submissions.append({
                'SubmissionID': SubmissionID,
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
                'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })

            if CauHoi:
                if 1 <= CauHoi <= 12:
                    responses.append({'SubmissionID': SubmissionID, 'CauHoi': CauHoi, 'DanhGia': DanhGia, 'GopY': None})
                elif 13 <= CauHoi <= 16:
                    gopy = gopy_values[CauHoi - 13]
                    responses.append({'SubmissionID': SubmissionID, 'CauHoi': CauHoi, 'DanhGia': None, 'GopY': gopy})

        except:
            logging.warning(f"Bỏ qua dòng {line_num}")

    submissions_df = pd.DataFrame(submissions).drop_duplicates(subset=['SubmissionID']).reset_index(drop=True)
    responses_df = pd.DataFrame(responses).reset_index(drop=True)

    logging.info(f"Hoàn tất ETL → Submissions: {len(submissions_df)} | Responses: {len(responses_df)}")
    return submissions_df, responses_df


# ==================== MAIN ====================
if __name__ == "__main__":
    logging.info("=" * 90)
    logging.info("     BẮT ĐẦU ETL PIPELINE")
    logging.info("=" * 90)

    # Bước 1: Tải file từ Azure Blob
    download_from_blob()

    # Bước 2: Xử lý ETL
    submissions_df, responses_df = extract_and_transform_survey(SURVEY_FILE)

    # Xuất file CSV
    sub_file = f"submissions_cleaned_{FILE_NAME}.csv"
    res_file = f"responses_cleaned_{FILE_NAME}.csv"

    submissions_df.to_csv(sub_file, index=False, encoding='utf-8-sig')
    responses_df.to_csv(res_file, index=False, encoding='utf-8-sig')

    logging.info(f"Đã xuất: {sub_file} và {res_file}")

    # In mẫu
    print("\n" + "="*100)
    print("10 DÒNG MẪU - SUBMISSIONS")
    print("="*100)
    print(submissions_df.head(10).to_string(index=False))

    print("\n" + "="*100)
    print("10 DÒNG MẪU - RESPONSES")
    print("="*100)
    print(responses_df.head(10).to_string(index=False))

    logging.info("=== HOÀN TẤT ETL ===")
