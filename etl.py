import os
import sys
import logging
from datetime import datetime

# ==================== CẤU HÌNH THƯ VIỆN ====================
import pandas as pd

# Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

# ==================== BIẾN MÔI TRƯỜNG (BẮT BUỘC) ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

# Kiểm tra các biến môi trường bắt buộc
missing_vars = []
if not CONNECTION_STRING:
    missing_vars.append("CONNECTION_STRING")
if not SEMESTER:
    missing_vars.append("SEMESTER")
if not SURVEY_FILE:
    missing_vars.append("SURVEY_FILE")

if missing_vars:
    logging.error("Thiếu biến môi trường bắt buộc: " + ", ".join(missing_vars))
    logging.error("Vui lòng thiết lập các biến này trước khi chạy.")
    sys.exit(1)

# Lấy tên file để tạo SubmissionID
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

logging.info(f"File input       : {SURVEY_FILE}")
logging.info(f"Semester         : {SEMESTER}")
logging.info(f"File name for ID : {FILE_NAME}")

# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    if not os.path.exists(file_path):
        logging.error(f"Không tìm thấy file: {file_path}")
        sys.exit(1)

    logging.info("Đang đọc và xử lý file raw...")

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
            # Các trường cố định
            Lop = parts[0]
            MaSV = parts[1]

            # Tìm NgaySinh
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if isinstance(x, str) and '/' in x and len(x.split('/')) == 3), None)
            if ngay_sinh_idx is None:
                logging.warning(f"Dòng {line_num}: Không tìm thấy NgaySinh")
                continue

            # Họ tên Sinh viên
            ho_ten_sv_list = parts[2:ngay_sinh_idx]
            HoDem = ho_ten_sv_list[0] if ho_ten_sv_list else ''
            Ten = ','.join(ho_ten_sv_list[1:]) if len(ho_ten_sv_list) > 1 else ''

            # Tên học phần
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''

            # Tìm MaGV
            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                logging.warning(f"Dòng {line_num}: Không tìm thấy MaGV")
                continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Họ tên Giảng viên
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                logging.warning(f"Dòng {line_num}: Không tìm thấy LopHP")
                continue

            ho_ten_gv_list = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv_list[0] if ho_ten_gv_list else ''
            TenGV = ','.join(ho_ten_gv_list[1:]) if len(ho_ten_gv_list) > 1 else ''

            LopHP = parts[LopHP_idx]

            # Cột sau LopHP
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and str(parts[cau_hoi_idx]).isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if (cau_hoi_idx + 1 < len(parts) and str(parts[cau_hoi_idx + 1]).isdigit()) else None

            # 4 cột góp ý mở (giữ nguyên nguyên bản)
            gopy_start = cau_hoi_idx + 3
            CauHoi13 = parts[gopy_start] if gopy_start < len(parts) else None
            CauHoi14 = parts[gopy_start + 1] if gopy_start + 1 < len(parts) else None
            CauHoi15 = parts[gopy_start + 2] if gopy_start + 2 < len(parts) else None
            CauHoi16 = parts[gopy_start + 3] if gopy_start + 3 < len(parts) else None

            # Tạo SubmissionID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Submission
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

            # Response
            if CauHoi:
                if 1 <= CauHoi <= 12:
                    responses.append({
                        'SubmissionID': SubmissionID,
                        'CauHoi': CauHoi,
                        'DanhGia': DanhGia,
                        'GopY': None
                    })
                elif 13 <= CauHoi <= 16:
                    gopy = [CauHoi13, CauHoi14, CauHoi15, CauHoi16][CauHoi - 13]
                    responses.append({
                        'SubmissionID': SubmissionID,
                        'CauHoi': CauHoi,
                        'DanhGia': None,
                        'GopY': gopy
                    })

        except Exception as e:
            logging.error(f"Lỗi xử lý dòng {line_num}: {e}")

    # Tạo DataFrame
    submissions_df = pd.DataFrame(submissions).drop_duplicates(subset=['SubmissionID']).reset_index(drop=True)
    responses_df = pd.DataFrame(responses).reset_index(drop=True)

    logging.info(f"Hoàn tất xử lý: {len(submissions_df)} submissions | {len(responses_df)} responses")

    return submissions_df, responses_df


# ==================== CHẠY CHƯƠNG TRÌNH ====================
if __name__ == "__main__":
    logging.info("=" * 90)
    logging.info("          BẮT ĐẦU XỬ LÝ DỮ LIỆU KHẢO SÁT")
    logging.info("=" * 90)

    submissions_df, responses_df = extract_and_transform_survey(SURVEY_FILE)

    # Xuất file CSV
    sub_file = f"submissions_cleaned_{FILE_NAME}.csv"
    res_file = f"responses_cleaned_{FILE_NAME}.csv"

    submissions_df.to_csv(sub_file, index=False, encoding='utf-8-sig')
    responses_df.to_csv(res_file, index=False, encoding='utf-8-sig')

    logging.info(f"Đã xuất file:")
    logging.info(f"   • {sub_file}")
    logging.info(f"   • {res_file}")

    # In 10 dòng mẫu để kiểm tra
    print("\n" + "="*100)
    print("10 DÒNG DỮ LIỆU MẪU - SUBMISSIONS")
    print("="*100)
    print(submissions_df.head(10).to_string(index=False))

    print("\n" + "="*100)
    print("10 DÒNG DỮ LIỆU MẪU - RESPONSES")
    print("="*100)
    print(responses_df.head(10).to_string(index=False))

    logging.info("=" * 90)
    logging.info("HOÀN TẤT XỬ LÝ DỮ LIỆU KHẢO SÁT")
    logging.info("=" * 90)
