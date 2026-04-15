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
    # Dictionary để theo dõi các câu hỏi đã được xử lý cho mỗi SubmissionID
    processed_questions = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # Lấy Lop từ phần tử đầu tiên của dòng (loại bỏ ký tự BOM nếu có)
            Lop = parts[0].lstrip('\ufeff') if len(parts) > 0 else ''
            
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

            # Đọc số thứ tự câu hỏi (sau LopHP)
            cauhoi_idx = LopHP_idx + 1
            cauhoi_num = int(parts[cauhoi_idx]) if cauhoi_idx < len(parts) and parts[cauhoi_idx].isdigit() else None
            
            # Đọc điểm đánh giá
            danhgia_idx = cauhoi_idx + 1
            danhgia = int(parts[danhgia_idx]) if danhgia_idx < len(parts) and parts[danhgia_idx].isdigit() else None
            
            # Đọc 4 cột góp ý (câu hỏi 13-16) - nằm ở cuối mỗi dòng
            gopy_start_idx = danhgia_idx + 1
            gopy_values = []
            for i in range(4):
                if gopy_start_idx + i < len(parts):
                    val = parts[gopy_start_idx + i]
                    gopy_values.append(val if val and val != 'NULL' else None)
                else:
                    gopy_values.append(None)

            # Tạo SubmissionID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo nếu chưa có
            if SubmissionID not in survey_data:
                survey_data[SubmissionID] = {
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
                    'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    # Khởi tạo các cột câu hỏi
                    'CauHoi1': None, 'CauHoi2': None, 'CauHoi3': None, 'CauHoi4': None,
                    'CauHoi5': None, 'CauHoi6': None, 'CauHoi7': None, 'CauHoi8': None,
                    'CauHoi9': None, 'CauHoi10': None, 'CauHoi11': None, 'CauHoi12': None,
                    'CauHoi13': None, 'CauHoi14': None, 'CauHoi15': None, 'CauHoi16': None,
                    'Col13': None
                }
                processed_questions[SubmissionID] = set()
            
            # Điền giá trị cho câu hỏi
            if cauhoi_num is not None:
                # Chỉ xử lý nếu câu hỏi này chưa được xử lý
                if cauhoi_num not in processed_questions[SubmissionID]:
                    if 1 <= cauhoi_num <= 12:
                        # Điền điểm đánh giá cho câu hỏi 1-12
                        if danhgia is not None:
                            survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] = int(danhgia)
                            processed_questions[SubmissionID].add(cauhoi_num)
                    elif 13 <= cauhoi_num <= 16:
                        # Điền góp ý cho câu hỏi 13-16 (chỉ lấy 1 lần duy nhất)
                        idx = cauhoi_num - 13
                        if idx < len(gopy_values) and survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] is None:
                            val = gopy_values[idx]
                            survey_data[SubmissionID][f'CauHoi{cauhoi_num}'] = val
                            if cauhoi_num == 13:
                                survey_data[SubmissionID]['Col13'] = val
                            processed_questions[SubmissionID].add(cauhoi_num)
            
            # TRƯỜNG HỢP ĐẶC BIỆT: Nếu là câu hỏi 12, lấy giá trị góp ý 13-16 từ dòng này
            if cauhoi_num == 12:
                for i in range(4):
                    cauhoi_13_16 = 13 + i
                    if cauhoi_13_16 not in processed_questions[SubmissionID]:
                        val = gopy_values[i] if i < len(gopy_values) else None
                        if val is not None and survey_data[SubmissionID][f'CauHoi{cauhoi_13_16}'] is None:
                            survey_data[SubmissionID][f'CauHoi{cauhoi_13_16}'] = val
                            if cauhoi_13_16 == 13:
                                survey_data[SubmissionID]['Col13'] = val
                            processed_questions[SubmissionID].add(cauhoi_13_16)

        except Exception as e:
            logging.warning(f"Bỏ qua dòng {line_num} do lỗi định dạng: {e}")
            continue

    # SAU KHI XỬ LÝ XONG: Kiểm tra và xử lý các sinh viên không có đủ 12 câu hỏi
    logging.info("🔍 Kiểm tra dữ liệu không đầy đủ...")
    incomplete_students = []
    for submission_id, data in survey_data.items():
        missing_questions = []
        for i in range(1, 13):
            if data[f'CauHoi{i}'] is None:
                missing_questions.append(i)
        if missing_questions:
            incomplete_students.append({
                'SubmissionID': submission_id,
                'MaSV': data['MaSV'],
                'Missing': missing_questions,
                'Count': len(missing_questions)
            })
            logging.warning(f"⚠️ SV {data['MaSV']} thiếu {len(missing_questions)} câu hỏi: {missing_questions}")
    
    if incomplete_students:
        logging.info(f"📊 Tổng số sinh viên bị thiếu dữ liệu: {len(incomplete_students)}")
    else:
        logging.info("✅ Tất cả sinh viên đều có đủ 12 câu hỏi")

    # Chuyển đổi thành DataFrame
    survey_df = pd.DataFrame(list(survey_data.values()))
    
    # Sắp xếp lại các cột theo đúng thứ tự yêu cầu
    column_order = [
        'SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 
        'HoDemGV', 'TenGV', 'LopHP', 'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10', 
        'CauHoi11', 'CauHoi12', 'Col13', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ giữ các cột có trong DataFrame
    existing_columns = [col for col in column_order if col in survey_df.columns]
    survey_df = survey_df[existing_columns]
    
    # Chuyển đổi CauHoi1-12 sang Int64 (nullable integer) để tránh .0
    for i in range(1, 13):
        col_name = f'CauHoi{i}'
        if col_name in survey_df.columns:
            survey_df[col_name] = pd.to_numeric(survey_df[col_name], errors='coerce').astype('Int64')
    
    logging.info(f"✅ Hoàn tất xử lý: {len(survey_df)} hàng dữ liệu survey")
    
    return survey_df, incomplete_students

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
    survey_df, incomplete_students = extract_and_transform_survey(SURVEY_FILE)

    # 3. Xuất file CSV và Upload lên Azure
    output_file = f"survey_cleaned_{FILE_NAME}.csv"
    survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    logging.info(f"📁 Đã xuất file local: {output_file}")
    
    # Xuất file báo cáo sinh viên thiếu dữ liệu
    if incomplete_students:
        incomplete_df = pd.DataFrame(incomplete_students)
        incomplete_file = f"incomplete_data_{FILE_NAME}.csv"
        incomplete_df.to_csv(incomplete_file, index=False, encoding='utf-8-sig')
        logging.info(f"📁 Đã xuất file báo cáo thiếu dữ liệu: {incomplete_file}")

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
        
        # Upload file báo cáo nếu có
        if incomplete_students:
            incomplete_output = incomplete_df.to_csv(index=False, encoding='utf-8-sig')
            incomplete_path = f"{SEMESTER}/incomplete_{SURVEY_FILE.replace('.csv', '_report.csv')}"
            processed_container.get_blob_client(incomplete_path).upload_blob(incomplete_output, overwrite=True)
            logging.info(f"✅ Upload báo cáo thiếu dữ liệu: {incomplete_path}")
            
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
    
    # Kiểm tra dữ liệu chi tiết cho sinh viên đầu tiên
    if len(survey_df) > 0:
        print("\n" + "="*150)
        print("🔍 KIỂM TRA CHI TIẾT SINH VIÊN ĐẦU TIÊN")
        print("="*150)
        first_row = survey_df.iloc[0]
        print(f"SubmissionID: {first_row.get('SubmissionID', 'N/A')}")
        print(f"Lop: {first_row.get('Lop', 'N/A')}")
        print(f"MaSV: {first_row.get('MaSV', 'N/A')}")
        print(f"HoDem: {first_row.get('HoDem', 'N/A')}")
        print(f"Ten: {first_row.get('Ten', 'N/A')}")
        print(f"LopHP: {first_row.get('LopHP', 'N/A')}")
        print("\n📊 Điểm đánh giá các câu hỏi 1-12:")
        for i in range(1, 13):
            col = f'CauHoi{i}'
            val = first_row.get(col, 'N/A')
            print(f"  {col}: {val} (type: {type(val).__name__})")
        
        print("\n💬 Góp ý câu hỏi 13-16:")
        for i in range(13, 17):
            col = f'CauHoi{i}'
            val = first_row.get(col, 'N/A')
            print(f"  {col}: {val} (type: {type(val).__name__})")
    
    # In báo cáo thiếu dữ liệu
    if incomplete_students:
        print("\n" + "="*150)
        print("⚠️ DANH SÁCH SINH VIÊN THIẾU DỮ LIỆU")
        print("="*150)
        for student in incomplete_students[:10]:  # Chỉ hiển thị 10 sinh viên đầu
            print(f"MaSV: {student['MaSV']} - Thiếu {student['Count']} câu hỏi: {student['Missing']}")

    logging.info("=" * 90)
    logging.info("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    logging.info("=" * 90)
