import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ==================== BIẾN MÔI TRƯỜNG ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("❌ Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

print(f"Semester    : {SEMESTER}")
print(f"Survey File : {SURVEY_FILE}")
print(f"File Name   : {FILE_NAME}")

# ==================== DOWNLOAD FILE TỪ AZURE BLOB ====================
def download_from_blob(blob_service):
    print("📥 Connecting to Azure Storage...")
    try:
        container_client = blob_service.get_container_client("rawdata")
        blob_client = container_client.get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

        print(f"📤 Downloading blob: {SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()

        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        print(f"✅ Download thành công: {SURVEY_FILE} ({len(data)/1024:.1f} KB)")
    except Exception as e:
        print(f"❌ Lỗi download từ Azure Blob: {e}")
        sys.exit(1)

# ==================== UPLOAD FILE LÊN AZURE BLOB ====================
def upload_to_blob(blob_service, df, output_path):
    print("📤 Uploading to Azure...")
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
            print("📦 Đã tạo container 'processed-data'")
        
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        print(f"✅ Upload thành công lên Azure: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Lỗi upload lên Azure: {e}")
        return False

# ==================== ETL: EXTRACT + TRANSFORM ====================
def extract_and_transform_survey(file_path: str):
    print("🔄 Bắt đầu xử lý dữ liệu raw...")
    
    # Dictionary lưu dữ liệu theo SubmissionID
    temp_data = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # 1. Trích xuất thông tin định danh
            Lop = parts[0]
            MaSV = parts[1]

            # Tìm index NgaySinh (dd/mm/yyyy)
            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None: continue

            NgaySinh = parts[ngay_sinh_idx]
            HoDem = parts[2]
            Ten = ' '.join(parts[3:ngay_sinh_idx])

            # Học phần
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx]
            
            # Tìm MaGV (Số >= 6 chữ số)
            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None: continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Tìm LopHP (có chứa dấu _)
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) if '_' in str(parts[i])), None)
            if LopHP_idx is None: continue

            # Giảng viên
            ho_ten_gv_parts = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv_parts[0] if ho_ten_gv_parts else ''
            TenGV = ' '.join(ho_ten_gv_parts[1:]) if len(ho_ten_gv_parts) > 1 else ''
            
            LopHP = parts[LopHP_idx]

            # 2. Xử lý khảo sát
            # Cấu trúc: [LopHP], [STT Câu hỏi], [Điểm], [NULL], [Q13], [Q14], [Q15], [Q16]
            stt_idx = LopHP_idx + 1
            score_idx = LopHP_idx + 2
            null_idx = LopHP_idx + 3 # Đây là cột chắc chắn NULL theo yêu cầu
            
            stt_cau_hoi = int(parts[stt_idx])
            diem_likert = int(parts[score_idx])

            # Lấy 4 cột sau cột NULL (index: null_idx + 1)
            start_open_qs = null_idx + 1
            gopy_raw = parts[start_open_qs : start_open_qs + 4]
            # Xử lý làm sạch: chuyển các giá trị rác về None
            gopy_cleaned = []
            for val in gopy_raw:
                v = val.strip()
                if v.lower() in ['null', 'k', 'không', 'khong', '']:
                    gopy_cleaned.append(None)
                else:
                    gopy_cleaned.append(v)

            # 3. Gom nhóm dữ liệu vào Dictionary
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            if SubmissionID not in temp_data:
                temp_data[SubmissionID] = {
                    'SubmissionID': SubmissionID,
                    'Lop': Lop, 'MaSV': MaSV, 'HoDem': HoDem, 'Ten': Ten,
                    'NgaySinh': NgaySinh, 'MaHP': MaHP, 'TenHP': TenHP,
                    'MaGV': MaGV, 'HoDemGV': HoDemGV, 'TenGV': TenGV,
                    'LopHP': LopHP, 'Semester': SEMESTER,
                    'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'Scores': {}, # Lưu Q1-Q12
                    'OpenQs': [None, None, None, None] # Lưu Q13-Q16
                }
            
            # Lưu điểm câu hỏi Likert
            if 1 <= stt_cau_hoi <= 12:
                temp_data[SubmissionID]['Scores'][stt_cau_hoi] = diem_likert
            
            # Lưu câu hỏi mở (Cập nhật nếu chưa có hoặc ưu tiên lấy dữ liệu có nghĩa)
            for i in range(4):
                if gopy_cleaned[i] is not None:
                    temp_data[SubmissionID]['OpenQs'][i] = gopy_cleaned[i]

        except Exception as e:
            print(f"⚠️ Lỗi dòng {line_num}: {e}")

    # 4. Chuyển đổi sang DataFrame hoàn chỉnh
    final_rows = []
    for sid, data in temp_data.items():
        row = {
            'SubmissionID': sid,
            'Lop': data['Lop'], 'MaSV': data['MaSV'], 'HoDem': data['HoDem'], 'Ten': data['Ten'],
            'NgaySinh': data['NgaySinh'], 'MaHP': data['MaHP'], 'TenHP': data['TenHP'],
            'MaGV': data['MaGV'], 'HoDemGV': data['HoDemGV'], 'TenGV': data['TenGV'],
            'LopHP': data['LopHP'], 'Semester': data['Semester'], 'SubmittedAt': data['SubmittedAt']
        }
        # Thêm Q1 - Q12
        for i in range(1, 13):
            row[f'CauHoi{i}'] = data['Scores'].get(i, None)
        
        # Thêm Q13 - Q16
        for i in range(4):
            row[f'CauHoi{13+i}'] = data['OpenQs'][i]
            
        final_rows.append(row)

    df = pd.DataFrame(final_rows)

    # Ép kiểu điểm Likert sang Int64 (để tránh .0)
    likert_cols = [f'CauHoi{i}' for i in range(1, 13)]
    for col in likert_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

    print(f"✅ Hoàn tất xử lý: {len(df)} SubmissionID")
    return df

# ==================== MAIN ====================
if __name__ == "__main__":
    print("=" * 90)
    print("     BẮT ĐẦU SURVEY ETL PIPELINE")
    print("=" * 90)

    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("✅ Kết nối Azure Storage thành công")
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure Storage: {e}")
        sys.exit(1)

    # 1. Tải dữ liệu
    download_from_blob(blob_service)

    # 2. Xử lý
    survey_df = extract_and_transform_survey(SURVEY_FILE)

    if not survey_df.empty:
        # 3. Lưu local
        output_file = f"survey_cleaned_{FILE_NAME}.csv"
        survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # 4. Tải lên Azure
        output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
        upload_to_blob(blob_service, survey_df, output_path)

        # 5. Review
        print("\n📋 DỮ LIỆU MẪU (PIVOTED):")
        pd.set_option('display.max_columns', None)
        print(survey_df.head(5))
    else:
        print("❌ Không có dữ liệu để xử lý.")

    print("=" * 90)
    print("🎉 KẾT THÚC PIPELINE")
    print("=" * 90)
