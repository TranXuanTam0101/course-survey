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
print(f"Semester: {SEMESTER} | File: {SURVEY_FILE}")

# ==================== DOWNLOAD ====================
def download_from_blob(blob_service):
    print("📥 Downloading...")
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        print(f"✅ Downloaded: {len(data)/1024:.1f} KB")
    except Exception as e:
        print(f"❌ Download error: {e}")
        sys.exit(1)

# ==================== UPLOAD ====================
def upload_to_blob(blob_service, df, output_path):
    print("📤 Uploading...")
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        container = blob_service.get_container_client("processed-data")
        if not container.exists():
            container.create_container()
        container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        print(f"✅ Uploaded: {output_path}")
        return True
    except Exception as e:
        print(f"❌ Upload error: {e}")
        return False

# ==================== ETL PROCESSING ====================
def extract_and_transform_survey(file_path: str):
    print("🔄 Processing...")
    
    temp_data = {}
    final_data = {}

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line:
            continue

        parts = [p.strip() for p in line.split(',')]

        try:
            # Lấy thông tin cơ bản
            Lop = parts[0]
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

            # Tìm MaGV
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''
            
            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            # Tìm LopHP
            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                continue

            ho_ten_gv = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv[0] if ho_ten_gv else ''
            TenGV = ','.join(ho_ten_gv[1:]) if len(ho_ten_gv) > 1 else ''
            LopHP = parts[LopHP_idx]

            # Xử lý CauHoi, DanhGia
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and parts[cau_hoi_idx].isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and parts[cau_hoi_idx + 1].isdigit() else None

            # Xử lý góp ý: bỏ qua NULL
            current_pos = cau_hoi_idx + 2
            gopy_values = []
            while len(gopy_values) < 4 and current_pos < len(parts):
                if parts[current_pos].upper() == 'NULL':
                    current_pos += 1
                else:
                    gopy_values.append(parts[current_pos])
                    current_pos += 1
            while len(gopy_values) < 4:
                gopy_values.append(None)

            # Tạo SubmissionID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo hoặc cập nhật
            if SubmissionID not in temp_data:
                temp_data[SubmissionID] = {
                    'info': (Lop, MaSV, HoDem, Ten, parts[ngay_sinh_idx], MaHP, TenHP, 
                            MaGV, HoDemGV, TenGV, LopHP, SEMESTER, 
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
                    'responses': {},
                    'gopy': [None, None, None, None]
                }
            
            # Lưu đánh giá (1-12)
            if CauHoi and 1 <= CauHoi <= 12:
                temp_data[SubmissionID]['responses'][CauHoi] = DanhGia
            
            # Lưu góp ý (13-16) - ưu tiên giá trị đầu tiên
            if CauHoi and 13 <= CauHoi <= 16:
                idx = CauHoi - 13
                if idx < len(gopy_values) and gopy_values[idx] is not None:
                    if temp_data[SubmissionID]['gopy'][idx] is None:
                        temp_data[SubmissionID]['gopy'][idx] = gopy_values[idx]
            else:
                for i in range(4):
                    if gopy_values[i] is not None and temp_data[SubmissionID]['gopy'][i] is None:
                        temp_data[SubmissionID]['gopy'][i] = gopy_values[i]

        except Exception:
            continue

    # Xây dựng kết quả
    for SubmissionID, data in temp_data.items():
        Lop, MaSV, HoDem, Ten, NgaySinh, MaHP, TenHP, MaGV, HoDemGV, TenGV, LopHP, Semester, SubmittedAt = data['info']
        
        record = {
            'SubmissionID': SubmissionID, 'Lop': Lop, 'MaSV': MaSV, 'HoDem': HoDem, 'Ten': Ten,
            'NgaySinh': NgaySinh, 'MaHP': MaHP, 'TenHP': TenHP, 'MaGV': MaGV,
            'HoDemGV': HoDemGV, 'TenGV': TenGV, 'LopHP': LopHP
        }
        
        # Thêm câu hỏi 1-16
        for i in range(1, 17):
            record[f'CauHoi{i}'] = None
        
        for cauhoi, value in data['responses'].items():
            if 1 <= cauhoi <= 12:
                record[f'CauHoi{cauhoi}'] = value
        
        for i in range(4):
            if data['gopy'][i] is not None and str(data['gopy'][i]).upper() != 'NULL':
                record[f'CauHoi{13+i}'] = data['gopy'][i]
        
        final_data[SubmissionID] = record

    # Thống kê
    total_ids = len(temp_data)
    total_responses = sum(len(d['responses']) for d in temp_data.values())
    total_gopy = sum(1 for d in temp_data.values() for g in d['gopy'] if g is not None and str(g).upper() != 'NULL')
    
    print(f"📊 Statistics: {total_ids} IDs | Avg responses: {total_responses/total_ids:.1f}/12 | Avg gopy: {total_gopy/total_ids:.1f}/4")
    
    return pd.DataFrame(list(final_data.values()))

# ==================== MAIN ====================
if __name__ == "__main__":
    print("=" * 70)
    print("SURVEY ETL PIPELINE")
    print("=" * 70)

    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("✅ Connected to Azure")
    except Exception as e:
        print(f"❌ Connection error: {e}")
        sys.exit(1)

    download_from_blob(blob_service)
    survey_df = extract_and_transform_survey(SURVEY_FILE)

    if survey_df.empty:
        print("❌ No data processed!")
        sys.exit(1)

    # Xuất file
    output_file = f"survey_cleaned_{FILE_NAME}.csv"
    survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"📁 Local: {output_file} ({len(survey_df)} rows)")

    # Upload
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    upload_to_blob(blob_service, survey_df, output_path)

    print("=" * 70)
    print("✅ ETL PIPELINE COMPLETED")
    print("=" * 70)
