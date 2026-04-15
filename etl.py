import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ==================== CẤU HÌNH ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("❌ Thiếu SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ==================== DOWNLOAD FILE ====================
def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
    except Exception as e:
        print(f"❌ Lỗi download: {e}")
        sys.exit(1)

# ==================== UPLOAD FILE ====================
def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        return True
    except Exception as e:
        print(f"❌ Lỗi upload: {e}")
        return False

# ==================== ETL OPTIMIZED ====================
def extract_and_transform_survey(file_path: str):
    # Dictionary lưu dữ liệu
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
            Lop = parts[0] if len(parts) > 0 else ''
            MaSV = parts[1]
            
            # Tìm NgaySinh
            ngay_sinh_idx = None
            for i, x in enumerate(parts):
                if '/' in str(x) and len(str(x).split('/')) == 3:
                    ngay_sinh_idx = i
                    break
            if ngay_sinh_idx is None:
                continue
            
            # Họ tên SV
            HoDem = parts[2] if ngay_sinh_idx > 2 else ''
            Ten = ','.join(parts[3:ngay_sinh_idx]) if ngay_sinh_idx > 3 else ''
            
            # MaHP
            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''
            
            # Tìm MaGV
            MaGV_idx = None
            for i in range(MaHP_idx + 1, len(parts)):
                if str(parts[i]).isdigit() and len(str(parts[i])) >= 6:
                    MaGV_idx = i
                    break
            if MaGV_idx is None:
                continue
            
            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]
            
            # Tìm LopHP
            LopHP_idx = None
            for i in range(MaGV_idx + 1, len(parts)):
                if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i])):
                    LopHP_idx = i
                    break
            if LopHP_idx is None:
                continue
            
            HoDemGV = parts[MaGV_idx + 1] if MaGV_idx + 1 < LopHP_idx else ''
            TenGV = ','.join(parts[MaGV_idx + 2:LopHP_idx]) if LopHP_idx > MaGV_idx + 2 else ''
            LopHP = parts[LopHP_idx]
            
            # Xử lý câu hỏi
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and parts[cau_hoi_idx].isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and parts[cau_hoi_idx + 1].isdigit() else None
            
            # Xử lý góp ý (bỏ qua NULL)
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
            
            # Khởi tạo hoặc cập nhật dữ liệu
            if SubmissionID not in temp_data:
                temp_data[SubmissionID] = {
                    'Lop': Lop, 'MaSV': MaSV, 'HoDem': HoDem, 'Ten': Ten,
                    'NgaySinh': parts[ngay_sinh_idx], 'MaHP': MaHP, 'TenHP': TenHP,
                    'MaGV': MaGV, 'HoDemGV': HoDemGV, 'TenGV': TenGV, 'LopHP': LopHP,
                    'Semester': SEMESTER, 'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'responses': {}, 'gopy_values': [None, None, None, None]
                }
            
            # Lưu câu trả lời
            if CauHoi:
                if 1 <= CauHoi <= 12:
                    temp_data[SubmissionID]['responses'][CauHoi] = DanhGia
                elif 13 <= CauHoi <= 16:
                    idx = CauHoi - 13
                    if idx < len(gopy_values) and gopy_values[idx]:
                        temp_data[SubmissionID]['gopy_values'][idx] = gopy_values[idx]
            else:
                for i in range(4):
                    if gopy_values[i]:
                        temp_data[SubmissionID]['gopy_values'][i] = gopy_values[i]
                        
        except:
            continue
    
    # Xử lý final data
    for SubmissionID, data in temp_data.items():
        record = {
            'SubmissionID': SubmissionID, 'Lop': data['Lop'], 'MaSV': data['MaSV'],
            'HoDem': data['HoDem'], 'Ten': data['Ten'], 'NgaySinh': data['NgaySinh'],
            'MaHP': data['MaHP'], 'TenHP': data['TenHP'], 'MaGV': data['MaGV'],
            'HoDemGV': data['HoDemGV'], 'TenGV': data['TenGV'], 'LopHP': data['LopHP']
        }
        
        # Khởi tạo cột
        for i in range(1, 17):
            record[f'CauHoi{i}'] = None
        
        # Điền CauHoi 1-12
        for cauhoi in range(1, 13):
            if cauhoi in data['responses']:
                record[f'CauHoi{cauhoi}'] = data['responses'][cauhoi]
        
        # Điền CauHoi 13-16
        for i in range(4):
            if data['gopy_values'][i]:
                record[f'CauHoi{13 + i}'] = data['gopy_values'][i]
        
        final_data[SubmissionID] = record
    
    # Tạo DataFrame
    survey_df = pd.DataFrame(list(final_data.values()))
    
    # Sắp xếp cột
    columns = ['SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 
               'MaGV', 'HoDemGV', 'TenGV', 'LopHP'] + [f'CauHoi{i}' for i in range(1, 17)]
    survey_df = survey_df[[c for c in columns if c in survey_df.columns]]
    
    return survey_df

# ==================== MAIN ====================
if __name__ == "__main__":
    # Khởi tạo connection
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Lỗi kết nối: {e}")
        sys.exit(1)

    # Download
    download_from_blob(blob_service)
    
    # Xử lý ETL
    survey_df = extract_and_transform_survey(SURVEY_FILE)
    
    # Xuất file
    output_file = f"survey_cleaned_{FILE_NAME}.csv"
    survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    # Upload
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    upload_to_blob(blob_service, survey_df, output_path)
    
    # Chỉ in 10 dòng kết quả
    print(survey_df.head(10).to_string(index=False))
