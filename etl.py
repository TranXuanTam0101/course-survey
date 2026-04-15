import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

# ==================== CẤU HÌNH ====================
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
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")

        print(f"📤 Downloading blob: {SEMESTER}/{SURVEY_FILE}")

        data = blob_client.download_blob().readall()

        # Lưu file vào thư mục hiện tại
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
    
    # Dictionary lưu dữ liệu tạm thời theo SubmissionID
    temp_data = {}
    # Dictionary lưu dữ liệu cuối cùng
    final_data = {}

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

            # Xử lý CauHoi và DanhGia (sau LopHP là cột CauHoi ngay lập tức)
            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and str(parts[cau_hoi_idx]).isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and str(parts[cau_hoi_idx + 1]).isdigit() else None

            # XỬ LÝ CauHoi 13-16: NẾU CÓ CỘT NÀO CÓ GIÁ TRỊ = 'NULL' THÌ LẤY SAU CỘT ĐÓ
            # Bắt đầu từ vị trí sau DanhGia
            current_pos = cau_hoi_idx + 2
            gopy_values = []
            
            # Duyệt để tìm 4 giá trị góp ý (bỏ qua các cột NULL)
            while len(gopy_values) < 4 and current_pos < len(parts):
                if parts[current_pos].upper() == 'NULL':
                    # Nếu gặp NULL, bỏ qua và tiếp tục
                    current_pos += 1
                else:
                    # Nếu không phải NULL, lấy giá trị làm góp ý
                    gopy_values.append(parts[current_pos])
                    current_pos += 1
            
            # Đảm bảo luôn có 4 giá trị (thêm None nếu thiếu)
            while len(gopy_values) < 4:
                gopy_values.append(None)

            # TẠO SUBMISSIONID
            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            # Khởi tạo dữ liệu cho SubmissionID
            if SubmissionID not in temp_data:
                temp_data[SubmissionID] = {
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
                    'responses': {},  # Dictionary lưu câu trả lời cho CauHoi 1-12
                    'gopy_values': [None, None, None, None]  # Mảng 4 phần tử cho góp ý CauHoi 13-16
                }
            
            # Lưu câu trả lời cho CauHoi 1-12
            if CauHoi and 1 <= CauHoi <= 12:
                temp_data[SubmissionID]['responses'][CauHoi] = DanhGia
            
            # Lưu góp ý cho CauHoi 13-16 (chỉ lưu nếu có giá trị không phải None)
            if CauHoi and 13 <= CauHoi <= 16:
                idx = CauHoi - 13
                # Lấy giá trị góp ý tương ứng từ danh sách gopy_values
                if idx < len(gopy_values) and gopy_values[idx] is not None:
                    # Chỉ ghi đè nếu giá trị mới không phải None
                    if temp_data[SubmissionID]['gopy_values'][idx] is None:
                        temp_data[SubmissionID]['gopy_values'][idx] = gopy_values[idx]
            else:
                # Nếu dòng này không phải câu hỏi 13-16 nhưng có gopy_values, vẫn lưu
                for i in range(4):
                    if i < len(gopy_values) and gopy_values[i] is not None:
                        if temp_data[SubmissionID]['gopy_values'][i] is None:
                            temp_data[SubmissionID]['gopy_values'][i] = gopy_values[i]

        except Exception as e:
            print(f"⚠️ Bỏ qua dòng {line_num} do lỗi định dạng: {e}")

    # XỬ LÝ TẤT CẢ SUBMISSIONID (kể cả không đủ 12 câu)
    for SubmissionID, data in temp_data.items():
        # Tạo bản ghi cho SubmissionID
        record = {
            'SubmissionID': SubmissionID,
            'Lop': data['Lop'],
            'MaSV': data['MaSV'],
            'HoDem': data['HoDem'],
            'Ten': data['Ten'],
            'NgaySinh': data['NgaySinh'],
            'MaHP': data['MaHP'],
            'TenHP': data['TenHP'],
            'MaGV': data['MaGV'],
            'HoDemGV': data['HoDemGV'],
            'TenGV': data['TenGV'],
            'LopHP': data['LopHP'],
            'Semester': data['Semester'],
            'SubmittedAt': data['SubmittedAt'],
        }
        
        # Khởi tạo các cột CauHoi 1-16
        for i in range(1, 17):
            record[f'CauHoi{i}'] = None
        
        # Điền giá trị cho CauHoi 1-12 từ responses
        for cauhoi in range(1, 13):
            if cauhoi in data['responses']:
                record[f'CauHoi{cauhoi}'] = data['responses'][cauhoi]
        
        # Điền giá trị cho CauHoi 13-16 từ gopy_values (bỏ qua NULL)
        gopy_vals = data['gopy_values'] if data['gopy_values'] else [None, None, None, None]
        for i in range(4):
            cauhoi = 13 + i
            if i < len(gopy_vals) and gopy_vals[i] is not None and str(gopy_vals[i]).upper() != 'NULL':
                record[f'CauHoi{cauhoi}'] = gopy_vals[i]
        
        # Lưu vào final_data
        final_data[SubmissionID] = record
        
        response_count = len([k for k in data['responses'].keys() if 1 <= k <= 12])
        gopy_count = len([g for g in data['gopy_values'] if g is not None and str(g).upper() != 'NULL'])
        print(f"✅ Đã xử lý SubmissionID: {SubmissionID} ({response_count}/12 câu đánh giá, {gopy_count}/4 câu góp ý)")
    
    # Chuyển đổi thành DataFrame
    survey_df = pd.DataFrame(list(final_data.values()))
    
    # Sắp xếp lại các cột theo đúng thứ tự yêu cầu
    column_order = [
        'SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 
        'HoDemGV', 'TenGV', 'LopHP', 'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4', 
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10', 
        'CauHoi11', 'CauHoi12', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    # Chỉ giữ các cột có trong DataFrame
    existing_columns = [col for col in column_order if col in survey_df.columns]
    survey_df = survey_df[existing_columns]

    print(f"✅ Hoàn tất xử lý: {len(survey_df)} hàng dữ liệu survey")
    return survey_df

# ==================== MAIN ====================
if __name__ == "__main__":
    print("=" * 90)
    print("     BẮT ĐẦU SURVEY ETL PIPELINE")
    print("=" * 90)

    # Khởi tạo BlobServiceClient
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("✅ Kết nối Azure Storage thành công")
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure Storage: {e}")
        sys.exit(1)

    # 1. Download file từ Azure Blob
    download_from_blob(blob_service)

    # 2. Xử lý ETL
    survey_df = extract_and_transform_survey(SURVEY_FILE)

    # 3. Kiểm tra dữ liệu trước khi xuất
    if survey_df.empty:
        print("❌ Không có dữ liệu sau khi xử lý!")
        sys.exit(1)

    # 4. Xuất file CSV local
    output_file = f"survey_cleaned_{FILE_NAME}.csv"
    survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"📁 Đã xuất file local: {output_file}")

    # 5. Upload lên Azure Blob
    output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
    upload_success = upload_to_blob(blob_service, survey_df, output_path)
    
    if not upload_success:
        print("❌ Upload thất bại, nhưng vẫn giữ file local để kiểm tra")
    
    # 6. In 10 dòng mẫu để kiểm tra
    print("\n" + "="*150)
    print("📋 10 DÒNG MẪU - SURVEY DATA (PIVOTED)")
    print("="*150)
    print(survey_df.head(10).to_string(index=False))

    print("=" * 90)
    print("🎉 HOÀN TẤT SURVEY ETL PIPELINE")
    print("=" * 90)
