import os
import sys
from datetime import datetime
import pandas as pd
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
    except Exception as e:
        sys.exit(1)

def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        return True
    except Exception as e:
        return False

def extract_and_transform_survey(file_path: str):
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
            Lop = parts[0] if len(parts) > 0 else ''
            MaSV = parts[1]

            ngay_sinh_idx = next((i for i, x in enumerate(parts) 
                                if '/' in str(x) and len(str(x).split('/')) == 3), None)
            if ngay_sinh_idx is None:
                continue

            ho_ten_sv = parts[2:ngay_sinh_idx]
            HoDem = ho_ten_sv[0] if ho_ten_sv else ''
            Ten = ','.join(ho_ten_sv[1:]) if len(ho_ten_sv) > 1 else ''

            MaHP_idx = ngay_sinh_idx + 1
            MaHP = parts[MaHP_idx] if MaHP_idx < len(parts) else ''

            MaGV_idx = next((i for i in range(MaHP_idx + 1, len(parts)) 
                           if str(parts[i]).isdigit() and len(str(parts[i])) >= 6), None)
            if MaGV_idx is None:
                continue

            TenHP = ','.join(parts[MaHP_idx + 1:MaGV_idx]).strip()
            MaGV = parts[MaGV_idx]

            LopHP_idx = next((i for i in range(MaGV_idx + 1, len(parts)) 
                            if '_' in str(parts[i]) and any(c.isdigit() for c in str(parts[i]))), None)
            if LopHP_idx is None:
                continue

            ho_ten_gv = parts[MaGV_idx + 1:LopHP_idx]
            HoDemGV = ho_ten_gv[0] if ho_ten_gv else ''
            TenGV = ','.join(ho_ten_gv[1:]) if len(ho_ten_gv) > 1 else ''

            LopHP = parts[LopHP_idx]

            cau_hoi_idx = LopHP_idx + 1
            CauHoi = int(parts[cau_hoi_idx]) if cau_hoi_idx < len(parts) and parts[cau_hoi_idx].isdigit() else None
            DanhGia = int(parts[cau_hoi_idx + 1]) if cau_hoi_idx + 1 < len(parts) and parts[cau_hoi_idx + 1].isdigit() else None

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

            SubmissionID = f"{MaSV}_{LopHP}_{MaGV}_{FILE_NAME}"

            if SubmissionID not in temp_data:
                temp_data[SubmissionID] = {
                    'Lop': Lop, 'MaSV': MaSV, 'HoDem': HoDem, 'Ten': Ten,
                    'NgaySinh': parts[ngay_sinh_idx], 'MaHP': MaHP, 'TenHP': TenHP,
                    'MaGV': MaGV, 'HoDemGV': HoDemGV, 'TenGV': TenGV, 'LopHP': LopHP,
                    'Semester': SEMESTER, 'SubmittedAt': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'responses': {}, 'gopy_values': [None, None, None, None]
                }
            
            if CauHoi and 1 <= CauHoi <= 12:
                temp_data[SubmissionID]['responses'][CauHoi] = DanhGia
            
            if CauHoi and 13 <= CauHoi <= 16:
                idx = CauHoi - 13
                if idx < len(gopy_values) and gopy_values[idx] is not None:
                    if temp_data[SubmissionID]['gopy_values'][idx] is None:
                        temp_data[SubmissionID]['gopy_values'][idx] = gopy_values[idx]
            else:
                for i in range(4):
                    if i < len(gopy_values) and gopy_values[i] is not None:
                        if temp_data[SubmissionID]['gopy_values'][i] is None:
                            temp_data[SubmissionID]['gopy_values'][i] = gopy_values[i]

        except Exception:
            continue

    for SubmissionID, data in temp_data.items():
        record = {
            'SubmissionID': SubmissionID, 'Lop': data['Lop'], 'MaSV': data['MaSV'],
            'HoDem': data['HoDem'], 'Ten': data['Ten'], 'NgaySinh': data['NgaySinh'],
            'MaHP': data['MaHP'], 'TenHP': data['TenHP'], 'MaGV': data['MaGV'],
            'HoDemGV': data['HoDemGV'], 'TenGV': data['TenGV'], 'LopHP': data['LopHP'],
            'Semester': data['Semester'], 'SubmittedAt': data['SubmittedAt'],
        }
        
        for i in range(1, 17):
            record[f'CauHoi{i}'] = None
        
        for cauhoi in range(1, 13):
            if cauhoi in data['responses']:
                record[f'CauHoi{cauhoi}'] = data['responses'][cauhoi]
        
        for i in range(4):
            cauhoi = 13 + i
            if i < len(data['gopy_values']) and data['gopy_values'][i] is not None:
                record[f'CauHoi{cauhoi}'] = data['gopy_values'][i]
        
        final_data[SubmissionID] = record
    
    survey_df = pd.DataFrame(list(final_data.values()))
    
    column_order = [
        'SubmissionID', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV',
        'HoDemGV', 'TenGV', 'LopHP', 'CauHoi1', 'CauHoi2', 'CauHoi3', 'CauHoi4',
        'CauHoi5', 'CauHoi6', 'CauHoi7', 'CauHoi8', 'CauHoi9', 'CauHoi10',
        'CauHoi11', 'CauHoi12', 'CauHoi13', 'CauHoi14', 'CauHoi15', 'CauHoi16'
    ]
    
    existing_columns = [col for col in column_order if col in survey_df.columns]
    survey_df = survey_df[existing_columns]
    
    return survey_df

if __name__ == "__main__":
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    download_from_blob(blob_service)
    survey_df = extract_and_transform_survey(SURVEY_FILE)
    
    if not survey_df.empty:
        output_file = f"survey_cleaned_{FILE_NAME}.csv"
        survey_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        output_path = f"{SEMESTER}/{SURVEY_FILE.replace('.csv', '_processed.csv')}"
        upload_to_blob(blob_service, survey_df, output_path)
        
        print(survey_df.head(10).to_string(index=False))
