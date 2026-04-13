import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
from datetime import datetime
import ftfy

print("🚀 Starting ETL Pipeline (Refined for Unicode & Tab Delimiter)...")

# ==================== CONFIGURATION ====================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not CONNECTION_STRING or not SEMESTER or not SURVEY_FILE:
    print("❌ Missing required environment variables!")
    sys.exit(1)

def clean_text(text, max_len=None):
    """Sửa lỗi encoding và xử lý khoảng trắng"""
    if pd.isna(text) or str(text).lower() in ['nan', 'null', '']:
        return None
    text = ftfy.fix_text(str(text)).strip()
    if max_len and len(text) > max_len:
        text = text[:max_len]
    return text

def safe_join_cols(df, col_range):
    """Nối các cột text lại với nhau (ví dụ Họ và Tên bị tách bởi Tab)"""
    return df[col_range].apply(lambda x: ' '.join(x.dropna().astype(str).str.strip()), axis=1).replace('', None)

try:
    # 1. KẾT NỐI AZURE
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    
    data = blob_client.download_blob().readall()
    
    # 2. ĐỌC DỮ LIỆU (Mô phỏng thao tác Click)
    print("📊 Reading Data...")
    try:
        # Thử đọc bằng UTF-8 (65001) như bạn mong muốn
        df_raw = pd.read_csv(
            io.BytesIO(data),
            sep='\t',
            header=None,
            dtype=str,
            encoding='utf-8-sig',
            engine='python'
        )
    except UnicodeDecodeError:
        print("⚠️ UTF-8 decoding failed, trying 'cp1258' (Vietnamese Windows)...")
        # Nếu UTF-8 lỗi, thử dùng encoding Tiếng Việt của Windows
        df_raw = pd.read_csv(
            io.BytesIO(data),
            sep='\t',
            header=None,
            dtype=str,
            encoding='cp1258',
            engine='python'
        )

    print(f"✅ Successfully loaded {len(df_raw)} rows.")
    # 3. XỬ LÝ TÁCH CỘT THÔNG MINH
    # Vì file dùng Tab làm phân cách, đôi khi Họ Tên bị nhảy cột. 
    # Ta dùng logic tìm cột Ngày Sinh (chứa ký tự '/') để làm mốc neo.
    
    processed_data = []
    
    # Tìm cột chứa ngày sinh (thường nằm ở cột 2 đến cột 6)
    date_col_idx = None
    for i in range(2, 7):
        if df_raw[i].str.contains(r'\d{1,2}/\d{1,2}/', na=False).any():
            date_col_idx = i
            break

    # Duyệt từng dòng để bóc tách dựa trên mốc date_col_idx
    for _, row in df_raw.iterrows():
        # Lớp và Mã SV thường cố định ở cột 0 và 1
        lop = clean_text(row[0])
        masv = clean_text(row[1])
        
        # Họ tên nằm giữa Mã SV (cột 1) và Ngày sinh (date_col_idx)
        ho_dem_ten = " ".join(row[2:date_col_idx].dropna().values).strip()
        parts = ho_dem_ten.split()
        ten = parts[-1] if parts else None
        ho_dem = " ".join(parts[:-1]) if len(parts) > 1 else None
        
        ngay_sinh = clean_text(row[date_col_idx])
        ma_hp = clean_text(row[date_col_idx + 1])
        
        # Tìm cột Mã GV (thường là cột số sau Tên HP)
        # Logic: Duyệt từ sau MaHP, lấy cột nào chỉ có số
        magv = None
        ten_hp_parts = []
        for j in range(date_col_idx + 2, len(df_raw.columns)):
            val = str(row[j]).strip()
            if val.isdigit() and len(val) > 2: # Giả định mã GV là dãy số
                magv = val
                magv_idx = j
                break
            else:
                if val.lower() != 'nan': ten_hp_parts.append(val)
        
        ten_hp = " ".join(ten_hp_parts)
        
        # Các cột còn lại: Họ tên GV, Lớp HP, Điểm Q1-Q12
        # Giả sử sau MaGV là HoDemGV, TenGV, LopHP
        ho_dem_gv = clean_text(row[magv_idx + 1])
        ten_gv = clean_text(row[magv_idx + 2])
        lop_hp = clean_text(row[magv_idx + 3])
        
        # Câu hỏi và Đánh giá (Q1-Q12 thường nằm ở cột tiếp theo)
        cau_hoi = row[magv_idx + 4]
        danh_gia = row[magv_idx + 5]

        processed_data.append({
            'Lop': lop, 'MaSV': masv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': magv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
            'LopHP': lop_hp, 'CauHoi': cau_hoi, 'DanhGia': danh_gia
        })

    df_final = pd.DataFrame(processed_data)

    # 4. PIVOT (Chuyển từ dòng dọc thành cột ngang Q1-Q12)
    # Tạo Key để nhóm các dòng của cùng 1 sinh viên/môn học
    df_final['GroupKey'] = df_final['MaSV'] + "_" + df_final['MaHP'] + "_" + df_final['MaGV']
    
    pivot_df = df_final.pivot_table(
        index=['GroupKey', 'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV', 'LopHP'],
        columns='CauHoi',
        values='DanhGia',
        aggfunc='first'
    ).reset_index()

    # Đổi tên cột Q1, Q2...
    pivot_df.columns = [f'Q{c}' if str(c).isdigit() else c for c in pivot_df.columns]

    # 5. LƯU KẾT QUẢ
    output_filename = SURVEY_FILE.replace(".csv", "_final.csv")
    pivot_df.to_csv("processed_temp.csv", index=False, encoding='utf-8-sig')
    
    # Upload lên Azure
    processed_container = blob_service.get_container_client("processed-data")
    with open("processed_temp.csv", "rb") as data:
        processed_container.upload_blob(name=f"{SEMESTER}/{output_filename}", data=data, overwrite=True)

    print(f"✅ SUCCESS: Exported {len(pivot_df)} survey records.")

except Exception as e:
    print(f"❌ ERROR: {str(e)}")
    sys.exit(1)
