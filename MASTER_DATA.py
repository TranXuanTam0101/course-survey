import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
import pyodbc
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=120;"
)

TAILIEU_CONTAINER = "tailieu"

print("=" * 70)
print("📚 PIPELINE 1: MASTER DATA")
print("   DIM_KHOA | DIM_NGANH | DIM_CHUYEN_NGANH | DIM_HOC_PHAN")
print("=" * 70)

# ================= UTILS =================
def generate_id(prefix, existing_ids, start=1):
    """Tự sinh ID: PREFIX + số (VD: KHOA01, KHOA02...)"""
    i = start
    while True:
        new_id = f"{prefix}{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1

def generate_ma_nganh(ma_khoa, existing_ids, start=1):
    """Tự sinh Mã Ngành: KHOA + NG + số"""
    i = start
    while True:
        new_id = f"{ma_khoa}NG{i:02d}"
        if new_id not in existing_ids:
            return new_id
        i += 1

def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except Exception as e:
        print(f"  ⚠️ Lỗi download {path}: {e}")
        return ""


def load_table_replace(cursor, table, df, columns, id_col):
    """Load dữ liệu - xóa cũ, insert mới"""
    if df.empty:
        print(f"  ⚠️ {table}: No data")
        return 0
    
    print(f"  -> Loading {table}: {len(df)} records...")
    
    inserted = 0
    for i in range(0, len(df), 1000):
        batch = df.iloc[i:i+1000]
        
        # Xóa các ID sẽ update
        batch_ids = batch[id_col].dropna().astype(str).tolist()
        if batch_ids:
            placeholders_ids = ','.join(['?'] * len(batch_ids))
            cursor.execute(f"DELETE FROM {table} WHERE {id_col} IN ({placeholders_ids})", batch_ids)
        
        # Insert
        batch_data = []
        for _, row in batch.iterrows():
            tuple_data = []
            for c in columns:
                val = row[c]
                if pd.isna(val):
                    tuple_data.append('')
                else:
                    tuple_data.append(str(val)[:500])
            batch_data.append(tuple(tuple_data))
        
        placeholders_val = ', '.join(['?'] * len(columns))
        query_insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders_val})"
        cursor.executemany(query_insert, batch_data)
        cursor.connection.commit()
        inserted += len(batch_data)
    
    return inserted


# ================= MAIN =================
def main():
    start = time.time()
    
    # Kết nối
    print("\n📥 Kết nối Azure & Database...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # ===== LOAD TOÀN Bộ KHOA HIỆN CÓ =====
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    khoa_db = {}  # TenKhoa -> MaKhoa
    existing_khoa_ids = set()
    for row in cursor.fetchall():
        ten = str(row[1]).strip()
        ma = str(row[0]).strip()
        khoa_db[ten] = ma
        existing_khoa_ids.add(ma)
    
    # ===== LOAD TOÀN Bộ NGÀNH HIỆN CÓ =====
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # ===== LOAD TOÀN Bộ CHUYÊN NGÀNH HIỆN CÓ =====
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # ===== LOAD TOÀN Bộ HỌC PHẦN HIỆN CÓ =====
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    print(f"  -> Existing: Khoa={len(khoa_db)}, Nganh={len(existing_nganh_ids)}, CN={len(existing_cn_ids)}, HP={len(existing_hp_ids)}")
    
    # Hàm helper lấy hoặc tạo MaKhoa
    def get_or_create_ma_khoa(ten_khoa):
        """Lấy MaKhoa từ DB hoặc tạo mới nếu chưa có"""
        ten_khoa_str = str(ten_khoa).strip() if pd.notna(ten_khoa) else ''
        if not ten_khoa_str:
            ten_khoa_str = 'Trường Đại học Kinh tế'
        
        # Kiểm tra trong dict đã load từ DB
        if ten_khoa_str in khoa_db:
            return khoa_db[ten_khoa_str]
        
        # Tạo mới
        new_id = generate_id('KHOA', existing_khoa_ids)
        existing_khoa_ids.add(new_id)
        khoa_db[ten_khoa_str] = new_id
        
        # Insert vào DB ngay
        try:
            cursor.execute(
                "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)",
                (new_id, ten_khoa_str)
            )
            cursor.connection.commit()
        except Exception as e:
            # Nếu lỗi (race condition), lấy lại từ DB
            cursor.execute("SELECT MaKhoa FROM DIM_KHOA WHERE TenKhoa = ?", (ten_khoa_str,))
            row = cursor.fetchone()
            if row:
                new_id = str(row[0]).strip()
                khoa_db[ten_khoa_str] = new_id
                existing_khoa_ids.add(new_id)
        
        return new_id
    
    # ==========================================
    # PHẦN 1: LOAD TenChuyenNganh-Khoa.csv
    # ==========================================
    print("\n" + "=" * 50)
    print("📄 PHẦN 1: TenChuyenNganh-Khoa.csv")
    print("=" * 50)
    
    content_cn = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    
    if not content_cn:
        print("❌ Không tìm thấy file TenChuyenNganh-Khoa.csv!")
    else:
        df_cn = pd.read_csv(io.StringIO(content_cn))
        df_cn.columns = [c.strip() for c in df_cn.columns]
        print(f"  -> {len(df_cn)} dòng, columns: {list(df_cn.columns)}")
        
        # Tìm cột
        col_ma_cn = None
        col_ten_cn = None
        col_ten_nganh = None
        col_ten_khoa = None
        
        for col in df_cn.columns:
            col_lower = col.lower().strip()
            if 'mã cn' in col_lower:
                col_ma_cn = col
            elif 'chuyên ngành' in col_lower:
                col_ten_cn = col
            elif 'ngành' in col_lower and 'chuyên' not in col_lower and 'khối' not in col_lower:
                col_ten_nganh = col
            elif 'khoa' in col_lower and 'mã' not in col_lower:
                col_ten_khoa = col
        
        # Fallback dùng vị trí nếu không tìm thấy
        if not col_ma_cn:
            cols = df_cn.columns.tolist()
            if len(cols) >= 5:
                col_ten_khoa = col_ten_khoa or cols[1]
                col_ten_nganh = col_ten_nganh or cols[2]
                col_ten_cn = col_ten_cn or cols[3]
                col_ma_cn = cols[4]
        
        print(f"  -> Cột: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
        
        if col_ma_cn:
            # ===== TẠO DANH SÁCH KHOA TỪ FILE CN =====
            khoa_from_cn = set()
            if col_ten_khoa:
                for tk in df_cn[col_ten_khoa].dropna():
                    tk_str = str(tk).strip()
                    if tk_str:
                        khoa_from_cn.add(tk_str)
                        get_or_create_ma_khoa(tk_str)  # Đảm bảo có trong DB
            
            # Thêm các khoa mặc định nếu chưa có
            default_khoas = [
                'Trường Đại học Kinh tế',
                'Trường Đại học Sư phạm',
                'Trường Đại học Ngoại ngữ',
                'Phòng Đào tạo'
            ]
            for dk in default_khoas:
                get_or_create_ma_khoa(dk)
            
            print(f"  -> Khoa từ file CN: {len(khoa_from_cn)} + {len(default_khoas)} mặc định")
            
            # ===== TẠO DANH SÁCH NGÀNH =====
            nganh_list = []
            if col_ten_nganh and col_ten_khoa:
                for _, row in df_cn.iterrows():
                    ten_khoa = str(row[col_ten_khoa]).strip() if pd.notna(row[col_ten_khoa]) else ''
                    ten_nganh = str(row[col_ten_nganh]).strip() if pd.notna(row[col_ten_nganh]) else ''
                    
                    if ten_khoa and ten_nganh:
                        ma_khoa = get_or_create_ma_khoa(ten_khoa)
                        key = f"{ma_khoa}_{ten_nganh}"
                        
                        existing_keys = [f"{n['MaKhoa']}_{n['TenNganh']}" for n in nganh_list]
                        if key not in existing_keys:
                            ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                            existing_nganh_ids.add(ma_nganh)
                            nganh_list.append({
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh,
                                'MaKhoa': ma_khoa
                            })
            
            # ===== TẠO DANH SÁCH CHUYÊN NGÀNH =====
            cn_list = []
            if col_ma_cn and col_ten_cn:
                for _, row in df_cn.iterrows():
                    ma_cn = str(row[col_ma_cn]).strip() if pd.notna(row[col_ma_cn]) else ''
                    ten_cn = str(row[col_ten_cn]).strip() if pd.notna(row[col_ten_cn]) else ''
                    ten_nganh = str(row[col_ten_nganh]).strip() if col_ten_nganh and pd.notna(row[col_ten_nganh]) else ''
                    ten_khoa = str(row[col_ten_khoa]).strip() if col_ten_khoa and pd.notna(row[col_ten_khoa]) else ''
                    
                    if ma_cn:
                        ma_khoa = get_or_create_ma_khoa(ten_khoa) if ten_khoa else 'KHOA01'
                        
                        # Tìm MaNganh
                        ma_nganh = ''
                        for n in nganh_list:
                            if n['TenNganh'] == ten_nganh and n['MaKhoa'] == ma_khoa:
                                ma_nganh = n['MaNganh']
                                break
                        
                        if not ma_nganh:
                            ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                            existing_nganh_ids.add(ma_nganh)
                            nganh_list.append({
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh if ten_nganh else 'Ngành mặc định',
                                'MaKhoa': ma_khoa
                            })
                        
                        if ma_cn not in [c['MaChuyenNganh'] for c in cn_list]:
                            cn_list.append({
                                'MaChuyenNganh': ma_cn,
                                'TenChuyenNganh': ten_cn if ten_cn else f'Chuyên ngành {ma_cn}',
                                'MaNganh': ma_nganh
                            })
            
            print(f"  -> Ngành: {len(nganh_list)}, Chuyên ngành: {len(cn_list)}")
            
            # Load DIM_NGANH
            print("\n💾 LOAD DIM_NGANH...")
            df_nganh = pd.DataFrame(nganh_list)[['MaNganh', 'TenNganh', 'MaKhoa']]
            count = load_table_replace(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
            print(f"  ✅ DIM_NGANH: {count} records")
            
            # Load DIM_CHUYEN_NGANH
            print("\n💾 LOAD DIM_CHUYEN_NGANH...")
            df_cn_out = pd.DataFrame(cn_list)[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']]
            count = load_table_replace(cursor, 'DIM_CHUYEN_NGANH', df_cn_out,
                                       ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
            print(f"  ✅ DIM_CHUYEN_NGANH: {count} records")
    
    # ==========================================
    # PHẦN 2: LOAD HP-Khoa.csv -> DIM_HOC_PHAN
    # ==========================================
    print("\n" + "=" * 50)
    print("📄 PHẦN 2: HP-Khoa.csv -> DIM_HOC_PHAN")
    print("=" * 50)
    
    content_hp = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    
    if not content_hp:
        print("❌ Không tìm thấy file HP-Khoa.csv!")
    else:
        df_hp = pd.read_csv(io.StringIO(content_hp))
        df_hp.columns = [c.strip() for c in df_hp.columns]
        print(f"  -> {len(df_hp)} dòng, columns: {list(df_hp.columns)[:5]}...")
        
        # Tìm cột
        col_ma_hp = None
        col_ten_hp = None
        col_khoa_hp = None
        
        for col in df_hp.columns:
            col_lower = col.lower().strip()
            if 'mã học phần' in col_lower or 'mã hp' in col_lower:
                col_ma_hp = col
            elif 'tên học phần' in col_lower or 'tên hp' in col_lower:
                col_ten_hp = col
            elif 'khoa' in col_lower and 'mã' not in col_lower:
                col_khoa_hp = col
        
        # Fallback dùng vị trí
        if not col_ma_hp:
            cols = [c for c in df_hp.columns if 'unnamed' not in c.lower() and 'stt' not in c.lower()]
            if len(cols) >= 3:
                col_ma_hp = cols[0]
                col_khoa_hp = cols[1]
                col_ten_hp = cols[2]
        
        print(f"  -> Cột: MaHP={col_ma_hp}, Khoa={col_khoa_hp}, TenHP={col_ten_hp}")
        
        if col_ma_hp:
            # Tạo DataFrame chuẩn
            df_hp_data = pd.DataFrame()
            df_hp_data['MaHP'] = df_hp[col_ma_hp].astype(str).str.strip()
            df_hp_data['TenHP'] = df_hp[col_ten_hp].astype(str).str.strip() if col_ten_hp else ''
            df_hp_data['Khoa_Original'] = df_hp[col_khoa_hp].astype(str).str.strip() if col_khoa_hp else ''
            
            # ===== XỬ LÝ ĐẶC BIỆT =====
            # Nếu Khoa chứa "Ngữ Văn" hoặc "Toán" -> Trường Đại học Sư phạm
            df_hp_data['TenKhoa'] = df_hp_data['Khoa_Original'].apply(
                lambda x: 'Trường Đại học Sư phạm'
                if isinstance(x, str) and ('Ngữ Văn' in x or 'Toán' in x)
                else x
            )
            
            special_mask = df_hp_data['TenKhoa'] != df_hp_data['Khoa_Original']
            if special_mask.sum() > 0:
                print(f"  -> Đặc biệt: {special_mask.sum()} HP đổi Khoa -> Trường ĐHSP")
            
            # ===== TẠO MaKhoa CHO TỪNG HP =====
            # Dùng get_or_create_ma_khoa đã có từ phần 1
            df_hp_data['MaKhoa'] = df_hp_data['TenKhoa'].apply(
                lambda x: get_or_create_ma_khoa(x) if x else get_or_create_ma_khoa('Trường Đại học Kinh tế')
            )
            
            # Loại bỏ dòng trống
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != '']
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != 'nan']
            df_hp_data = df_hp_data.drop_duplicates('MaHP')
            
            print(f"  -> Sau xử lý: {len(df_hp_data)} HP")
            print(f"  -> Mẫu:")
            sample = df_hp_data[['MaHP', 'TenHP', 'TenKhoa', 'MaKhoa']].head(5)
            for _, r in sample.iterrows():
                print(f"     {r['MaHP']} | {r['TenHP'][:40]} | {r['TenKhoa'][:30]} | {r['MaKhoa']}")
            
            # ===== LOAD DIM_HOC_PHAN =====
            print("\n💾 LOAD DIM_HOC_PHAN...")
            df_hp_out = df_hp_data[['MaHP', 'TenHP', 'MaKhoa']]
            count = load_table_replace(cursor, 'DIM_HOC_PHAN', df_hp_out,
                                       ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
            print(f"  ✅ DIM_HOC_PHAN: {count} records")
    
    # ==========================================
    # PHẦN 3: LOAD DIM_KHOA (TỔNG HỢP CUỐI CÙNG)
    # ==========================================
    print("\n" + "=" * 50)
    print("📄 PHẦN 3: DIM_KHOA (Tổng hợp)")
    print("=" * 50)
    
    # Lấy tất cả khoa từ DB
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    all_khoa = []
    for row in cursor.fetchall():
        all_khoa.append({'MaKhoa': str(row[0]).strip(), 'TenKhoa': str(row[1]).strip()})
    
    df_khoa_final = pd.DataFrame(all_khoa)
    print(f"  -> Tổng Khoa: {len(df_khoa_final)}")
    
    # In danh sách
    for _, r in df_khoa_final.iterrows():
        print(f"     {r['MaKhoa']} | {r['TenKhoa'][:50]}")
    
    count = load_table_replace(cursor, 'DIM_KHOA', df_khoa_final, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
    print(f"  ✅ DIM_KHOA: {count} records")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! ({time.time()-start:.1f}s)")
    print("=" * 70)


if __name__ == "__main__":
    main()
