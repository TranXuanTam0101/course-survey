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
print("   BƯỚC 1: HP-Khoa.csv -> DIM_KHOA (GỐC) + DIM_HOC_PHAN")
print("   BƯỚC 2: TenChuyenNganh-Khoa.csv -> DIM_NGANH + DIM_CHUYEN_NGANH")
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


def load_table_merge(cursor, table, df, columns, id_col, update_cols=None):
    """
    Load dữ liệu dùng UPDATE + INSERT (không DELETE)
    - UPDATE các dòng đã tồn tại
    - INSERT các dòng mới
    """
    if df.empty:
        print(f"  ⚠️ {table}: No data")
        return 0
    
    if update_cols is None:
        update_cols = [c for c in columns if c != id_col]
    
    # Lấy danh sách ID hiện có
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # Tách thành UPDATE và INSERT
    df['_id_str'] = df[id_col].astype(str).str.strip()
    df_update = df[df['_id_str'].isin(existing_ids)]
    df_insert = df[~df['_id_str'].isin(existing_ids)]
    
    updated = 0
    inserted = 0
    
    # UPDATE
    if not df_update.empty:
        set_clause = ', '.join([f"{c} = ?" for c in update_cols])
        query = f"UPDATE {table} SET {set_clause} WHERE {id_col} = ?"
        
        for _, row in df_update.iterrows():
            data = []
            for c in update_cols:
                val = row[c]
                data.append(str(val)[:500] if val and pd.notna(val) else '')
            data.append(str(row[id_col]).strip())
            cursor.execute(query, data)
        
        cursor.connection.commit()
        updated = len(df_update)
    
    # INSERT
    if not df_insert.empty:
        placeholders = ', '.join(['?'] * len(columns))
        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        data = []
        for _, row in df_insert.iterrows():
            tuple_data = []
            for c in columns:
                val = row[c]
                tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
            data.append(tuple(tuple_data))
        
        cursor.fast_executemany = True
        cursor.executemany(query, data)
        cursor.connection.commit()
        inserted = len(df_insert)
    
    if updated > 0 or inserted > 0:
        print(f"    -> Updated: {updated}, Inserted: {inserted}")
    
    return updated + inserted


# ================= MAIN =================
def main():
    start = time.time()
    
    # Kết nối
    print("\n📥 Kết nối Azure & Database...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # ===== LOAD TOÀN BỘ KHOA HIỆN CÓ TỪ DB =====
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
    khoa_db = {}  # TenKhoa -> MaKhoa
    existing_khoa_ids = set()
    for row in cursor.fetchall():
        ten = str(row[1]).strip()
        ma = str(row[0]).strip()
        khoa_db[ten] = ma
        existing_khoa_ids.add(ma)
    
    print(f"  -> Khoa hiện có: {len(khoa_db)}")
    
    # ===== LOAD TOÀN BỘ NGÀNH HIỆN CÓ =====
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # ===== LOAD TOÀN BỘ CHUYÊN NGÀNH HIỆN CÓ =====
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    # ===== LOAD TOÀN BỘ HỌC PHẦN HIỆN CÓ =====
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    print(f"  -> Existing: Nganh={len(existing_nganh_ids)}, CN={len(existing_cn_ids)}, HP={len(existing_hp_ids)}")
    
    # Hàm helper lấy hoặc tạo MaKhoa
    def get_or_create_ma_khoa(ten_khoa):
        """Lấy MaKhoa từ dict hoặc tạo mới nếu chưa có"""
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
        print(f"    -> Tạo Khoa mới: {new_id} - {ten_khoa_str}")
        
        # Insert vào DB ngay
        try:
            cursor.execute(
                "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)",
                (new_id, ten_khoa_str)
            )
            cursor.connection.commit()
        except Exception as e:
            # Nếu lỗi, lấy lại từ DB
            cursor.execute("SELECT MaKhoa FROM DIM_KHOA WHERE TenKhoa = ?", (ten_khoa_str,))
            row = cursor.fetchone()
            if row:
                new_id = str(row[0]).strip()
                khoa_db[ten_khoa_str] = new_id
                existing_khoa_ids.add(new_id)
                print(f"    -> Khoa đã tồn tại: {new_id}")
        
        return new_id
    
    # ==========================================
    # BƯỚC 1: HP-Khoa.csv (GỐC)
    #   -> DIM_KHOA (gốc)
    #   -> DIM_HOC_PHAN
    # ==========================================
    print("\n" + "=" * 60)
    print("📄 BƯỚC 1: HP-Khoa.csv (Khoa GỐC)")
    print("   -> DIM_KHOA + DIM_HOC_PHAN")
    print("=" * 60)
    
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
            # Tạo DataFrame
            df_hp_data = pd.DataFrame()
            df_hp_data['MaHP'] = df_hp[col_ma_hp].astype(str).str.strip()
            df_hp_data['TenHP'] = df_hp[col_ten_hp].astype(str).str.strip() if col_ten_hp else ''
            df_hp_data['Khoa_Original'] = df_hp[col_khoa_hp].astype(str).str.strip() if col_khoa_hp else ''
            
            # ===== XỬ LÝ ĐẶC BIỆT: Ngữ Văn, Toán -> Trường ĐHSP =====
            df_hp_data['TenKhoa'] = df_hp_data['Khoa_Original'].apply(
                lambda x: 'Trường Đại học Sư phạm'
                if isinstance(x, str) and ('Ngữ Văn' in x or 'Toán' in x)
                else x
            )
            
            special_mask = df_hp_data['TenKhoa'] != df_hp_data['Khoa_Original']
            if special_mask.sum() > 0:
                print(f"  -> Đặc biệt: {special_mask.sum()} HP đổi Khoa -> Trường ĐHSP")
                special_khoas = df_hp_data[special_mask]['Khoa_Original'].unique()
                for sk in special_khoas:
                    print(f"     {sk} -> Trường Đại học Sư phạm")
            
            # ===== 1.1: TẠO DIM_KHOA TỪ HP-Khoa.csv (GỐC) =====
            print("\n  📌 1.1: Tạo DIM_KHOA từ HP-Khoa.csv (GỐC)...")
            
            # Lấy danh sách khoa unique từ HP
            khoa_from_hp = df_hp_data['TenKhoa'].dropna().unique()
            print(f"  -> {len(khoa_from_hp)} Khoa từ HP-Khoa.csv")
            
            for tk in khoa_from_hp:
                tk_str = str(tk).strip()
                if tk_str:
                    get_or_create_ma_khoa(tk_str)
            
            # ===== 1.2: LOAD DIM_HOC_PHAN =====
            print("\n  📌 1.2: Load DIM_HOC_PHAN...")
            
            df_hp_data['MaKhoa'] = df_hp_data['TenKhoa'].apply(get_or_create_ma_khoa)
            
            # Clean
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != '']
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != 'nan']
            df_hp_data = df_hp_data.drop_duplicates('MaHP')
            
            print(f"  -> {len(df_hp_data)} HP sau xử lý")
            
            # In mẫu
            print("  -> Mẫu:")
            for _, r in df_hp_data.head(5).iterrows():
                print(f"     {r['MaHP']} | {r['TenHP'][:40]} | {r['MaKhoa']} | {r['TenKhoa'][:30]}")
            
            df_hp_out = df_hp_data[['MaHP', 'TenHP', 'MaKhoa']]
            count = load_table_merge(cursor, 'DIM_HOC_PHAN', df_hp_out,
                                     ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP',
                                     ['TenHP', 'MaKhoa'])
            print(f"  ✅ DIM_HOC_PHAN: {count} processed")
            
            # In danh sách Khoa sau bước 1
            print(f"\n  📊 Khoa sau BƯỚC 1: {len(khoa_db)}")
            for ten, ma in sorted(khoa_db.items()):
                print(f"     {ma} | {ten[:50]}")
    
    # ==========================================
    # BƯỚC 2: TenChuyenNganh-Khoa.csv (BỔ SUNG)
    #   -> DIM_KHOA (bổ sung nếu có khoa mới)
    #   -> DIM_NGANH
    #   -> DIM_CHUYEN_NGANH
    # ==========================================
    print("\n" + "=" * 60)
    print("📄 BƯỚC 2: TenChuyenNganh-Khoa.csv (BỔ SUNG)")
    print("   -> DIM_KHOA (bổ sung) + DIM_NGANH + DIM_CHUYEN_NGANH")
    print("=" * 60)
    
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
        
        # Fallback dùng vị trí
        if not col_ma_cn:
            cols = df_cn.columns.tolist()
            if len(cols) >= 5:
                col_ten_khoa = col_ten_khoa or cols[1]
                col_ten_nganh = col_ten_nganh or cols[2]
                col_ten_cn = col_ten_cn or cols[3]
                col_ma_cn = cols[4]
        
        print(f"  -> Cột: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
        
        if col_ma_cn:
            # ===== 2.1: BỔ SUNG KHOA TỪ FILE CN (NẾU CÓ MỚI) =====
            print("\n  📌 2.1: Bổ sung Khoa từ TenChuyenNganh-Khoa.csv...")
            
            khoa_from_cn = set()
            if col_ten_khoa:
                for tk in df_cn[col_ten_khoa].dropna():
                    tk_str = str(tk).strip()
                    if tk_str:
                        khoa_from_cn.add(tk_str)
            
            # Thêm khoa mặc định
            default_khoas = [
                'Trường Đại học Kinh tế',
                'Trường Đại học Sư phạm',
                'Trường Đại học Ngoại ngữ',
                'Phòng Đào tạo'
            ]
            for dk in default_khoas:
                khoa_from_cn.add(dk)
            
            khoa_moi = 0
            for tk in khoa_from_cn:
                if tk not in khoa_db:
                    get_or_create_ma_khoa(tk)
                    khoa_moi += 1
            
            if khoa_moi > 0:
                print(f"  -> Bổ sung {khoa_moi} Khoa mới từ file CN")
            else:
                print(f"  -> Không có Khoa mới (tất cả đã có từ HP-Khoa.csv)")
            
            # ===== 2.2: TẠO NGÀNH =====
            print("\n  📌 2.2: Tạo DIM_NGANH...")
            
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
            
            print(f"  -> {len(nganh_list)} Ngành")
            
            # Load DIM_NGANH
            df_nganh = pd.DataFrame(nganh_list)[['MaNganh', 'TenNganh', 'MaKhoa']]
            count = load_table_merge(cursor, 'DIM_NGANH', df_nganh,
                                     ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh',
                                     ['TenNganh', 'MaKhoa'])
            print(f"  ✅ DIM_NGANH: {count} processed")
            
            # ===== 2.3: TẠO CHUYÊN NGÀNH =====
            print("\n  📌 2.3: Tạo DIM_CHUYEN_NGANH...")
            
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
                            # Load ngành mới vào DB
                            df_nganh_new = pd.DataFrame([{
                                'MaNganh': ma_nganh,
                                'TenNganh': ten_nganh if ten_nganh else 'Ngành mặc định',
                                'MaKhoa': ma_khoa
                            }])
                            load_table_merge(cursor, 'DIM_NGANH', df_nganh_new,
                                           ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh',
                                           ['TenNganh', 'MaKhoa'])
                        
                        if ma_cn not in [c['MaChuyenNganh'] for c in cn_list]:
                            cn_list.append({
                                'MaChuyenNganh': ma_cn,
                                'TenChuyenNganh': ten_cn if ten_cn else f'CN {ma_cn}',
                                'MaNganh': ma_nganh
                            })
            
            print(f"  -> {len(cn_list)} Chuyên ngành")
            
            # Load DIM_CHUYEN_NGANH
            df_cn_out = pd.DataFrame(cn_list)[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']]
            count = load_table_merge(cursor, 'DIM_CHUYEN_NGANH', df_cn_out,
                                     ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh',
                                     ['TenChuyenNganh', 'MaNganh'])
            print(f"  ✅ DIM_CHUYEN_NGANH: {count} processed")
    
    # ==========================================
    # TỔNG KẾT
    # ==========================================
    print("\n" + "=" * 70)
    print("📊 TỔNG KẾT")
    print("=" * 70)
    
    cursor.execute("SELECT COUNT(*) FROM DIM_KHOA")
    count_khoa = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM DIM_NGANH")
    count_nganh = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM DIM_CHUYEN_NGANH")
    count_cn = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM DIM_HOC_PHAN")
    count_hp = cursor.fetchone()[0]
    
    print(f"  DIM_KHOA: {count_khoa}")
    print(f"  DIM_NGANH: {count_nganh}")
    print(f"  DIM_CHUYEN_NGANH: {count_cn}")
    print(f"  DIM_HOC_PHAN: {count_hp}")
    
    # In danh sách Khoa cuối cùng
    print(f"\n  📋 Danh sách Khoa ({count_khoa}):")
    cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA ORDER BY MaKhoa")
    for row in cursor.fetchall():
        print(f"     {row[0]} | {row[1][:60]}")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! ({time.time()-start:.1f}s)")
    print("=" * 70)


if __name__ == "__main__":
    main()
