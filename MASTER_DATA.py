#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 1: MASTER DATA
- Load DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH, DIM_HOC_PHAN
- Mã Chuyên ngành: lấy từ cột Mã CN trong file
- Mã Ngành, Mã Khoa: tự sinh (không theo logic tên)
- Mã HP: lấy từ file HP-Khoa.csv
- Chạy khi có thay đổi về khoa, ngành, chuyên ngành, học phần
"""

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
    
    # Xóa dữ liệu cũ sẽ được update
    ids_to_update = df[id_col].dropna().tolist()
    
    # Thực hiện DELETE + INSERT theo batch
    inserted = 0
    for i in range(0, len(df), 1000):
        batch = df.iloc[i:i+1000]
        
        # Xóa các ID sẽ update
        batch_ids = batch[id_col].tolist()
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
    
    # Lấy danh sách ID hiện có
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing_khoa_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp_ids = {str(row[0]).strip() for row in cursor.fetchall()}
    
    print(f"  -> Existing: Khoa={len(existing_khoa_ids)}, Nganh={len(existing_nganh_ids)}, CN={len(existing_cn_ids)}, HP={len(existing_hp_ids)}")
    
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
        
        # Fallback dùng vị trí
        if not col_ma_cn:
            cols = df_cn.columns.tolist()
            if len(cols) >= 5:
                col_ten_khoa = cols[1]
                col_ten_nganh = cols[2]
                col_ten_cn = cols[3]
                col_ma_cn = cols[4]
        
        print(f"  -> Cột: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
        
        if col_ma_cn:
            # ===== TẠO DANH SÁCH KHOA UNIQUE =====
            khoa_mapping = {}
            
            # Từ file CN
            if col_ten_khoa:
                for tk in df_cn[col_ten_khoa].dropna().unique():
                    tk_str = str(tk).strip()
                    if tk_str and tk_str not in khoa_mapping:
                        ma_khoa = generate_id('KHOA', existing_khoa_ids)
                        existing_khoa_ids.add(ma_khoa)
                        khoa_mapping[tk_str] = {'MaKhoa': ma_khoa, 'TenKhoa': tk_str}
            
            # Thêm các khoa mặc định
            default_khoas = [
                'Trường Đại học Kinh tế',
                'Trường Đại học Sư phạm', 
                'Trường Đại học Ngoại ngữ',
                'Phòng Đào tạo'
            ]
            for dk in default_khoas:
                if dk not in khoa_mapping:
                    ma_khoa = generate_id('KHOA', existing_khoa_ids)
                    existing_khoa_ids.add(ma_khoa)
                    khoa_mapping[dk] = {'MaKhoa': ma_khoa, 'TenKhoa': dk}
            
            # ===== TẠO DANH SÁCH NGÀNH UNIQUE =====
            nganh_list = []
            if col_ten_nganh and col_ten_khoa:
                for _, row in df_cn.iterrows():
                    ten_khoa = str(row[col_ten_khoa]).strip() if pd.notna(row[col_ten_khoa]) else ''
                    ten_nganh = str(row[col_ten_nganh]).strip() if pd.notna(row[col_ten_nganh]) else ''
                    
                    if ten_khoa and ten_nganh:
                        ma_khoa = khoa_mapping.get(ten_khoa, {}).get('MaKhoa', 'KHOA01')
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
                        ma_khoa = khoa_mapping.get(ten_khoa, {}).get('MaKhoa', 'KHOA01')
                        
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
            
            print(f"\n📊 Tổng hợp từ TenChuyenNganh-Khoa.csv:")
            print(f"  Khoa: {len(khoa_mapping)}")
            print(f"  Ngành: {len(nganh_list)}")
            print(f"  Chuyên ngành: {len(cn_list)}")
            
            # ===== LOAD DIM_KHOA =====
            print("\n💾 LOAD DIM_KHOA...")
            df_khoa = pd.DataFrame(list(khoa_mapping.values()))[['MaKhoa', 'TenKhoa']]
            count = load_table_replace(cursor, 'DIM_KHOA', df_khoa, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
            print(f"  ✅ DIM_KHOA: {count} records")
            
            # ===== LOAD DIM_NGANH =====
            print("\n💾 LOAD DIM_NGANH...")
            df_nganh = pd.DataFrame(nganh_list)[['MaNganh', 'TenNganh', 'MaKhoa']]
            count = load_table_replace(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
            print(f"  ✅ DIM_NGANH: {count} records")
            
            # ===== LOAD DIM_CHUYEN_NGANH =====
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
        print(f"  -> {len(df_hp)} dòng, columns: {list(df_hp.columns)}")
        
        # Tìm cột
        col_ma_hp = None
        col_ten_hp = None
        col_khoa_hp = None
        
        for col in df_hp.columns:
            col_lower = col.lower().strip()
            if 'mã học phần' in col_lower or 'mã hp' in col_lower or col_lower == 'mã học phần':
                col_ma_hp = col
            elif 'tên học phần' in col_lower or 'tên hp' in col_lower or col_lower == 'tên học phần':
                col_ten_hp = col
            elif 'khoa' in col_lower and 'mã' not in col_lower:
                col_khoa_hp = col
        
        # Fallback dùng vị trí
        if not col_ma_hp:
            cols = df_hp.columns.tolist()
            # Bỏ cột STT nếu có
            data_cols = [c for c in cols if 'stt' not in c.lower() and 'unnamed' not in c.lower()]
            if len(data_cols) >= 3:
                col_ma_hp = data_cols[0]
                col_khoa_hp = data_cols[1]
                col_ten_hp = data_cols[2]
            elif len(cols) >= 4:
                col_ma_hp = cols[1]
                col_khoa_hp = cols[2]
                col_ten_hp = cols[3]
        
        print(f"  -> Cột: MaHP={col_ma_hp}, Khoa={col_khoa_hp}, TenHP={col_ten_hp}")
        
        if col_ma_hp:
            # Tạo DataFrame chuẩn
            df_hp_data = pd.DataFrame()
            df_hp_data['MaHP'] = df_hp[col_ma_hp].astype(str).str.strip()
            
            if col_ten_hp:
                df_hp_data['TenHP'] = df_hp[col_ten_hp].astype(str).str.strip()
            else:
                df_hp_data['TenHP'] = ''
            
            # Xử lý cột Khoa
            if col_khoa_hp:
                df_hp_data['Khoa_Original'] = df_hp[col_khoa_hp].astype(str).str.strip()
            else:
                df_hp_data['Khoa_Original'] = 'Trường Đại học Kinh tế'
            
            # ===== XỬ LÝ ĐẶC BIỆT =====
            # Nếu Khoa chứa "Ngữ Văn - Truyền thông" hoặc "Toán - Tin" -> Trường Đại học Sư phạm
            df_hp_data['TenKhoa'] = df_hp_data['Khoa_Original'].apply(
                lambda x: 'Trường Đại học Sư phạm' 
                if isinstance(x, str) and ('Ngữ Văn' in x or 'Toán' in x)
                else x
            )
            
            # Log các trường hợp đặc biệt
            special_mask = df_hp_data['TenKhoa'] != df_hp_data['Khoa_Original']
            special_count = special_mask.sum()
            if special_count > 0:
                print(f"  -> Đặc biệt: {special_count} HP đổi Khoa -> Trường ĐHSP")
                print(f"     Các Khoa gốc: {df_hp_data[special_mask]['Khoa_Original'].unique()}")
            
            # Đảm bảo tất cả TenKhoa có trong khoa_mapping
            # Lấy lại danh sách khoa hiện có sau khi load phần 1
            cursor.execute("SELECT MaKhoa, TenKhoa FROM DIM_KHOA")
            khoa_mapping_final = {}
            for row in cursor.fetchall():
                khoa_mapping_final[str(row[1]).strip()] = str(row[0]).strip()
            
            existing_khoa_ids_final = set(khoa_mapping_final.values())
            
            # Tạo MaKhoa cho HP
            def get_ma_khoa(ten_khoa):
                if ten_khoa in khoa_mapping_final:
                    return khoa_mapping_final[ten_khoa]
                # Tạo mới nếu chưa có
                new_id = generate_id('KHOA', existing_khoa_ids_final)
                existing_khoa_ids_final.add(new_id)
                khoa_mapping_final[ten_khoa] = new_id
                
                # Thêm vào DIM_KHOA
                cursor.execute(
                    "INSERT INTO DIM_KHOA (MaKhoa, TenKhoa) VALUES (?, ?)",
                    (new_id, ten_khoa)
                )
                cursor.connection.commit()
                return new_id
            
            df_hp_data['MaKhoa'] = df_hp_data['TenKhoa'].apply(get_ma_khoa)
            
            # Loại bỏ dòng trống
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != '']
            df_hp_data = df_hp_data[df_hp_data['MaHP'] != 'nan']
            df_hp_data = df_hp_data.drop_duplicates('MaHP')
            
            print(f"  -> Sau xử lý: {len(df_hp_data)} HP")
            print(f"  -> Mẫu:")
            print(df_hp_data[['MaHP', 'TenHP', 'TenKhoa', 'MaKhoa']].head(10).to_string())
            
            # ===== LOAD DIM_HOC_PHAN =====
            print("\n💾 LOAD DIM_HOC_PHAN...")
            df_hp_out = df_hp_data[['MaHP', 'TenHP', 'MaKhoa']]
            count = load_table_replace(cursor, 'DIM_HOC_PHAN', df_hp_out,
                                       ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
            print(f"  ✅ DIM_HOC_PHAN: {count} records")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! ({time.time()-start:.1f}s)")
    print("=" * 70)


if __name__ == "__main__":
    main()
