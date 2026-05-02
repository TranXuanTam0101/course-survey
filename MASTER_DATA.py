#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 1: MASTER DATA
- Load DIM_KHOA, DIM_NGANH, DIM_CHUYEN_NGANH
- Mã Chuyên ngành: lấy từ cột Mã CN trong file
- Mã Ngành, Mã Khoa: tự sinh (không theo logic tên)
- Chạy khi có thay đổi về khoa, ngành, chuyên ngành
"""

import os
import sys
import re
import io
import time
import pandas as pd
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
print("   DIM_KHOA | DIM_NGANH | DIM_CHUYEN_NGANH")
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
    except:
        return ""


def load_table_replace(cursor, table, df, columns, id_col):
    """Load dữ liệu - xóa cũ, insert mới (dùng MERGE hoặc DELETE+INSERT)"""
    if df.empty:
        print(f"  ⚠️ {table}: No data")
        return 0
    
    # Xóa dữ liệu cũ không có trong danh sách mới
    ids_to_keep = df[id_col].tolist()
    # Không xóa hết, chỉ thêm mới và update
    
    placeholders = ', '.join(['?'] * len(columns))
    update_cols = ', '.join([f"{c}=excluded.{c}" for c in columns if c != id_col])
    
    # Dùng MERGE nếu SQL Server hỗ trợ
    query = f"""
        MERGE {table} AS target
        USING (SELECT {placeholders}) AS source ({', '.join(columns)})
        ON target.{id_col} = source.{id_col}
        WHEN MATCHED THEN UPDATE SET {update_cols}
        WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) VALUES ({placeholders});
    """
    
    data = []
    for _, row in df.iterrows():
        tuple_data = []
        for c in columns:
            val = row[c]
            tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
        data.append(tuple(tuple_data))
    
    cursor.fast_executemany = True
    try:
        # Nếu MERGE không hoạt động, fallback DELETE+INSERT
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
                    tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
                batch_data.append(tuple(tuple_data))
            
            placeholders_val = ', '.join(['?'] * len(columns))
            query_insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders_val})"
            cursor.executemany(query_insert, batch_data)
            cursor.connection.commit()
    
    except Exception as e:
        print(f"  ⚠️ MERGE failed: {e}, using DELETE+INSERT")
        # Fallback: xóa hết rồi insert
        cursor.execute(f"DELETE FROM {table}")
        cursor.connection.commit()
        
        for i in range(0, len(df), 1000):
            batch = df.iloc[i:i+1000]
            batch_data = []
            for _, row in batch.iterrows():
                tuple_data = []
                for c in columns:
                    val = row[c]
                    tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
                batch_data.append(tuple(tuple_data))
            
            placeholders_val = ', '.join(['?'] * len(columns))
            query_insert = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders_val})"
            cursor.executemany(query_insert, batch_data)
            cursor.connection.commit()
    
    return len(df)


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
    existing_khoa_ids = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh_ids = {row[0] for row in cursor.fetchall()}
    
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_cn_ids = {row[0] for row in cursor.fetchall()}
    
    # ===== LOAD TenChuyenNganh-Khoa.csv =====
    print("\n📄 Đọc TenChuyenNganh-Khoa.csv...")
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    
    if not content:
        print("❌ Không tìm thấy file!")
        conn.close()
        sys.exit(1)
    
    df = pd.read_csv(io.StringIO(content))
    df.columns = [c.strip() for c in df.columns]
    print(f"  -> {len(df)} dòng, columns: {list(df.columns)}")
    
    # Tìm cột Mã CN
    col_ma_cn = None
    col_ten_cn = None
    col_ten_nganh = None
    col_ten_khoa = None
    
    for col in df.columns:
        col_lower = col.lower().strip()
        if 'mã cn' in col_lower or col_lower == 'mã cn':
            col_ma_cn = col
        elif 'chuyên ngành' in col_lower or col_lower == 'chuyên ngành':
            col_ten_cn = col
        elif ('ngành' in col_lower and 'chuyên' not in col_lower and 'khối' not in col_lower) or col_lower == 'ngành':
            col_ten_nganh = col
        elif ('khoa' in col_lower and 'mã' not in col_lower) or col_lower == 'khoa':
            col_ten_khoa = col
    
    # Nếu không tìm thấy, dùng vị trí cột
    if not col_ma_cn and len(df.columns) > 4:
        col_ten_khoa = df.columns[1]
        col_ten_nganh = df.columns[2]
        col_ten_cn = df.columns[3]
        col_ma_cn = df.columns[4]
        print(f"  -> Dùng vị trí: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
    
    if not col_ma_cn:
        print("❌ Không tìm thấy cột Mã CN!")
        conn.close()
        sys.exit(1)
    
    print(f"  -> Cột: Khoa={col_ten_khoa}, Ngành={col_ten_nganh}, CN={col_ten_cn}, Mã CN={col_ma_cn}")
    
    # ===== TẠO DANH SÁCH KHOA UNIQUE =====
    ten_khoa_list = df[col_ten_khoa].dropna().unique() if col_ten_khoa else []
    
    # Tạo mã khoa tự sinh
    khoa_mapping = {}
    for tk in ten_khoa_list:
        if tk and str(tk).strip():
            ma_khoa = generate_id('KHOA', existing_khoa_ids)
            existing_khoa_ids.add(ma_khoa)
            khoa_mapping[str(tk).strip()] = {'MaKhoa': ma_khoa, 'TenKhoa': str(tk).strip()}
    
    # Thêm các khoa mặc định
    default_khoas = ['Trường Đại học Kinh tế', 'Trường Đại học Sư phạm', 'Trường Đại học Ngoại ngữ', 'Phòng Đào tạo']
    for dk in default_khoas:
        if dk not in khoa_mapping:
            ma_khoa = generate_id('KHOA', existing_khoa_ids)
            existing_khoa_ids.add(ma_khoa)
            khoa_mapping[dk] = {'MaKhoa': ma_khoa, 'TenKhoa': dk}
    
    # ===== TẠO DANH SÁCH NGÀNH UNIQUE =====
    # Mỗi dòng có 1 Ngành - Khoa
    nganh_list = []
    for _, row in df.iterrows():
        ten_khoa = str(row[col_ten_khoa]).strip() if col_ten_khoa and pd.notna(row[col_ten_khoa]) else ''
        ten_nganh = str(row[col_ten_nganh]).strip() if col_ten_nganh and pd.notna(row[col_ten_nganh]) else ''
        
        if ten_khoa and ten_nganh:
            ma_khoa = khoa_mapping.get(ten_khoa, {}).get('MaKhoa', 'KHOA01')
            key = f"{ma_khoa}_{ten_nganh}"
            if key not in [f"{n['MaKhoa']}_{n['TenNganh']}" for n in nganh_list]:
                ma_nganh = generate_ma_nganh(ma_khoa, existing_nganh_ids)
                existing_nganh_ids.add(ma_nganh)
                nganh_list.append({
                    'MaNganh': ma_nganh,
                    'TenNganh': ten_nganh,
                    'MaKhoa': ma_khoa,
                    'TenKhoa': ten_khoa
                })
    
    # ===== TẠO DANH SÁCH CHUYÊN NGÀNH =====
    cn_list = []
    for _, row in df.iterrows():
        ma_cn = str(row[col_ma_cn]).strip() if col_ma_cn and pd.notna(row[col_ma_cn]) else ''
        ten_cn = str(row[col_ten_cn]).strip() if col_ten_cn and pd.notna(row[col_ten_cn]) else ''
        ten_nganh = str(row[col_ten_nganh]).strip() if col_ten_nganh and pd.notna(row[col_ten_nganh]) else ''
        ten_khoa = str(row[col_ten_khoa]).strip() if col_ten_khoa and pd.notna(row[col_ten_khoa]) else ''
        
        if ma_cn and ten_cn:
            ma_khoa = khoa_mapping.get(ten_khoa, {}).get('MaKhoa', 'KHOA01')
            # Tìm MaNganh tương ứng
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
                    'TenNganh': ten_nganh,
                    'MaKhoa': ma_khoa,
                    'TenKhoa': ten_khoa
                })
            
            if ma_cn not in [c['MaChuyenNganh'] for c in cn_list]:
                cn_list.append({
                    'MaChuyenNganh': ma_cn,
                    'TenChuyenNganh': ten_cn,
                    'MaNganh': ma_nganh
                })
    
    print(f"\n📊 Tổng hợp:")
    print(f"  Khoa: {len(khoa_mapping)}")
    print(f"  Ngành: {len(nganh_list)}")
    print(f"  Chuyên ngành: {len(cn_list)}")
    
    # ===== LOAD DIM_KHOA =====
    print("\n💾 LOAD DIM_KHOA...")
    df_khoa = pd.DataFrame(list(khoa_mapping.values()))
    count = load_table_replace(cursor, 'DIM_KHOA', df_khoa, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
    print(f"  ✅ DIM_KHOA: {count} records")
    
    # ===== LOAD DIM_NGANH =====
    print("\n💾 LOAD DIM_NGANH...")
    df_nganh = pd.DataFrame(nganh_list)[['MaNganh', 'TenNganh', 'MaKhoa']]
    count = load_table_replace(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
    print(f"  ✅ DIM_NGANH: {count} records")
    
    # ===== LOAD DIM_CHUYEN_NGANH =====
    print("\n💾 LOAD DIM_CHUYEN_NGANH...")
    df_cn = pd.DataFrame(cn_list)[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']]
    count = load_table_replace(cursor, 'DIM_CHUYEN_NGANH', df_cn,
                               ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
    print(f"  ✅ DIM_CHUYEN_NGANH: {count} records")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! ({time.time()-start:.1f}s)")
    print(f"📊 Khoa: {len(df_khoa)} | Ngành: {len(df_nganh)} | Chuyên ngành: {len(df_cn)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
