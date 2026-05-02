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
def create_ma_khoa(ten_khoa):
    if not ten_khoa or not isinstance(ten_khoa, str):
        return "UNKNOWN"
    
    special_map = {
        'trường đhsp': 'TĐHSP',
        'trường đhkt': 'TĐHKT',
        'trường đhnn': 'TĐHNN',
        'phòng đào tạo': 'PĐT',
    }
    
    ten_lower = ten_khoa.lower().strip()
    for key, value in special_map.items():
        if key in ten_lower:
            return value
    
    words = re.split(r'[\s\-]+', ten_khoa)
    return ''.join([w[0].upper() for w in words if w and w[0].isalpha()]) or "UNKNOWN"


def download_blob(blob_service, container, path):
    try:
        client = blob_service.get_container_client(container).get_blob_client(path)
        return client.download_blob().readall().decode('utf-8-sig') if client.exists() else ""
    except:
        return ""


def load_table(cursor, table, df, columns, id_col):
    """Load dữ liệu - truncate & insert"""
    if df.empty:
        print(f"  ⚠️ {table}: No data")
        return 0
    
    # Xóa dữ liệu cũ và insert mới
    cursor.execute(f"DELETE FROM {table}")
    
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    data = []
    for _, row in df.iterrows():
        tuple_data = []
        for c in columns:
            val = row[c]
            tuple_data.append(str(val)[:500] if val and pd.notna(val) else '')
        data.append(tuple(tuple_data))
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    
    return len(data)


# ================= MAIN =================
def main():
    start = time.time()
    
    # Kết nối
    print("\n📥 Kết nối Azure & Database...")
    blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    
    # ===== LOAD TenChuyenNganh-Khoa.csv =====
    print("\n📄 Đọc TenChuyenNganh-Khoa.csv...")
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    
    if not content:
        print("❌ Không tìm thấy file!")
        sys.exit(1)
    
    df = pd.read_csv(io.StringIO(content))
    df.columns = [c.strip() for c in df.columns]
    print(f"  -> {len(df)} dòng, {len(df.columns)} cột")
    print(f"  -> Columns: {list(df.columns)}")
    
    # Tìm các cột cần thiết
    col_map = {}
    for col in df.columns:
        col_lower = col.lower()
        if 'khoa' in col_lower and 'mã' not in col_lower:
            col_map['TenKhoa'] = col
        elif 'ngành' in col_lower and 'chuyên' not in col_lower and 'khối' not in col_lower:
            col_map['TenNganh'] = col
        elif 'chuyên' in col_lower:
            col_map['TenChuyenNganh'] = col
        elif 'mã cn' in col_lower:
            col_map['MaChuyenNganh'] = col
    
    # Kiểm tra đủ cột
    required = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    missing = [r for r in required if r not in col_map]
    if missing:
        print(f"❌ Thiếu cột: {missing}")
        print(f"   Columns hiện có: {list(df.columns)}")
        # Thử dùng vị trí cột
        if len(df.columns) >= 4:
            col_map = {
                'TenKhoa': df.columns[1],
                'TenNganh': df.columns[2],
                'TenChuyenNganh': df.columns[3],
                'MaChuyenNganh': df.columns[4] if len(df.columns) > 4 else df.columns[3]
            }
            print(f"   -> Dùng vị trí: {col_map}")
        else:
            sys.exit(1)
    
    # Tạo DataFrame chuẩn
    df_master = pd.DataFrame()
    df_master['TenKhoa'] = df[col_map['TenKhoa']].astype(str).str.strip()
    df_master['TenNganh'] = df[col_map['TenNganh']].astype(str).str.strip()
    df_master['TenChuyenNganh'] = df[col_map['TenChuyenNganh']].astype(str).str.strip()
    df_master['MaChuyenNganh'] = df[col_map['MaChuyenNganh']].astype(str).str.strip()
    
    # Tạo mã
    df_master['MaKhoa'] = df_master['TenKhoa'].apply(create_ma_khoa)
    df_master['MaNganh'] = df_master['TenNganh'].apply(
        lambda x: ''.join([w[0].upper() for w in re.split(r'[\s\-]+', str(x)) if w and w[0].isalpha()])
    )
    
    # Loại bỏ dòng trống
    df_master = df_master[df_master['MaChuyenNganh'] != '']
    df_master = df_master.drop_duplicates()
    
    print(f"\n📊 Dữ liệu sau xử lý: {len(df_master)} dòng")
    print(df_master[['MaKhoa', 'TenKhoa', 'MaNganh', 'TenNganh', 'MaChuyenNganh', 'TenChuyenNganh']].head(10).to_string())
    
    # ===== LOAD DIM_KHOA =====
    print("\n💾 LOAD DIM_KHOA...")
    df_khoa = df_master[['MaKhoa', 'TenKhoa']].drop_duplicates('MaKhoa')
    
    # Thêm các khoa đặc biệt
    khoa_default = pd.DataFrame([
        {'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường Đại học Kinh tế'},
        {'MaKhoa': 'TĐHSP', 'TenKhoa': 'Trường Đại học Sư phạm'},
        {'MaKhoa': 'TĐHNN', 'TenKhoa': 'Trường Đại học Ngoại ngữ'},
        {'MaKhoa': 'PĐT', 'TenKhoa': 'Phòng Đào tạo'},
    ])
    df_khoa = pd.concat([df_khoa, khoa_default]).drop_duplicates('MaKhoa')
    
    count = load_table(cursor, 'DIM_KHOA', df_khoa, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
    print(f"  ✅ DIM_KHOA: {count} records")
    
    # ===== LOAD DIM_NGANH =====
    print("\n💾 LOAD DIM_NGANH...")
    df_nganh = df_master[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
    count = load_table(cursor, 'DIM_NGANH', df_nganh, ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
    print(f"  ✅ DIM_NGANH: {count} records")
    
    # ===== LOAD DIM_CHUYEN_NGANH =====
    print("\n💾 LOAD DIM_CHUYEN_NGANH...")
    df_cn = df_master[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    count = load_table(cursor, 'DIM_CHUYEN_NGANH', df_cn,
                       ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
    print(f"  ✅ DIM_CHUYEN_NGANH: {count} records")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH! ({time.time()-start:.1f}s)")
    print(f"📊 DIM_KHOA: {len(df_khoa)} | DIM_NGANH: {len(df_nganh)} | DIM_CHUYEN_NGANH: {len(df_cn)}")
    print("=" * 70)


if __name__ == "__main__":
    main()
