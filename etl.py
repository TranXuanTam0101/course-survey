import os
import sys
from azure.storage.blob import BlobServiceClient
import pandas as pd
import numpy as np
import io
from datetime import datetime
import ftfy
import sqlalchemy as sa
import urllib

# ==================== CẤU HÌNH TỐI ƯU ====================
# Tăng tốc độ bằng cách sử dụng công cụ chuyên dụng
# Bật fast_executemany là cách nhanh nhất để insert vào SQL Server
FAST_EXECUTE = True 

def clean_text_optimized(series, max_len=500):
    # Tránh dùng .apply(lambda) nếu có thể, dùng các hàm vector bẩm sinh của pandas
    series = series.astype(str).str.strip().replace(['nan', 'NULL', 'None', ''], np.nan)
    
    # ftfy là hàm xử lý chuỗi phức tạp, vẫn dùng apply nhưng chỉ dùng cho các dòng không null
    mask = series.notna()
    series[mask] = series[mask].apply(ftfy.fix_text)
    
    if max_len:
        series = series.str[:max_len]
    return series

# ==================== DOWNLOAD & READ ====================
# (Giữ nguyên phần kết nối Azure của bạn)
# ... [Phần code lấy dữ liệu từ Azure] ...

print("📊 Reading CSV...")
# Tối ưu: Chỉ đọc các cột cần thiết nếu file quá lớn
df = pd.read_csv(
    io.BytesIO(data),
    sep='\t',
    header=None,
    dtype=str,
    encoding='cp1258',
    on_bad_lines='skip',
    engine='c', # Sử dụng engine C để nhanh hơn
    low_memory=False
)

# Rename nhanh
df.columns = [
    'Lop', 'MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaHP', 'TenHP', 'MaGV', 'HoDemGV', 'TenGV',
    'LopHP', 'CauHoi', 'DanhGia', 'Col13', 'Q13', 'Q14', 'Q15', 'Q16',
    'Col18','Col19','Col20','Col21','Col22','Col23','Col24','Col25','Col26','Col27',
    'Col28','Col29','Col30','Col31'
]

# ==================== XỬ LÝ DỮ LIỆU TỐI ƯU ====================
print("🧹 Cleaning data...")
cols_to_clean = {
    'Lop': 50, 'HoDem': 100, 'Ten': 50, 'MaHP': 50, 
    'TenHP': 200, 'MaGV': 50, 'HoDemGV': 100, 'TenGV': 100, 'LopHP': 100
}
for col, length in cols_to_clean.items():
    df[col] = clean_text_optimized(df[col], length)

# Xử lý MaSV dùng vector thay vì map(safe_convert)
df['MaSV'] = df['MaSV'].str.replace(',', '', regex=False).str.split('.').str[0]

# Chuyển đổi số hàng loạt
df['CauHoi'] = pd.to_numeric(df['CauHoi'], errors='coerce')
df['DanhGia'] = pd.to_numeric(df['DanhGia'], errors='coerce')

# ==================== LOGIC ETL TẬP TRUNG ====================
# Tạo StudentKey cực nhanh bằng cách cộng chuỗi trực tiếp
df['StudentKey'] = (df['Lop'].fillna('') + df['MaSV'].fillna('') + 
                    df['HoDem'].fillna('') + df['Ten'].fillna('') + 
                    df['NgaySinh'].fillna(''))

# Pivot table là điểm gây chậm. Ta lọc trước khi pivot.
print("🔄 Pivoting Q1-Q12...")
df_q1_12 = df[df['CauHoi'].between(1, 12)]
pivot_q = df_q1_12.pivot_table(
    index='StudentKey',
    columns='CauHoi',
    values='DanhGia',
    aggfunc='first'
).reset_index()
pivot_q.columns = ['StudentKey'] + [f'Q{int(i)}' for i in pivot_q.columns[1:]]

# Xử lý Q13-Q16: Thay vì map(groupby), dùng pivot tương tự hoặc merge
df_q13_16 = df[['StudentKey', 'Q13', 'Q14', 'Q15', 'Q16']].drop_duplicates('StudentKey')

# Tổng hợp df_final bằng cách merge các bảng đã rút gọn (nhanh hơn groupby trên bảng lớn)
df_main_info = df.drop_duplicates('StudentKey').drop(columns=['CauHoi', 'DanhGia', 'Q13', 'Q14', 'Q15', 'Q16'])
df_final = df_main_info.merge(pivot_q, on='StudentKey', how='left')
df_final = df_final.merge(df_q13_16, on='StudentKey', how='left')

# Tạo ID duy nhất bằng factorize (nhanh hơn dict map rất nhiều)
df_final['ID'] = pd.Series(pd.factorize(df_final['StudentKey'])[0] + 1).apply(lambda x: f"SV{x:06d}")

# ==================== LOAD VÀO SQL (SUPER FAST) ====================
print("\n🚀 Loading data into Azure SQL with fast_executemany...")

# Quan trọng: Bật fast_executemany=True
engine = sa.create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}", 
    fast_executemany=True, 
    future=True
)

def secure_insert(df_to_load, table_name, conn, chunk=10000):
    if df_to_load.empty: return
    # Loại bỏ các cột không có trong schema nếu cần
    df_to_load.to_sql(table_name, conn, if_exists='append', index=False, chunksize=chunk)

try:
    with engine.connect() as conn:
        # Tắt kiểm tra ràng buộc hoặc dùng transaction để tăng tốc
        with conn.begin():
            print("   - Inserting SINH_VIEN...")
            sv_cols = ['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']
            secure_insert(df_final[sv_cols].drop_duplicates('ID'), 'SINH_VIEN', conn)

            print("   - Inserting GIANG_VIEN...")
            gv_cols = ['MaGV', 'HoDemGV', 'TenGV']
            secure_insert(df_final[gv_cols].dropna(subset=['MaGV']).drop_duplicates('MaGV'), 'GIANG_VIEN', conn)
            
            # ... Làm tương tự cho các bảng khác ...
            
    print("✅ All data loaded!")
except Exception as e:
    print(f"❌ SQL Error: {e}")
