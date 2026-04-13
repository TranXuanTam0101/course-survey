import pandas as pd
import sqlalchemy as sa
import urllib
import os
import sys

def load_to_azure_sql():
    print("⏳ [Step 2] Starting SQL Load Process...")
    
    # 1. Kiểm tra file dữ liệu đầu vào
    file_path = "processed_data_temp.csv"
    if not os.path.exists(file_path):
        print(f"❌ Error: {file_path} not found. Run etl.py first!")
        return

    # 2. Đọc dữ liệu (Ép kiểu MaSV là string để tránh lỗi E+)
    print("📊 Reading processed CSV...")
    df = pd.read_csv(file_path, dtype={'MaSV': str, 'MaGV': str, 'MaHP': str})

    # 3. Cấu hình kết nối (Điền thông tin trực tiếp)
    sql_server = "course-survey.database.windows.net"
    sql_db     = "course-survey-db"
    sql_user   = "sqladmin"
    sql_pass   = "Due@2026"

    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={sql_server};"
        f"DATABASE={sql_db};"
        f"UID={sql_user};"
        f"PWD={sql_pass};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    try:
        with engine.connect() as conn:
            # --- 4. NẠP BẢNG SINH_VIEN (Sử dụng ID làm PK) ---
            print("   - Upserting: SINH_VIEN...")
            df_sv = df[['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].drop_duplicates(subset=['ID'])
            # Đảm bảo MaSV không bị dạng E+ trước khi insert
            df_sv['MaSV'] = df_sv['MaSV'].apply(lambda x: str(int(float(x.replace(',', '.')))) if 'E+' in str(x) else str(x))
            
            # Xóa SV cũ nếu trùng ID (Optional: hoặc dùng if_exists='append')
            df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False)

            # --- 5. NẠP BẢNG HOC_PHAN ---
            print("   - Upserting: HOC_PHAN...")
            df_hp = df[['MaHP', 'TenHP']].drop_duplicates(subset=['MaHP']).dropna(subset=['MaHP'])
            df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False)

            # --- 6. NẠP BẢNG GIANG_VIEN ---
            print("   - Upserting: GIANG_VIEN...")
            df_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).dropna(subset=['MaGV'])
            df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False)

            # --- 7. NẠP BẢNG LOP_HOC_PHAN ---
            print("   - Upserting: LOP_HOC_PHAN...")
            df_lhp = df[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
            df_lhp.columns = ['MaLopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']
            # Bổ sung TenLopHP nếu cần (trong Schema có nhưng ETL chưa có, ta tạm để trống hoặc lấy MaLopHP)
            df_lhp['TenLopHP'] = df_lhp['MaLopHP']
            df_lhp = df_lhp.drop_duplicates(subset=['MaLopHP']).dropna(subset=['MaLopHP'])
            df_lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False)

            # --- 8. NẠP BẢNG PHIEU_KHAO_SAT (Fact) ---
            print("   - Upserting: PHIEU_KHAO_SAT (Fact Table)...")
            fact_cols = {
                'ID': 'ID_SV', 
                'LopHP': 'MaLopHP', 
                'HocKy': 'HocKy', 
                'NamHoc': 'NamHoc'
            }
            # Thêm Q1 -> Q16 vào map
            for i in range(1, 17):
                fact_cols[f'Q{i}'] = f'Q{i}'
            
            df_fact = df[list(fact_cols.keys())].copy().rename(columns=fact_cols)
            df_fact.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False)

            print("✅ Data successfully loaded to SQL Azure!")

    except Exception as e:
        print(f"❌ SQL ERROR: {str(e)}")
        # In thêm chi tiết lỗi nếu là lỗi trùng khóa
        if "2627" in str(e):
            print("💡 Gợi ý: Lỗi trùng khóa chính. Hãy kiểm tra xem dữ liệu đã tồn tại trong DB chưa.")
        sys.exit(1)

if __name__ == "__main__":
    load_to_azure_sql()
