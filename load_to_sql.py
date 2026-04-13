import pandas as pd
import sqlalchemy as sa
import urllib
import os
import sys

def load_to_azure_sql():
    print("⏳ [Step 2] Starting SQL Load Process...")
    
    file_path = "processed_data_temp.csv"
    if not os.path.exists(file_path):
        print(f"❌ Error: {file_path} not found.")
        return

    # Đọc dữ liệu
    df = pd.read_csv(file_path, dtype={'MaSV': str, 'MaGV': str, 'MaHP': str})

    # Cấu hình kết nối
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
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

    try:
        with engine.connect() as conn:
            # Dùng transaction để xóa và nạp mới hoàn toàn
            with conn.begin():
                print("🧹 Cleaning old data from all tables...")
                # Xóa theo thứ tự ngược của khóa ngoại
                conn.execute(sa.text("DELETE FROM PHIEU_KHAO_SAT"))
                conn.execute(sa.text("DELETE FROM LOP_HOC_PHAN"))
                conn.execute(sa.text("DELETE FROM SINH_VIEN"))
                conn.execute(sa.text("DELETE FROM HOC_PHAN"))
                conn.execute(sa.text("DELETE FROM GIANG_VIEN"))

                # 1. NẠP SINH_VIEN
                print("   - Inserting: SINH_VIEN...")
                df_sv = df[['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].drop_duplicates(subset=['ID'])
                # Sửa lỗi MaSV (Xóa dấu phẩy, xử lý số khoa học)
                df_sv['MaSV'] = df_sv['MaSV'].apply(lambda x: str(int(float(str(x).replace(',', '.')))) if 'E+' in str(x) else str(x).split(',')[0])
                df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False)

                # 2. NẠP HOC_PHAN
                print("   - Inserting: HOC_PHAN...")
                df_hp = df[['MaHP', 'TenHP']].drop_duplicates(subset=['MaHP']).dropna(subset=['MaHP'])
                df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False)

                # 3. NẠP GIANG_VIEN (Sửa lỗi truncation)
                print("   - Inserting: GIANG_VIEN...")
                df_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV']).dropna(subset=['MaGV'])
                # Cắt ngắn dữ liệu nếu vượt quá 100/200 ký tự để tránh lỗi truncation
                df_gv['HoDemGV'] = df_gv['HoDemGV'].astype(str).str[:199]
                df_gv['TenGV'] = df_gv['TenGV'].astype(str).str[:99]
                df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False)

                # 4. NẠP LOP_HOC_PHAN
                print("   - Inserting: LOP_HOC_PHAN...")
                df_lhp = df[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
                df_lhp.columns = ['MaLopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']
                df_lhp['TenLopHP'] = df_lhp['MaLopHP']
                df_lhp = df_lhp.drop_duplicates(subset=['MaLopHP']).dropna(subset=['MaLopHP'])
                df_lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False)

                # 5. NẠP PHIEU_KHAO_SAT
                print("   - Inserting: PHIEU_KHAO_SAT...")
                fact_cols = {'ID': 'ID_SV', 'LopHP': 'MaLopHP', 'HocKy': 'HocKy', 'NamHoc': 'NamHoc'}
                for i in range(1, 17): fact_cols[f'Q{i}'] = f'Q{i}'
                df_fact = df[list(fact_cols.keys())].copy().rename(columns=fact_cols)
                df_fact.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False)

            print("✅ Data successfully re-loaded (Truncate & Load)!")

    except Exception as e:
        print(f"❌ SQL ERROR: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    load_to_azure_sql()
