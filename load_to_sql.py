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
                
                # Xử lý giá trị NaN thành None (NULL trong SQL)
                df_gv['HoDemGV'] = df_gv['HoDemGV'].where(pd.notna(df_gv['HoDemGV']), None)
                df_gv['TenGV'] = df_gv['TenGV'].where(pd.notna(df_gv['TenGV']), None)
                
                # Chuyển đổi sang string và cắt ngắn theo độ dài cột trong DB
                # Giả sử: HoDemGV là NVARCHAR(100), TenGV là NVARCHAR(50)
                MAX_HODEM_LEN = 100
                MAX_TEN_LEN = 50
                
                # Chỉ xử lý các giá trị không phải None
                df_gv['HoDemGV'] = df_gv['HoDemGV'].apply(
                    lambda x: str(x)[:MAX_HODEM_LEN] if x is not None else None
                )
                df_gv['TenGV'] = df_gv['TenGV'].apply(
                    lambda x: str(x)[:MAX_TEN_LEN] if x is not None else None
                )
                
                # Thay thế 'nan' string thành None nếu có
                df_gv['HoDemGV'] = df_gv['HoDemGV'].apply(
                    lambda x: None if x == 'nan' else x
                )
                df_gv['TenGV'] = df_gv['TenGV'].apply(
                    lambda x: None if x == 'nan' else x
                )
                
                # In thử 5 dòng đầu để kiểm tra
                print("   Sample data after processing:")
                print(df_gv.head())
                
                # Chèn vào database
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
