import pandas as pd
import sqlalchemy as sa
import urllib
import os
import sys
import glob

def load_to_azure_sql():
    print("⏳ [Step 2] Starting SQL Load Process...")

    # ==================== TÌM FILE PROCESSED ====================
    # Tự động tìm file có đuôi _processed.csv
    processed_files = glob.glob("*_processed.csv")
    
    if not processed_files:
        print("❌ Error: Không tìm thấy file *_processed.csv")
        print("   Vui lòng chạy etl.py trước!")
        return
    
    # Lấy file mới nhất (theo thời gian sửa)
    file_path = max(processed_files, key=os.path.getctime)
    print(f"📄 Found processed file: {file_path}")

    # Đọc dữ liệu
    try:
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        print(f"✅ Loaded {len(df):,} rows, {len(df.columns)} columns")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return

    # ==================== CẤU HÌNH KẾT NỐI ====================
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
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    
    engine = sa.create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}", 
        fast_executemany=True,
        pool_pre_ping=True
    )

    try:
        with engine.connect() as conn:
            with conn.begin():
                
                # 1. SINH_VIEN
                print("   - Inserting into SINH_VIEN...")
                sv_cols = ['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']
                df_sv = df[[col for col in sv_cols if col in df.columns]].drop_duplicates(subset=['ID'])
                df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False, method='multi')

                # 2. HOC_PHAN
                print("   - Inserting into HOC_PHAN...")
                hp_cols = ['MaHP', 'TenHP']
                df_hp = df[[col for col in hp_cols if col in df.columns]].drop_duplicates(subset=['MaHP']).dropna(subset=['MaHP'])
                df_hp.to_sql('HOC_PHAN', conn, if_exists='append', index=False, method='multi')

                # 3. GIANG_VIEN
                print("   - Inserting into GIANG_VIEN...")
                gv_cols = ['MaGV', 'HoDemGV', 'TenGV']
                df_gv = df[[col for col in gv_cols if col in df.columns]].drop_duplicates(subset=['MaGV']).dropna(subset=['MaGV'])
                df_gv.to_sql('GIANG_VIEN', conn, if_exists='append', index=False, method='multi')

                # 4. LOP_HOC_PHAN
                print("   - Inserting into LOP_HOC_PHAN...")
                lhp = df[['LopHP', 'MaHP', 'MaGV', 'HocKy', 'NamHoc']].copy()
                lhp = lhp.rename(columns={'LopHP': 'MaLopHP'})
                lhp['TenLopHP'] = lhp['MaLopHP']
                lhp = lhp.drop_duplicates(subset=['MaLopHP']).dropna(subset=['MaLopHP'])
                lhp.to_sql('LOP_HOC_PHAN', conn, if_exists='append', index=False, method='multi')

                # 5. PHIEU_KHAO_SAT
                print("   - Inserting into PHIEU_KHAO_SAT...")
                fact_cols = ['ID', 'LopHP', 'HocKy', 'NamHoc']
                for i in range(1, 17):
                    q_col = f'Q{i}'
                    if q_col in df.columns:
                        fact_cols.append(q_col)
                
                df_fact = df[fact_cols].copy()
                df_fact = df_fact.rename(columns={
                    'ID': 'ID_SV',
                    'LopHP': 'MaLopHP'
                })
                df_fact.to_sql('PHIEU_KHAO_SAT', conn, if_exists='append', index=False, method='multi')

            print("\n✅ All tables loaded successfully into Azure SQL!")

    except Exception as e:
        print(f"\n❌ SQL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    load_to_azure_sql()
