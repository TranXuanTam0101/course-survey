import pandas as pd
import sqlalchemy as sa
import urllib
import os

def load_data():
    print("⏳ Loading processed data...")
    # Giả sử etl.py đã lưu file sạch ra đây
    df = pd.read_csv("processed_data_temp.csv") 

    # Thông tin kết nối
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
        "Encrypt=yes;TrustServerCertificate=no;"
    )
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    try:
        with engine.connect() as conn:
            # XỬ LÝ LỖI TRÙNG (TRƯỚC KHI ĐẨY)
            print("🧹 Cleaning duplicates before insert...")
            
            # 1. Bảng SINH_VIEN (Chỉ lấy mỗi MaSV 1 lần duy nhất)
            # Lỗi "1,91122E+11" phải được xử lý triệt để ở etl.py trước khi tới đây
            df_sv = df[['ID', 'MaSV', 'Lop', 'HoDem', 'Ten', 'NgaySinh']].drop_duplicates(subset=['MaSV'])
            
            # Xóa dữ liệu cũ để tránh lỗi IntegrityError (Nếu bạn muốn nạp đè)
            # conn.execute(sa.text("DELETE FROM PHIEU_KHAO_SAT"))
            # conn.execute(sa.text("DELETE FROM SINH_VIEN"))
            
            print("📤 Inserting to SINH_VIEN...")
            df_sv.to_sql('SINH_VIEN', conn, if_exists='append', index=False)
            
            # ... Các bảng khác làm tương tự ...
            print("✅ Load to SQL Azure completed!")

    except Exception as e:
        print(f"❌ SQL Load Error: {e}")

if __name__ == "__main__":
    load_data()
