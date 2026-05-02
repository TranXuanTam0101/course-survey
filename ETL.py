#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2B: LOAD CSV FROM BLOB TO DATABASE
- Đọc CSV từng bảng từ Azure Blob
- Insert vào SQL Server bằng executemany
"""
import os, sys, time, io, pyodbc
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;UID=sqladmin;PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=600;Command Timeout=1800;"
)

CONTAINER_NAME = SEMESTER
PROCESSED_PATH = "processed-data"
BATCH = 50000

def download_csv(container, path):
    """Download CSV từ Azure Blob"""
    content = container.get_blob_client(path).download_blob().readall().decode('utf-8-sig')
    lines = content.strip().split('\n')
    header = lines[0]
    data = [tuple(line.split('|')) for line in lines[1:] if line.strip()]
    return header, data

def insert_batch(cur, table, cols, data, batch=BATCH):
    """Insert với batch"""
    if not data: return 0
    ph = ','.join(['?']*len(cols))
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES ({ph})"
    done = 0
    for i in range(0, len(data), batch):
        chunk = data[i:i+batch]
        try:
            cur.executemany(sql, chunk)
            done += len(chunk)
        except:
            for row in chunk:
                try:
                    cur.execute(sql, row)
                    done += 1
                except: pass
    return done

def main():
    t0 = time.time()
    print("="*60)
    print("📊 PIPELINE 2B: LOAD CSV → DATABASE")
    print("="*60)
    
    blob = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    container = blob.get_container_client(CONTAINER_NAME)
    base = f"{PROCESSED_PATH}/{FILE_NAME}"
    
    conn = pyodbc.connect(CONN_STR, autocommit=False)
    cur = conn.cursor()
    cur.fast_executemany = True
    
    try:
        cur.execute("BEGIN TRANSACTION")
        
        # Tắt constraint
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} NOCHECK CONSTRAINT ALL")
            except: pass
        
        # 1. DIM_HOC_KY
        t1 = time.time()
        _, data = download_csv(container, f"{base}/DIM_HOC_KY.csv")
        if data:
            cur.execute("INSERT INTO DIM_HOC_KY(MaHocKy,NamHoc,HocKy) VALUES(?,?,?)", data[0])
        print(f"  DIM_HOC_KY ({time.time()-t1:.1f}s)")
        
        # 2-7. Các bảng còn lại
        tables = [
            ("DIM_LOP_SINH_VIEN", ['MaLop','Lop','MaChuyenNganh']),
            ("DIM_SINH_VIEN", ['MaSV','HoDem','Ten','NgaySinh','MaLop']),
            ("DIM_GIANG_VIEN", ['MaGV','HoDemGV','TenGV']),
            ("DIM_LOP_HOC_PHAN", ['MaLopHP','LopHP','MaHP','MaGV','MaHocKy']),
            ("FACT_GOP_Y_TU_LUAN", ['SubmissionID','MaSV','MaLopHP','NoiDungGopY','Sentiment','Is_Valid','Tag_HocPhan','Tag_DayHoc','Tag_KiemTra','Tag_Khac']),
            ("FACT_KET_QUA_DANH_GIA", ['SubmissionID','MaCauHoi','Diem']),
        ]
        
        for table, cols in tables:
            t1 = time.time()
            _, data = download_csv(container, f"{base}/{table}.csv")
            if data:
                # Xử lý NULL và escape
                clean_data = []
                for row in data:
                    clean_row = []
                    for i, val in enumerate(row):
                        if val == 'NULL' or val == '':
                            clean_row.append(None if i == 3 and table == 'DIM_SINH_VIEN' else '')
                        else:
                            clean_row.append(val[:4000] if table == 'FACT_GOP_Y_TU_LUAN' and i == 3 else val)
                    clean_data.append(tuple(clean_row))
                
                n = insert_batch(cur, table, cols, clean_data)
                print(f"  {table}: {n:,} rows ({time.time()-t1:.1f}s)")
            else:
                print(f"  {table}: 0 rows")
        
        cur.execute("COMMIT")
        print(f"  ✅ COMMIT")
        
    except Exception as e:
        cur.execute("ROLLBACK")
        print(f"  ❌ {e}")
    finally:
        for t in ['DIM_LOP_SINH_VIEN','DIM_SINH_VIEN','DIM_GIANG_VIEN',
                   'DIM_LOP_HOC_PHAN','FACT_GOP_Y_TU_LUAN','FACT_KET_QUA_DANH_GIA']:
            try: cur.execute(f"ALTER TABLE {t} CHECK CONSTRAINT ALL"); conn.commit()
            except: pass
        conn.close()
    
    print(f"\n🎉 Total: {time.time()-t0:.1f}s")

if __name__ == "__main__":
    main()
