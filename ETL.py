#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2B: LOAD CSV → DATABASE (SIÊU NHANH)
- Đọc CSV → executemany thẳng
- Không vòng lặp, không fallback
- Tắt constraint, 1 transaction
"""
import os, sys, time, pyodbc
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

def main():
    t0 = time.time()
    print("="*60)
    print("📊 PIPELINE 2B: LOAD SIÊU NHANH")
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
        
        # Định nghĩa các bảng: tên, cột, cách parse
        tables = [
            ("DIM_HOC_KY", "MaHocKy,NamHoc,HocKy", lambda r: (r[0], r[1], int(r[2])) if len(r) >= 2 else None),
            ("DIM_LOP_SINH_VIEN", "MaLop,Lop,MaChuyenNganh", lambda r: (r[0][:50], r[1][:50], r[2][:50]) if len(r) >= 3 else None),
            ("DIM_SINH_VIEN", "MaSV,HoDem,Ten,NgaySinh,MaLop", lambda r: (r[0][:50], r[1][:100] if r[1] != 'NULL' else '', r[2][:50] if r[2] != 'NULL' else '', None, r[4][:50]) if len(r) >= 5 else None),
            ("DIM_GIANG_VIEN", "MaGV,HoDemGV,TenGV", lambda r: (r[0][:50], r[1][:100] if r[1] != 'NULL' else '', r[2][:50] if r[2] != 'NULL' else '') if len(r) >= 3 else None),
            ("DIM_LOP_HOC_PHAN", "MaLopHP,LopHP,MaHP,MaGV,MaHocKy", lambda r: (r[0][:100], r[1][:100], r[2][:50], r[3][:50], r[4][:20]) if len(r) >= 5 else None),
            ("FACT_GOP_Y_TU_LUAN", "SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac", 
             lambda r: (r[0][:200], r[1][:50], r[2][:100], r[3][:4000], r[4][:20], int(r[5]), int(r[6]), int(r[7]), int(r[8]), int(r[9])) if len(r) >= 10 else None),
            ("FACT_KET_QUA_DANH_GIA", "SubmissionID,MaCauHoi,Diem", lambda r: (r[0][:200], int(r[1]), int(r[2])) if len(r) >= 3 else None),
        ]
        
        for table, cols_str, parser in tables:
            t1 = time.time()
            path = f"{base}/{table}.csv"
            
            # Download và parse CSV 1 lần
            content = container.get_blob_client(path).download_blob().readall().decode('utf-8-sig')
            lines = content.strip().split('\n')[1:]  # Bỏ header
            
            # Parse tất cả dòng thành list of tuples
            data = []
            for line in lines:
                if not line.strip(): continue
                parts = line.split('|')
                row = parser(parts)
                if row: data.append(row)
            
            # Insert 1 lần (không chia batch nếu dưới 100K dòng)
            if data:
                ph = ','.join(['?']*len(data[0]))
                sql = f"INSERT INTO {table} ({cols_str}) VALUES ({ph})"
                
                for i in range(0, len(data), 100000):
                    cur.executemany(sql, data[i:i+100000])
            
            print(f"  {table}: {len(data):,} rows ({time.time()-t1:.1f}s)")
        
        cur.execute("COMMIT")
        print(f"  ✅ COMMIT ({time.time()-t0:.1f}s)")
        
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
