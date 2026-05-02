#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PIPELINE 2B: LOAD SIÊU NHANH - FIXED
- FACT_KQ: MERGE thay INSERT (bỏ qua duplicate)
- FACT_GY: batch 10K (vì NVARCHAR(MAX))
"""
import os, sys, time, pyodbc
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")
FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

SERVER = "course-survey.database.windows.net"
DB = "course-survey-db"
UID = "sqladmin"

CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DB};"
    f"UID={UID};PWD={DB_PASSWORD};Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=600;Command Timeout=1800;"
)

CONTAINER_NAME = SEMESTER
PROCESSED_PATH = "processed-data"

def main():
    t0 = time.time()
    print("="*60)
    print("📊 PIPELINE 2B: LOAD FIXED")
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
        
        for table, cols_str, parser, batch_size in [
            ("DIM_HOC_KY", "MaHocKy,NamHoc,HocKy", 
             lambda r: (r[0], r[1], int(r[2])) if len(r)>=2 else None, 1),
            ("DIM_LOP_SINH_VIEN", "MaLop,Lop,MaChuyenNganh", 
             lambda r: (r[0][:50], r[1][:50], r[2][:50]) if len(r)>=3 else None, 50000),
            ("DIM_SINH_VIEN", "MaSV,HoDem,Ten,NgaySinh,MaLop", 
             lambda r: (r[0][:50], '', '', None, r[4][:50]) if len(r)>=5 else None, 50000),
            ("DIM_GIANG_VIEN", "MaGV,HoDemGV,TenGV", 
             lambda r: (r[0][:50], '', '') if len(r)>=3 else None, 50000),
            ("DIM_LOP_HOC_PHAN", "MaLopHP,LopHP,MaHP,MaGV,MaHocKy", 
             lambda r: (r[0][:100], r[1][:100], r[2][:50], r[3][:50], r[4][:20]) if len(r)>=5 else None, 50000),
            ("FACT_GOP_Y_TU_LUAN", "SubmissionID,MaSV,MaLopHP,NoiDungGopY,Sentiment,Is_Valid,Tag_HocPhan,Tag_DayHoc,Tag_KiemTra,Tag_Khac", 
             lambda r: (r[0][:200], r[1][:50], r[2][:100], r[3][:4000], r[4][:20], int(r[5]), int(r[6]), int(r[7]), int(r[8]), int(r[9])) if len(r)>=10 else None, 10000),
        ]:
            t1 = time.time()
            content = container.get_blob_client(f"{base}/{table}.csv").download_blob().readall().decode('utf-8-sig')
            lines = [l for l in content.strip().split('\n')[1:] if l.strip()]
            
            data = []
            for line in lines:
                parts = line.split('|')
                row = parser(parts)
                if row: data.append(row)
            
            if data:
                ph = ','.join(['?']*len(data[0]))
                sql = f"INSERT INTO {table} ({cols_str}) VALUES ({ph})"
                for i in range(0, len(data), batch_size):
                    cur.executemany(sql, data[i:i+batch_size])
            
            print(f"  {table}: {len(data):,} rows ({time.time()-t1:.1f}s)")
        
        # FACT_KET_QUA - Dùng MERGE để bỏ qua duplicate
        t1 = time.time()
        content = container.get_blob_client(f"{base}/FACT_KET_QUA_DANH_GIA.csv").download_blob().readall().decode('utf-8-sig')
        lines = [l for l in content.strip().split('\n')[1:] if l.strip()]
        
        # Gom thành batch 1000 dòng cho MERGE
        values_list = []
        for line in lines:
            parts = line.split('|')
            if len(parts) >= 3:
                sid = parts[0][:200].replace("'", "''")
                mc = int(parts[1])
                d = int(parts[2])
                values_list.append(f"('{sid}',{mc},{d})")
        
        # Chia thành batch 1000
        for i in range(0, len(values_list), 1000):
            batch = values_list[i:i+1000]
            vals = ",\n".join(batch)
            sql = f"""
                MERGE FACT_KET_QUA_DANH_GIA AS t
                USING (VALUES {vals}) AS s(SubmissionID, MaCauHoi, Diem)
                ON t.SubmissionID = s.SubmissionID AND t.MaCauHoi = s.MaCauHoi
                WHEN NOT MATCHED THEN INSERT (SubmissionID, MaCauHoi, Diem) VALUES (s.SubmissionID, s.MaCauHoi, s.Diem);
            """
            cur.execute(sql)
        
        print(f"  FACT_KET_QUA_DANH_GIA: {len(values_list):,} rows ({time.time()-t1:.1f}s)")
        
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
