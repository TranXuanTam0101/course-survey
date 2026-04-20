"""
Load: Kết nối database
"""
import pymssql
import pandas as pd
from contextlib import contextmanager
from loguru import logger
from ..config.settings import Settings

class DBConnection:
    """Quản lý kết nối database"""
    
    def __init__(self):
        self.config = Settings.DB_CONFIG
    
    @contextmanager
    def get_connection(self):
        """Context manager cho database connection"""
        conn = None
        try:
            conn = pymssql.connect(**self.config)
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None):
        """Execute single query"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            conn.commit()
    
    def bulk_insert(self, df: pd.DataFrame, table_name: str, columns: list, batch_size: int = 5000):
        """
        Bulk insert với batch processing để tối ưu tốc độ
        """
        if df.empty:
            return 0
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            
            total_inserted = 0
            data = [tuple(None if pd.isna(row[c]) else row[c] for c in columns) for _, row in df.iterrows()]
            
            # Insert theo batch
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                try:
                    cursor.executemany(query, batch)
                    conn.commit()
                    total_inserted += len(batch)
                except Exception as e:
                    logger.error(f"Batch insert error: {e}")
                    conn.rollback()
            
            return total_inserted
    
    def get_existing_ids(self, table_name: str, id_column: str) -> set:
        """Lấy danh sách ID đã tồn tại"""
        with self.get_connection() as conn:
            query = f"SELECT {id_column} FROM {table_name}"
            df = pd.read_sql(query, conn)
            return set(df[id_column].tolist())
