"""
Load: Load dữ liệu vào bảng FACT
"""
import pandas as pd
from loguru import logger
from .db_connection import DBConnection

class FactLoader:
    """Load dữ liệu vào FACT_TRA_LOI_KHAO_SAT"""
    
    def __init__(self, db: DBConnection):
        self.db = db
    
    def load(self, fact_df: pd.DataFrame):
        """Load fact data"""
        if fact_df.empty:
            logger.warning("Không có dữ liệu fact để load")
            return
        
        logger.info(f"Đang load {len(fact_df)} dòng vào FACT...")
        
        # Validate foreign keys
        valid_fact = self._validate_fk(fact_df)
        
        if valid_fact.empty:
            logger.warning("Không có dòng nào hợp lệ FK")
            return
        
        # Bulk insert
        count = self.db.bulk_insert(
            valid_fact, 
            'FACT_TRA_LOI_KHAO_SAT',
            ['SubmissionID', 'MaCauHoi', 'MaSV', 'MaLopHP', 'TraLoiSo', 'TraLoiText', 'IsCTS']
        )
        
        skipped = len(fact_df) - len(valid_fact)
        logger.success(f"  FACT: {count} dòng đã insert (bỏ {skipped} dòng lỗi FK)")
    
    def _validate_fk(self, fact_df: pd.DataFrame) -> pd.DataFrame:
        """Lọc các dòng có FK hợp lệ"""
        with self.db.get_connection() as conn:
            # Lấy danh sách MaSV hợp lệ
            sv_df = pd.read_sql("SELECT MaSV FROM DIM_SINH_VIEN", conn)
            valid_sv = set(sv_df['MaSV'].tolist())
            
            # Lấy danh sách MaLopHP hợp lệ
            lhp_df = pd.read_sql("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN", conn)
            valid_lhp = set(lhp_df['MaLopHP'].tolist())
        
        # Lọc
        valid_fact = fact_df[
            fact_df['MaSV'].isin(valid_sv) & 
            fact_df['MaLopHP'].isin(valid_lhp)
        ]
        
        return valid_fact
