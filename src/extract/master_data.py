"""
Extract: Đọc master data (HP-Khoa.csv, TenChuyenNganh-Khoa.csv)
"""
import pandas as pd
from loguru import logger
from .azure_blob import AzureBlobExtractor
from ..utils.helpers import create_ma_khoa

class MasterDataExtractor:
    """Extract master data từ Azure"""
    
    def __init__(self, extractor: AzureBlobExtractor):
        self.extractor = extractor
    
    def extract_all(self, semester: str) -> dict:
        """
        Đọc tất cả master data
        Returns: {'hp': df_hp, 'cn': df_cn}
        """
        logger.info("Đang đọc master data...")
        
        hp_df = self._extract_hp_khoa(semester)
        cn_df = self._extract_chuyen_nganh(semester)
        
        logger.success(f"Đã đọc master data: {len(hp_df)} học phần, {len(cn_df)} chuyên ngành")
        
        return {'hp': hp_df, 'cn': cn_df}
    
    def _extract_hp_khoa(self, semester: str) -> pd.DataFrame:
        """Đọc file HP-Khoa.csv"""
        blob_path = f"{semester}/HP-Khoa.csv"
        df = self.extractor.extract_csv_file("tailieu", blob_path)
        
        if not df.empty:
            cols = df.columns.tolist()
            if len(cols) >= 4:
                df = df.iloc[:, 1:4]
                df.columns = ['MaHP', 'TenKhoa', 'TenHP']
            df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
        
        return df
    
    def _extract_chuyen_nganh(self, semester: str) -> pd.DataFrame:
        """Đọc file TenChuyenNganh-Khoa.csv"""
        blob_path = f"{semester}/TenChuyenNganh-Khoa.csv"
        df = self.extractor.extract_csv_file("tailieu", blob_path)
        
        if not df.empty:
            cols = df.columns.tolist()
            if len(cols) >= 4:
                df = df.iloc[:, 1:4]
                df.columns = ['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']
            df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
        
        return df
