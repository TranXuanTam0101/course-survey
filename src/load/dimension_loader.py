"""
Load: Load các bảng dimension
"""
import pandas as pd
from loguru import logger
from .db_connection import DBConnection

class DimensionLoader:
    """Load dữ liệu vào các bảng DIM"""
    
    def __init__(self, db: DBConnection):
        self.db = db
    
    def load_all(self, dimensions: dict, ma_hoc_ky: str):
        """Load tất cả dimensions"""
        logger.info("Đang load dimensions...")
        
        # 1. DIM_HOC_KY
        self._load_hoc_ky(dimensions['hoc_ky'])
        
        # 2. DIM_KHOA
        self._load_khoa(dimensions['khoa'])
        
        # 3. DIM_CHUONG_TRINH_DAO_TAO
        self._ensure_ctdt()
        
        # 4. DIM_CHUYEN_NGANH
        self._load_chuyen_nganh(dimensions['chuyen_nganh'])
        
        # 5. DIM_LOP_SINH_VIEN
        self._load_lop_sv(dimensions['lop_sv'])
        
        # 6. DIM_GIANG_VIEN
        self._load_giang_vien(dimensions['giang_vien'])
        
        # 7. DIM_HOC_PHAN
        self._load_hoc_phan(dimensions['hoc_phan'])
        
        # 8. DIM_SINH_VIEN (PHẢI LOAD TRƯỚC FACT)
        self._load_sinh_vien(dimensions['sinh_vien'])
        
        # 9. DIM_LOP_HOC_PHAN
        self._load_lop_hp(dimensions['lop_hp'])
        
        logger.success("Đã load tất cả dimensions")
    
    def _load_hoc_ky(self, df: pd.DataFrame):
        hk = df.iloc[0]
        query = """
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = %s)
            INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (%s, %s, %s)
        """
        self.db.execute_query(query, (hk['MaHocKy'], hk['MaHocKy'], hk['NamHoc'], hk['HocKy']))
        logger.debug(f"  DIM_HOC_KY: {hk['MaHocKy']}")
    
    def _load_khoa(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_KHOA', 'MaKhoa')
        new_data = df[~df['MaKhoa'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'])
            logger.debug(f"  DIM_KHOA: {count} dòng mới")
    
    def _ensure_ctdt(self):
        query = """
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """
        self.db.execute_query(query)
        logger.debug("  DIM_CTDT: Đã đảm bảo CTDT_CHINHQUY")
    
    def _load_chuyen_nganh(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_CHUYEN_NGANH', 'MaChuyenNganh')
        new_data = df[~df['MaChuyenNganh'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_CHUYEN_NGANH', 
                                       ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'])
            logger.debug(f"  DIM_CHUYEN_NGANH: {count} dòng mới")
    
    def _load_lop_sv(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_LOP_SINH_VIEN', 'MaLop')
        new_data = df[~df['MaLop'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_LOP_SINH_VIEN', 
                                       ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'])
            logger.debug(f"  DIM_LOP_SINH_VIEN: {count} dòng mới")
    
    def _load_giang_vien(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_GIANG_VIEN', 'MaGV')
        new_data = df[~df['MaGV'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'])
            logger.debug(f"  DIM_GIANG_VIEN: {count} dòng mới")
    
    def _load_hoc_phan(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_HOC_PHAN', 'MaHP')
        new_data = df[~df['MaHP'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'])
            logger.debug(f"  DIM_HOC_PHAN: {count} dòng mới")
    
    def _load_sinh_vien(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_SINH_VIEN', 'MaSV')
        new_data = df[~df['MaSV'].isin(existing)].copy()
        if not new_data.empty:
            new_data['NgaySinh'] = new_data['NgaySinh'].dt.strftime('%Y-%m-%d')
            count = self.db.bulk_insert(new_data, 'DIM_SINH_VIEN', 
                                       ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop', 'IsCTS'])
            logger.debug(f"  DIM_SINH_VIEN: {count} dòng mới")
    
    def _load_lop_hp(self, df: pd.DataFrame):
        if df.empty:
            return
        existing = self.db.get_existing_ids('DIM_LOP_HOC_PHAN', 'MaLopHP')
        new_data = df[~df['MaLopHP'].isin(existing)]
        if not new_data.empty:
            count = self.db.bulk_insert(new_data, 'DIM_LOP_HOC_PHAN', 
                                       ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'])
            logger.debug(f"  DIM_LOP_HOC_PHAN: {count} dòng mới")
