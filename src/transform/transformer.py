"""
Transform: Chuyển đổi dữ liệu thành dimensions và fact
"""
import pandas as pd
from typing import Dict, Tuple
from loguru import logger
from ..config.settings import Settings
from ..utils.helpers import normalize_lop, derive_ma_hoc_ky
from .scoring import ScoreCalculator

class DataTransformer:
    """Transform survey data thành star schema"""
    
    def __init__(self, semester: str, survey_file: str):
        self.semester = semester
        self.survey_file = survey_file
        self.ma_hoc_ky = derive_ma_hoc_ky(semester, survey_file)
        self.file_name = survey_file.rsplit('.', 1)[0]
    
    def transform(self, df: pd.DataFrame, master_data: Dict) -> Tuple[Dict, pd.DataFrame]:
        """
        Transform DataFrame thành dimensions và fact
        """
        logger.info(f"Đang transform {len(df)} dòng...")
        
        hp_master = master_data.get('hp', pd.DataFrame())
        cn_master = master_data.get('cn', pd.DataFrame())
        
        # Chuẩn hóa lớp
        norm = df['Lop'].apply(normalize_lop)
        df['LopChuanHoa'] = norm.apply(lambda x: x[0])
        df['IsCTS'] = norm.apply(lambda x: x[1])
        
        # Merge với master data
        df = self._merge_master_data(df, hp_master)
        
        # Tính điểm
        df = self._calculate_scores(df)
        
        # Tạo dimensions
        dimensions = self._create_dimensions(df)
        
        # Tạo fact
        fact_df = self._create_fact(df)
        
        logger.success(f"Transform hoàn tất: {len(dimensions)} dimensions, {len(fact_df)} fact rows")
        
        return dimensions, fact_df
    
    def _merge_master_data(self, df: pd.DataFrame, hp_master: pd.DataFrame) -> pd.DataFrame:
        """Merge với master data"""
        if not hp_master.empty:
            df = df.merge(
                hp_master[['MaHP', 'TenHP', 'MaKhoa', 'TenKhoa']], 
                on='MaHP', 
                how='left', 
                suffixes=('', '_m')
            )
            df['TenHP'] = df['TenHP_m'].fillna(df['TenHP'])
            df['TenKhoa'] = df['TenKhoa'].fillna('UNKNOWN')
            df['MaKhoa'] = df['MaKhoa'].fillna('UNKNOWN')
            df.drop(columns=['TenHP_m'], inplace=True, errors='ignore')
        else:
            df['MaKhoa'] = 'UNKNOWN'
            df['TenKhoa'] = 'UNKNOWN'
        
        return df
    
    def _calculate_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tính điểm cho các câu hỏi"""
        calculator = ScoreCalculator()
        for col in Settings.COLUMN_ORDER:
            df[f'{col}_Score'] = df[col].apply(lambda x: calculator.calculate(x, col))
        return df
    
    def _create_dimensions(self, df: pd.DataFrame) -> Dict:
        """Tạo tất cả dimensions"""
        nam_hoc = self.semester
        hoc_ky = int(self.ma_hoc_ky[2])
        
        dimensions = {}
        
        # DIM_HOC_KY
        dimensions['hoc_ky'] = pd.DataFrame([{
            'MaHocKy': self.ma_hoc_ky, 
            'NamHoc': nam_hoc, 
            'HocKy': hoc_ky
        }])
        
        # DIM_KHOA
        dimensions['khoa'] = df[['MaKhoa', 'TenKhoa']].drop_duplicates(subset=['MaKhoa'])
        dimensions['khoa'] = dimensions['khoa'][dimensions['khoa']['MaKhoa'] != 'UNKNOWN']
        
        # DIM_CHUYEN_NGANH
        df['MaChuyenNganh'] = df['MaKhoa']  # Đơn giản hóa
        df['TenChuyenNganh'] = 'Chuyên ngành ' + df['MaChuyenNganh']
        dimensions['chuyen_nganh'] = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']].drop_duplicates(subset=['MaChuyenNganh'])
        dimensions['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
        
        # DIM_LOP_SINH_VIEN
        dimensions['lop_sv'] = df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates()
        dimensions['lop_sv'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
        dimensions['lop_sv'] = dimensions['lop_sv'][dimensions['lop_sv']['MaLop'] != '']
        
        # DIM_SINH_VIEN
        dimensions['sinh_vien'] = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']].drop_duplicates(subset=['MaSV'])
        dimensions['sinh_vien'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
        dimensions['sinh_vien']['NgaySinh'] = pd.to_datetime(dimensions['sinh_vien']['NgaySinh'], format='%d/%m/%Y', errors='coerce')
        
        # DIM_GIANG_VIEN
        dimensions['giang_vien'] = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV'])
        dimensions['giang_vien'] = dimensions['giang_vien'][dimensions['giang_vien']['MaGV'] != '']
        
        # DIM_HOC_PHAN
        dimensions['hoc_phan'] = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP'])
        dimensions['hoc_phan'] = dimensions['hoc_phan'][dimensions['hoc_phan']['MaHP'] != '']
        
        # DIM_LOP_HOC_PHAN
        df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
        dimensions['lop_hp'] = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates()
        dimensions['lop_hp']['MaHocKy'] = self.ma_hoc_ky
        dimensions['lop_hp'] = dimensions['lop_hp'][dimensions['lop_hp']['MaLopHP'] != '_']
        
        return dimensions
    
    def _create_fact(self, df: pd.DataFrame) -> pd.DataFrame:
        """Tạo fact table"""
        df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + self.file_name
        df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
        
        fact_rows = []
        for _, row in df.iterrows():
            for mc, col in zip([13, 14, 15, 16], Settings.COLUMN_ORDER):
                fact_rows.append({
                    'SubmissionID': row['SubmissionID'],
                    'MaCauHoi': mc,
                    'MaSV': row['MaSV'],
                    'MaLopHP': row['MaLopHP'],
                    'TraLoiSo': row[f'{col}_Score'],
                    'TraLoiText': str(row[col])[:1000] if row[col] else '',
                    'IsCTS': row['IsCTS']
                })
        
        return pd.DataFrame(fact_rows)
