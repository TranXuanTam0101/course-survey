"""
Transform: Parse file CSV survey thành DataFrame
"""
import csv
import pandas as pd
from typing import List, Dict
from loguru import logger
from ..config.settings import Settings

class SurveyParser:
    """Parse file survey CSV nhanh"""
    
    def parse(self, content: str) -> pd.DataFrame:
        """
        Parse content CSV thành DataFrame
        Tối ưu: Sử dụng list comprehension và batch processing
        """
        logger.info("Đang parse file survey...")
        
        lines = content.strip().split('\n')
        
        # Parse từng dòng
        results = []
        for line in lines:
            if not line.strip():
                continue
            
            row_data = self._parse_single_row(line)
            if row_data:
                results.append(row_data)
        
        df = pd.DataFrame(results)
        logger.success(f"Đã parse {len(df)} dòng hợp lệ")
        
        return df
    
    def _parse_single_row(self, line: str) -> Dict:
        """Parse một dòng CSV"""
        try:
            row = next(csv.reader([line], quotechar='"', skipinitialspace=True))
            row = [col.strip() for col in row]
        except:
            row = [col.strip() for col in line.split(',')]
        
        # Tìm NULL
        null_idx = -1
        for i, val in enumerate(row):
            if isinstance(val, str) and val.upper().strip() == 'NULL':
                null_idx = i
                break
        
        if null_idx == -1:
            return None
        
        # Lấy câu trả lời (sau NULL)
        after = [str(x) for x in row[null_idx+1:] if x]
        answers = ','.join(after)
        parts = [p.strip() for p in answers.split(',') if p.strip()]
        
        cau13 = parts[0] if len(parts) > 0 else ''
        cau14 = parts[1] if len(parts) > 1 else ''
        cau15 = parts[2] if len(parts) > 2 else ''
        cau16 = parts[3] if len(parts) > 3 else ''
        
        # Lấy thông tin trước NULL
        left = row[:null_idx]
        
        # Tìm ngày sinh
        ngay_sinh_idx = -1
        for i, v in enumerate(left):
            if isinstance(v, str) and Settings.DATE_PATTERN.match(v.strip()):
                ngay_sinh_idx = i
                break
        
        if ngay_sinh_idx == -1:
            return None
        
        ngay_sinh = left[ngay_sinh_idx].strip()
        ma_hp = left[ngay_sinh_idx + 1].strip() if ngay_sinh_idx + 1 < len(left) else ''
        
        # Tìm MaGV
        ma_gv = ''
        ma_gv_idx = -1
        for i in range(len(left) - 1, ngay_sinh_idx + 2, -1):
            if isinstance(left[i], str) and Settings.MA_GV_PATTERN.match(left[i].strip()):
                ma_gv = left[i].strip()
                ma_gv_idx = i
                break
        
        if ma_gv_idx == -1:
            ma_gv_idx = len(left) - 4
        
        # Lấy thông tin học phần
        ten_hp = ' '.join(str(x).strip() for x in left[ngay_sinh_idx+2:ma_gv_idx] if x and str(x).strip())
        
        # Thông tin giảng viên
        ho_dem_gv = left[ma_gv_idx+1].strip() if ma_gv_idx+1 < len(left) else ''
        ten_gv = left[ma_gv_idx+2].strip() if ma_gv_idx+2 < len(left) else ''
        lop_hp = left[ma_gv_idx+3].strip() if ma_gv_idx+3 < len(left) else ''
        
        # Thông tin sinh viên
        ho_ten_parts = left[2:ngay_sinh_idx]
        ho_ten = ' '.join(str(x).strip() for x in ho_ten_parts if x and str(x).strip())
        name_parts = ho_ten.split()
        ten = name_parts[-1] if name_parts else ''
        ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
        
        return {
            'Lop': str(left[0]).strip() if len(left) > 0 else '',
            'MaSV': str(left[1]).strip() if len(left) > 1 else '',
            'HoDem': ho_dem,
            'Ten': ten,
            'NgaySinh': ngay_sinh,
            'MaHP': ma_hp,
            'TenHP': ten_hp,
            'MaGV': ma_gv,
            'HoDemGV': ho_dem_gv,
            'TenGV': ten_gv,
            'LopHP': lop_hp,
            'Cau13': cau13,
            'Cau14': cau14,
            'Cau15': cau15,
            'Cau16': cau16
        }
