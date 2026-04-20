"""
Các hàm helper dùng chung
"""
import re
from typing import Tuple
from ..config.settings import Settings

def safe_str(value) -> str:
    """Convert value to safe string"""
    if value is None or (hasattr(value, 'isna') and value.isna()):
        return ''
    return str(value).strip()

def create_ma_khoa(ten_khoa: str) -> str:
    """Tạo mã khoa từ tên khoa"""
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "UNKNOWN"
    words = ten_khoa.split()
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"

def normalize_lop(lop: str) -> Tuple[str, bool]:
    """Chuẩn hóa mã lớp"""
    if not isinstance(lop, str):
        return "", False
    is_cts = bool(Settings.CTS_PATTERN.match(lop))
    if is_cts:
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip(), is_cts

def derive_ma_hoc_ky(semester: str, survey_file: str) -> str:
    """Tạo mã học kỳ"""
    years = semester.split('-')
    year_part = years[0][2:] + years[1][2:]
    if '252' in survey_file:
        hoc_ky = '2'
    elif '251' in survey_file:
        hoc_ky = '1'
    else:
        hoc_ky = '2'
    return f"HK{hoc_ky}_{year_part}"
