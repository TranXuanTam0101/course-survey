import os
import sys
import re
import io
import csv
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import pandas as pd
import numpy as np
import pymssql
from azure.storage.blob import BlobServiceClient

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

DB_CONFIG = {
    'server': 'course-survey.database.windows.net',
    'user': 'sqladmin',
    'password': 'Due@2026',
    'database': 'course-survey-db',
    'timeout': 120,
    'autocommit': False
}

# ================= TRỌNG SỐ CHO TỪNG CỘT =================
WEIGHTS_CAU13 = {
    'chuẩn đầu ra': 5.0, 'mục tiêu môn học': 4.5, 'đáp ứng chương trình': 4.0,
    'nội dung': 3.0, 'học phần': 3.0, 'chương trình': 2.5, 'môn học': 2.5,
    'trang bị': 2.0, 'cung cấp': 2.0, 'đào tạo': 2.0, 'bám sát': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'đầy đủ': 1.0, 'hợp lý': 1.0,
    'chất lượng': 1.0, 'bổ ích': 1.0, 'cần thiết': 1.0, 'quan trọng': 1.0,
    'chi tiết': 1.0, 'cụ thể': 1.0, 'chuẩn': 1.0
}

WEIGHTS_CAU14 = {
    'giảng viên': 5.0, 'thầy giáo': 5.0, 'cô giáo': 5.0, 'tận tâm': 4.5,
    'nhiệt tình': 4.0, 'tận tình': 4.0, 'truyền cảm hứng': 4.0,
    'thầy': 3.0, 'cô': 3.0, 'gv': 3.0, 'dạy': 3.0, 'giảng': 3.0,
    'nhiệt huyết': 3.0, 'tâm huyết': 3.0, 'dễ hiểu': 3.0,
    'bài giảng': 2.0, 'truyền đạt': 2.0, 'giải thích': 2.0, 'hướng dẫn': 2.0,
    'sinh động': 2.0, 'linh hoạt': 2.0, 'đa dạng': 2.0, 'thu hút': 2.0,
    'tương tác': 2.0, 'sôi nổi': 2.0, 'thú vị': 2.0, 'hấp dẫn': 2.0,
    'vui vẻ': 1.0, 'thân thiện': 1.0, 'gần gũi': 1.0, 'thoải mái': 1.0,
    'hay': 1.0, 'tốt': 1.0
}

WEIGHTS_CAU15 = {
    'kiểm tra': 5.0, 'đánh giá': 5.0, 'công bằng': 4.5, 'minh bạch': 4.0,
    'đánh giá đúng': 4.0, 'phản ánh đúng': 4.0,
    'thi': 3.0, 'đề thi': 3.0, 'bài kiểm tra': 3.0, 'cho điểm': 3.0,
    'công khai': 3.0, 'nghiêm túc': 3.0, 'khách quan': 3.0,
    'điểm': 2.0, 'bài tập': 2.0, 'chấm': 2.0, 'giữa kỳ': 2.0, 'cuối kỳ': 2.0,
    'thực lực': 2.0, 'công tâm': 2.0, 'chính xác': 2.0,
    'phù hợp': 1.0, 'rõ ràng': 1.0, 'kỹ càng': 1.0, 'chỉnh chu': 1.0
}

WEIGHTS_CAU16 = {
    'không có góp ý': 5.0, 'không ý kiến': 5.0, 'không góp ý': 4.5,
    'không': 3.0, 'ko': 3.0, 'k': 2.5, 'không có': 3.0,
    'tuyệt vời': 2.0, 'quá ok': 2.0, 'rất ok': 2.0, 'ổn hết': 2.0,
    'ok': 1.0, 'oki': 1.0, 'ổn': 1.0, 'được': 1.0, 'cảm ơn': 1.0, 'tốt hơn': 1.0
}

ALL_WEIGHTS = {
    'Cau13': WEIGHTS_CAU13,
    'Cau14': WEIGHTS_CAU14,
    'Cau15': WEIGHTS_CAU15,
    'Cau16': WEIGHTS_CAU16
}

COLUMN_ORDER = ['Cau13', 'Cau14', 'Cau15', 'Cau16']

# Cache cho các hàm tính toán thường xuyên
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^(\d{2})K(\d{2})$')
_cts_pattern = re.compile(r'^CTS-', re.IGNORECASE)


# ================= HELPER FUNCTIONS =================
def create_ma_khoa(ten_khoa: str) -> str:
    """Tạo MaKhoa từ chữ cái đầu viết hoa của từng từ"""
    if not isinstance(ten_khoa, str):
        return "UNKNOWN"
    words = ten_khoa.split()
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def normalize_lop(lop: str) -> Tuple[str, bool]:
    """Chuẩn hóa Lop: bỏ hậu tố ./-/_"""
    if not isinstance(lop, str):
        return "", False
    is_cts = bool(_cts_pattern.match(lop))
    if is_cts:
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip(), is_cts


def get_db_connection():
    return pymssql.connect(**DB_CONFIG)


def derive_ma_hoc_ky() -> str:
    year_part = SEMESTER.replace('-', '')[2:]
    if '252' in SURVEY_FILE:
        hoc_ky = '2'
    elif '251' in SURVEY_FILE:
        hoc_ky = '1'
    else:
        hoc_ky = '2'
    return f"HK{hoc_ky}_{year_part}"


def safe_str(value) -> str:
    if value is None or pd.isna(value):
        return ''
    return str(value).strip()


# ================= CÁC HÀM TIỀN XỬ LÝ (GIỮ NGUYÊN) =================
def is_date_format(value):
    return isinstance(value, str) and bool(_date_pattern.match(value.strip()))


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    return bool(_ma_gv_pattern.match(value.strip()))


def calculate_weighted_score(text, column_name):
    if not text or not isinstance(text, str):
        return 0.0
    text_lower = text.lower()
    total_score = 0.0
    weights = ALL_WEIGHTS.get(column_name, {})
    for keyword, weight in weights.items():
        if keyword in text_lower:
            count = text_lower.count(keyword)
            total_score += weight * (1 + 0.1 * (count - 1))
    total_score += min(len(text) * 0.03, 1.0)
    return total_score


def get_phrase_bonus(segment_parts):
    if len(segment_parts) < 2:
        return 0.0
    merged_text = ' '.join(segment_parts).lower()
    bonus = 0.0
    if 'nội dung' in merged_text:
        if 'đầy đủ' in merged_text or 'chi tiết' in merged_text:
            bonus += 1.0
    if 'đầu ra' in merged_text and 'chuẩn' in merged_text:
        bonus += 1.0
    if ('đánh giá' in merged_text or 'kiểm tra' in merged_text) and 'cụ thể' in merged_text:
        bonus += 1.5
    if ('đánh giá' in merged_text or 'kiểm tra' in merged_text) and 'công bằng' in merged_text:
        bonus += 1.0
    if ('giảng viên' in merged_text or 'bài giảng' in merged_text) and 'nhiệt tình' in merged_text:
        bonus += 1.0
    if 'bài giảng' in merged_text and 'dễ hiểu' in merged_text:
        bonus += 1.0
    return bonus


def split_by_condition_1(text):
    parts = []
    current = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] == ',':
            has_space_before = (i > 0 and text[i-1] == ' ')
            has_space_after = (i + 1 < n and text[i+1] == ' ')
            if not has_space_before and not has_space_after:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
    if current:
        parts.append(''.join(current).strip())
    return [p for p in parts if p]


def split_by_condition_2(text):
    parts = []
    current = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] == ',':
            if i + 1 < n and text[i+1] == ' ':
                current.append(',')
            else:
                if current:
                    parts.append(''.join(current).strip())
                    current = []
        else:
            current.append(text[i])
        i += 1
    if current:
        parts.append(''.join(current).strip())
    return [p for p in parts if p]


def split_by_condition_3(text):
    parts = []
    current = []
    i = 0
    n = len(text)
    while i < n:
        if text[i] == ',':
            if i + 1 < n:
                next_char = text[i + 1]
                if next_char != ' ' and next_char.isupper():
                    if current:
                        parts.append(''.join(current).strip())
                        current = []
                else:
                    current.append(',')
            else:
                current.append(',')
        else:
            current.append(text[i])
        i += 1
    if current:
        parts.append(''.join(current).strip())
    return [p for p in parts if p]


def try_create_4th_column(parts):
    if len(parts) == 3:
        last_col = parts[-1]
        if ',' in last_col:
            sub_parts = last_col.split(',')
            if len(sub_parts) >= 2:
                last_element = sub_parts[-1].strip()
                parts[-1] = ','.join(sub_parts[:-1]).strip()
                parts.append(last_element)
                return True, parts
    return False, parts


def sequential_scoring_classification(parts):
    if not parts:
        return []
    n = len(parts)
    num_columns = 4
    dp = [[-1e9] * num_columns for _ in range(n + 1)]
    choice = [[None] * num_columns for _ in range(n + 1)]
    dp[0][0] = 0
    for i in range(n):
        for j in range(num_columns):
            if dp[i][j] < -1e8:
                continue
            remaining_columns = num_columns - j
            min_remaining_parts = remaining_columns - 1
            max_k = n - i - min_remaining_parts
            for k in range(1, max_k + 1):
                segment_parts = parts[i:i+k]
                merged_text = ', '.join(segment_parts)
                base_score = calculate_weighted_score(merged_text, COLUMN_ORDER[j])
                score = base_score + get_phrase_bonus(segment_parts)
                if j + 1 < num_columns:
                    new_score = dp[i][j] + score
                    if new_score > dp[i + k][j + 1]:
                        dp[i + k][j + 1] = new_score
                        choice[i + k][j + 1] = (i, j, k, merged_text)
                else:
                    if i + k == n:
                        new_score = dp[i][j] + score
                        if new_score > dp[i + k][j]:
                            dp[i + k][j] = new_score
                            choice[i + k][j] = (i, j, k, merged_text)
    if dp[n][num_columns - 1] < -1e8:
        return fallback_even_split(parts)
    assignments = []
    i, j = n, num_columns - 1
    while i > 0 and j >= 0:
        if choice[i][j] is None:
            break
        prev_i, prev_j, k, text = choice[i][j]
        assignments.insert(0, {'column': COLUMN_ORDER[prev_j], 'text': text, 'num_parts': k})
        i, j = prev_i, prev_j
    return assignments


def fallback_even_split(parts):
    n = len(parts)
    num_columns = 4
    sizes = [1] * num_columns
    remaining = n - num_columns
    for i in range(remaining):
        sizes[i % num_columns] += 1
    assignments = []
    start = 0
    for col_idx, size in enumerate(sizes):
        end = start + size
        assignments.append({'column': COLUMN_ORDER[col_idx], 'text': ', '.join(parts[start:end]), 'num_parts': size})
        start = end
    return assignments


def split_after_null_by_scoring(after_null_list, row_number=None):
    if not after_null_list:
        return ['', '', '', ''], None
    original_text = ','.join(after_null_list)
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success and len(new_parts) == 4:
            return new_parts[:4], None
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    if len(best_parts) < 4:
        error_info = {'row_number': row_number, 'original_after_null': original_text, 'message': f'Chỉ có {len(best_parts)} phần tử, cần ít nhất 4'}
        return [original_text, '', '', ''], error_info
    assignments = sequential_scoring_classification(best_parts)
    result = {col: '' for col in COLUMN_ORDER}
    for assign in assignments:
        col = assign['column']
        text = assign['text']
        if result[col]:
            result[col] = f"{result[col]}, {text}"
        else:
            result[col] = text
    return [result['Cau13'], result['Cau14'], result['Cau15'], result['Cau16']], None


def process_row(row, row_number=None):
    if not row or len(row) < 2:
        return None, None, []
    try:
        lop = row[0].strip() if len(row) > 0 else ''
        ma_sv = row[1].strip() if len(row) > 1 else ''
        ngay_sinh = ''
        ngay_sinh_index = -1
        for i in range(2, len(row)):
            if is_date_format(row[i]):
                ngay_sinh = row[i].strip()
                ngay_sinh_index = i
                break
        ho_dem = ''
        ten = ''
        if ngay_sinh_index > 1:
            ho_dem_ten_parts = row[2:ngay_sinh_index]
            ho_dem_ten_str = ' '.join([p.strip() for p in ho_dem_ten_parts if p and p.strip()])
            if ho_dem_ten_str:
                parts = ho_dem_ten_str.split()
                if len(parts) > 0:
                    ten = parts[-1]
                    ho_dem = ' '.join(parts[:-1]) if len(parts) > 1 else ''
        ma_hp = ''
        if ngay_sinh_index >= 0 and ngay_sinh_index + 1 < len(row):
            ma_hp = row[ngay_sinh_index + 1].strip()
        ma_gv = ''
        ma_gv_index = -1
        start_idx = ngay_sinh_index + 2 if ngay_sinh_index >= 0 else 0
        for i in range(start_idx, len(row)):
            if is_ma_gv_format(row[i]):
                ma_gv = row[i].strip()
                ma_gv_index = i
                break
        ten_hp = ''
        if ngay_sinh_index >= 0 and ma_gv_index > ngay_sinh_index + 1:
            ten_hp_parts = row[ngay_sinh_index + 2:ma_gv_index]
            ten_hp = ' '.join([p.strip() for p in ten_hp_parts if p and p.strip()])
        ho_dem_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 1 < len(row):
            ho_dem_gv = row[ma_gv_index + 1].strip()
        ten_gv = ''
        if ma_gv_index >= 0 and ma_gv_index + 2 < len(row):
            ten_gv = row[ma_gv_index + 2].strip()
        lop_hp = ''
        if ma_gv_index >= 0 and ma_gv_index + 3 < len(row):
            lop_hp = row[ma_gv_index + 3].strip()
        cau_hoi = ''
        if ma_gv_index >= 0 and ma_gv_index + 4 < len(row):
            cau_hoi = row[ma_gv_index + 4].strip()
        gia_tri = ''
        if ma_gv_index >= 0 and ma_gv_index + 5 < len(row):
            gia_tri = row[ma_gv_index + 5].strip()
        null_index = -1
        null_value = ''
        gia_tri_index = ma_gv_index + 5 if ma_gv_index >= 0 else -1
        if gia_tri_index >= 0 and gia_tri_index + 1 < len(row):
            potential_null = row[gia_tri_index + 1].strip()
            if potential_null.upper() == 'NULL' or potential_null == '':
                null_index = gia_tri_index + 1
                null_value = potential_null if potential_null else 'NULL'
        cau13 = cau14 = cau15 = cau16 = ''
        split_errors = []
        if null_index >= 0 and null_index + 1 < len(row):
            after_null = row[null_index + 1:]
            split_result, error = split_after_null_by_scoring(after_null, row_number)
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            if error:
                split_errors.append(error)
        return {
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
            'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri, 'NULL': null_value,
            'Cau13': cau13, 'Cau14': cau14, 'Cau15': cau15, 'Cau16': cau16
        }, None, split_errors
    except Exception as e:
        return None, str(e), []


# ================= EXTRACT FUNCTIONS =================
def download_master_data(blob_service: BlobServiceClient) -> Tuple[pd.DataFrame, pd.DataFrame]:
    container_name = "tailieu"
    prefix = f"{SEMESTER}/"
    hp_df = pd.DataFrame(columns=['MaHP', 'TenKhoa', 'TenHP', 'MaKhoa'])
    cn_df = pd.DataFrame(columns=['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh', 'MaKhoa'])
    try:
        hp_blob = blob_service.get_container_client(container_name).get_blob_client(f"{prefix}HP-Khoa.csv")
        if hp_blob.exists():
            data = hp_blob.download_blob().readall()
            raw_df = pd.read_csv(io.StringIO(data.decode('utf-8')))
            if len(raw_df.columns) >= 4:
                raw_df.columns = ['STT', 'MaHP', 'TenKhoa', 'TenHP'][:len(raw_df.columns)]
                hp_df = raw_df[['MaHP', 'TenKhoa', 'TenHP']].copy()
                hp_df['MaKhoa'] = hp_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> Đã tải {len(hp_df)} học phần từ HP-Khoa.csv")
    except Exception as e:
        print(f"  -> Cảnh báo khi tải HP-Khoa.csv: {e}")
    try:
        cn_blob = blob_service.get_container_client(container_name).get_blob_client(f"{prefix}TenChuyenNganh-Khoa.csv")
        if cn_blob.exists():
            data = cn_blob.download_blob().readall()
            raw_df = pd.read_csv(io.StringIO(data.decode('utf-8')))
            if len(raw_df.columns) >= 4:
                raw_df.columns = ['STT', 'TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh'][:len(raw_df.columns)]
                cn_df = raw_df[['TenKhoa', 'TenChuyenNganh', 'MaChuyenNganh']].copy()
                cn_df['MaKhoa'] = cn_df['TenKhoa'].apply(create_ma_khoa)
            print(f"  -> Đã tải {len(cn_df)} chuyên ngành từ TenChuyenNganh-Khoa.csv")
    except Exception as e:
        print(f"  -> Cảnh báo khi tải TenChuyenNganh-Khoa.csv: {e}")
    return hp_df, cn_df


def parse_csv_with_quotes(content: str) -> List[List[str]]:
    rows = []
    for line in content.strip().split('\n'):
        if not line.strip():
            continue
        try:
            row = next(csv.reader([line], quotechar='"', skipinitialspace=True))
            rows.append([col.strip() for col in row])
        except Exception:
            rows.append([col.strip() for col in line.split(',')])
    return rows


# ================= TRANSFORM FUNCTIONS =================
def determine_chuyen_nganh(df: pd.DataFrame, hp_master: pd.DataFrame, cn_master: pd.DataFrame) -> pd.DataFrame:
    norm_data = df['Lop'].apply(normalize_lop)
    df['LopChuanHoa'] = norm_data.apply(lambda x: x[0])
    df['IsCTS'] = norm_data.apply(lambda x: x[1])
    if not hp_master.empty and 'MaHP' in hp_master.columns:
        df = df.merge(hp_master[['MaHP', 'TenHP', 'MaKhoa']], on='MaHP', how='left', suffixes=('', '_master'))
        df['TenHP'] = df['TenHP_master'].fillna(df['TenHP'])
        df.drop(columns=['TenHP_master'], inplace=True, errors='ignore')
    else:
        df['MaKhoa'] = 'UNKNOWN'
    df['MaKhoa'] = df['MaKhoa'].fillna('UNKNOWN')
    def get_th1_cn(lop_chuan):
        if not isinstance(lop_chuan, str):
            return None
        match = _lop_pattern.match(lop_chuan)
        if match:
            return f"K{match.group(2)}"
        return None
    df['MaChuyenNganh_TH1'] = df['LopChuanHoa'].apply(get_th1_cn)
    def get_final_cn(row):
        if pd.notna(row['MaChuyenNganh_TH1']):
            return row['MaChuyenNganh_TH1']
        else:
            return row['MaKhoa']
    df['MaChuyenNganh'] = df.apply(get_final_cn, axis=1)
    cn_names = {}
    if not cn_master.empty and 'MaChuyenNganh' in cn_master.columns:
        cn_names = cn_master.set_index('MaChuyenNganh')['TenChuyenNganh'].to_dict()
    df['TenChuyenNganh'] = df['MaChuyenNganh'].apply(
        lambda x: cn_names.get(x, f"Chuyên ngành {x}" if x != 'UNKNOWN' else "Không xác định")
    )
    df.drop(columns=['MaChuyenNganh_TH1'], inplace=True, errors='ignore')
    return df


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    for col in COLUMN_ORDER:
        df[f'{col}_Score'] = df[col].apply(lambda x: calculate_weighted_score(x, col) if x else None)
    return df


def prepare_dimension_tables(df: pd.DataFrame, ma_hoc_ky: str) -> Dict[str, pd.DataFrame]:
    dims = {}
    khoa_records = []
    if 'MaKhoa' in df.columns and 'TenKhoa' in df.columns:
        khoa_records = df[['MaKhoa', 'TenKhoa']].drop_duplicates().to_dict('records')
    dims['khoa'] = pd.DataFrame(khoa_records) if khoa_records else pd.DataFrame(columns=['MaKhoa', 'TenKhoa'])
    cn_cols = ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa']
    dims['chuyen_nganh'] = df[cn_cols].drop_duplicates() if all(c in df.columns for c in cn_cols) else pd.DataFrame(columns=cn_cols)
    if not dims['chuyen_nganh'].empty:
        dims['chuyen_nganh']['MaCTDT'] = 'CTDT_CHINHQUY'
    dims['lop_sv'] = df[['LopChuanHoa', 'Lop', 'MaChuyenNganh', 'IsCTS']].drop_duplicates()
    dims['lop_sv'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    sv_cols = ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'LopChuanHoa', 'IsCTS']
    dims['sinh_vien'] = df[sv_cols].drop_duplicates(subset=['MaSV'])
    dims['sinh_vien'].rename(columns={'LopChuanHoa': 'MaLop'}, inplace=True)
    dims['sinh_vien']['NgaySinh'] = pd.to_datetime(dims['sinh_vien']['NgaySinh'], format='%d/%m/%Y', errors='coerce')
    dims['giang_vien'] = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates(subset=['MaGV'])
    dims['hoc_phan'] = df[['MaHP', 'TenHP', 'MaKhoa']].drop_duplicates(subset=['MaHP'])
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    dims['lop_hp'] = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates()
    dims['lop_hp']['MaHocKy'] = ma_hoc_ky
    return dims


def prepare_fact_table(df: pd.DataFrame) -> pd.DataFrame:
    df['MaLopHP'] = df['LopHP'] + '_' + df['MaHP']
    df['SubmissionID'] = df['MaSV'] + '*' + df['LopHP'] + '*' + df['MaGV'] + '_' + FILE_NAME
    fact_records = []
    for _, row in df.iterrows():
        for ma_cau_hoi, col in zip([13, 14, 15, 16], COLUMN_ORDER):
            fact_records.append({
                'SubmissionID': row['SubmissionID'],
                'MaCauHoi': ma_cau_hoi,
                'MaSV': row['MaSV'],
                'MaLopHP': row['MaLopHP'],
                'TraLoiSo': row.get(f'{col}_Score'),
                'TraLoiText': safe_str(row.get(col, '')),
                'IsCTS': row.get('IsCTS', False)
            })
    return pd.DataFrame(fact_records)


# ================= LOAD FUNCTIONS (TỐI ƯU BULK INSERT) =================
def bulk_insert_ignore_duplicate(conn, df: pd.DataFrame, table_name: str, columns: List[str], pk_column: str):
    """Bulk INSERT bỏ qua duplicate key"""
    if df.empty:
        print(f"  -> {table_name}: 0 dòng")
        return 0
    
    df_clean = df.drop_duplicates(subset=[pk_column]).copy()
    df_clean = df_clean[df_clean[pk_column].notna()]
    
    if df_clean.empty:
        return 0
    
    cursor = conn.cursor()
    placeholders = ', '.join(['%s'] * len(columns))
    
    # Lấy danh sách giá trị PK đã tồn tại
    pk_values = df_clean[pk_column].tolist()
    if len(pk_values) > 1000:
        # Chia nhỏ nếu quá nhiều
        existing = set()
        for i in range(0, len(pk_values), 1000):
            batch = pk_values[i:i+1000]
            place = ','.join(['%s'] * len(batch))
            cursor.execute(f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} IN ({place})", tuple(batch))
            existing.update(r[0] for r in cursor.fetchall())
    else:
        place = ','.join(['%s'] * len(pk_values))
        cursor.execute(f"SELECT {pk_column} FROM {table_name} WHERE {pk_column} IN ({place})", tuple(pk_values))
        existing = {r[0] for r in cursor.fetchall()}
    
    # Lọc chỉ giữ bản ghi mới
    df_new = df_clean[~df_clean[pk_column].isin(existing)]
    if df_new.empty:
        print(f"  -> {table_name}: 0 dòng mới")
        return 0
    
    # Bulk insert
    data = []
    for _, row in df_new.iterrows():
        tuple_row = tuple(None if pd.isna(row[c]) else row[c] for c in columns)
        data.append(tuple_row)
    
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"  -> {table_name}: thêm {len(data)} dòng mới")
        return len(data)
    except Exception as e:
        print(f"  -> Lỗi INSERT {table_name}: {e}")
        conn.rollback()
        return 0


def bulk_insert_fact(conn, df: pd.DataFrame):
    """Bulk INSERT Fact (luôn insert)"""
    if df.empty:
        print("  -> FACT_TRA_LOI_KHAO_SAT: 0 dòng")
        return 0
    
    cursor = conn.cursor()
    columns = ['SubmissionID', 'MaCauHoi', 'MaSV', 'MaLopHP', 'TraLoiSo', 'TraLoiText', 'IsCTS']
    placeholders = ', '.join(['%s'] * len(columns))
    query = f"INSERT INTO FACT_TRA_LOI_KHAO_SAT ({', '.join(columns)}) VALUES ({placeholders})"
    
    data = []
    for _, row in df.iterrows():
        data.append((
            row['SubmissionID'],
            row['MaCauHoi'],
            row['MaSV'],
            row['MaLopHP'],
            float(row['TraLoiSo']) if pd.notna(row['TraLoiSo']) else None,
            str(row['TraLoiText'])[:1000] if row['TraLoiText'] else '',
            bool(row['IsCTS'])
        ))
    
    try:
        cursor.executemany(query, data)
        conn.commit()
        print(f"  -> FACT_TRA_LOI_KHAO_SAT: thêm {len(data)} dòng mới")
        return len(data)
    except Exception as e:
        print(f"  -> Lỗi INSERT FACT: {e}")
        conn.rollback()
        return 0


def load_to_database(dims: Dict[str, pd.DataFrame], fact_df: pd.DataFrame, ma_hoc_ky: str):
    """Load dữ liệu vào database - đảm bảo thứ tự FK"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # 1. DIM_HOC_KY
        nam_hoc = SEMESTER
        hoc_ky_so = int(ma_hoc_ky[2])
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = %s)
            INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (%s, %s, %s)
        """, (ma_hoc_ky, ma_hoc_ky, nam_hoc, hoc_ky_so))
        conn.commit()
        print(f"  -> DIM_HOC_KY: đảm bảo tồn tại {ma_hoc_ky}")
        
        # 2. DIM_KHOA - PHẢI INSERT TRƯỚC
        bulk_insert_ignore_duplicate(conn, dims.get('khoa', pd.DataFrame()), 'DIM_KHOA', 
                                     ['MaKhoa', 'TenKhoa'], 'MaKhoa')
        
        # 3. DIM_CHUONG_TRINH_DAO_TAO
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY')
            INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
        """)
        conn.commit()
        
        # 4. DIM_CHUYEN_NGANH - ĐẢM BẢO MaKhoa ĐÃ TỒN TẠI
        cn_df = dims.get('chuyen_nganh', pd.DataFrame())
        if not cn_df.empty:
            # Lọc chỉ giữ bản ghi có MaKhoa hợp lệ
            cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
            valid_khoa = {r[0] for r in cursor.fetchall()}
            cn_df_valid = cn_df[cn_df['MaKhoa'].isin(valid_khoa)]
            bulk_insert_ignore_duplicate(conn, cn_df_valid, 'DIM_CHUYEN_NGANH',
                                         ['MaChuyenNganh', 'TenChuyenNganh', 'MaKhoa', 'MaCTDT'], 'MaChuyenNganh')
        
        # 5. DIM_LOP_SINH_VIEN - ĐẢM BẢO MaChuyenNganh ĐÃ TỒN TẠI
        lop_df = dims.get('lop_sv', pd.DataFrame())
        if not lop_df.empty:
            cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
            valid_cn = {r[0] for r in cursor.fetchall()}
            lop_df_valid = lop_df[lop_df['MaChuyenNganh'].isin(valid_cn)]
            bulk_insert_ignore_duplicate(conn, lop_df_valid, 'DIM_LOP_SINH_VIEN',
                                         ['MaLop', 'Lop', 'MaChuyenNganh', 'IsCTS'], 'MaLop')
        
        # 6. DIM_GIANG_VIEN
        bulk_insert_ignore_duplicate(conn, dims.get('giang_vien', pd.DataFrame()), 'DIM_GIANG_VIEN',
                                     ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
        
        # 7. DIM_HOC_PHAN - ĐẢM BẢO MaKhoa ĐÃ TỒN TẠI
        hp_df = dims.get('hoc_phan', pd.DataFrame())
        if not hp_df.empty:
            cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
            valid_khoa = {r[0] for r in cursor.fetchall()}
            hp_df_valid = hp_df[hp_df['MaKhoa'].isin(valid_khoa)]
            bulk_insert_ignore_duplicate(conn, hp_df_valid, 'DIM_HOC_PHAN',
                                         ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
        
        # 8. DIM_LOP_HOC_PHAN - ĐẢM BẢO MaHP và MaGV ĐÃ TỒN TẠI
        lhp_df = dims.get('lop_hp', pd.DataFrame())
        if not lhp_df.empty:
            cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
            valid_hp = {r[0] for r in cursor.fetchall()}
            cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
            valid_gv = {r[0] for r in cursor.fetchall()}
            lhp_df_valid = lhp_df[lhp_df['MaHP'].isin(valid_hp) & lhp_df['MaGV'].isin(valid_gv)]
            bulk_insert_ignore_duplicate(conn, lhp_df_valid, 'DIM_LOP_HOC_PHAN',
                                         ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
        
        # 9. FACT - LUÔN INSERT
        bulk_insert_fact(conn, fact_df)
        
        print("\n✅ Hoàn tất ETL Database!")
        
    except Exception as e:
        print(f"\n❌ Lỗi DB: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()


# ================= MAIN =================
def main():
    print(f"=== SURVEY ETL PIPELINE ===")
    print(f"Semester: {SEMESTER}")
    print(f"File: {SURVEY_FILE}")
    print()
    
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"Lỗi kết nối Blob: {e}")
        sys.exit(1)
    
    # 1. EXTRACT
    print("1. EXTRACT - Đang tải dữ liệu...")
    hp_master, cn_master = download_master_data(blob_service)
    
    blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
    data = blob_client.download_blob().readall()
    content = data.decode('utf-8-sig')
    rows = parse_csv_with_quotes(content)
    print(f"  -> Đã đọc {len(rows)} dòng dữ liệu")
    
    # 2. TRANSFORM
    print("\n2. TRANSFORM - Đang xử lý...")
    processed_rows = []
    for idx, row in enumerate(rows, 1):
        result, error, _ = process_row(row, idx)
        if result:
            processed_rows.append(result)
    
    df = pd.DataFrame(processed_rows)
    print(f"  -> Đã parse {len(df)} dòng hợp lệ")
    
    if df.empty:
        print("Không có dữ liệu để xử lý")
        sys.exit(1)
    
    df = determine_chuyen_nganh(df, hp_master, cn_master)
    df = calculate_scores(df)
    
    ma_hoc_ky = derive_ma_hoc_ky()
    print(f"  -> MaHocKy: {ma_hoc_ky}")
    
    cts_count = df['IsCTS'].sum() if 'IsCTS' in df.columns else 0
    print(f"  -> Số sinh viên CTS: {cts_count}/{len(df)}")
    
    # 3. LOAD
    print("\n3. LOAD - Đang tải lên Database (BULK INSERT)...")
    dims = prepare_dimension_tables(df, ma_hoc_ky)
    fact_df = prepare_fact_table(df)
    load_to_database(dims, fact_df, ma_hoc_ky)
    
    # 4. UPLOAD PROCESSED FILE
    if len(processed_rows) > 0:
        result_df = pd.DataFrame(processed_rows)
        output_filename = f"{FILE_NAME}_processed_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        output = result_df.to_csv(index=False, encoding='utf-8-sig')
        processed_container = blob_service.get_container_client("processed-data")
        if not processed_container.exists():
            processed_container.create_container()
        processed_container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        print(f"\n✅ File kết quả: {output_path}")
    
    print("\n=== HOÀN THÀNH ===")


if __name__ == "__main__":
    main()
