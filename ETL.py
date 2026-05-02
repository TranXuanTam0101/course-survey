
import os
import sys
import re
import io
import time
import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import multiprocessing as mp

# ================= CONFIG =================
CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "Due@2026")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ODBC Connection
CONN_STR = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER=course-survey.database.windows.net;"
    f"DATABASE=course-survey-db;"
    f"UID=sqladmin;"
    f"PWD={DB_PASSWORD};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Connection Timeout=120;"
    f"Command Timeout=300;"
)

CONTAINER_NAME = SEMESTER
RAWDATA_PATH = "rawdata"
TAILIEU_CONTAINER = "tailieu"

# Số lượng worker
NUM_WORKERS = max(2, (mp.cpu_count() - 1) // 2)
CHUNK_SIZE = 10000
BATCH_SIZE = 50000

print("=" * 70)
print("🚀 SURVEY ETL - LOAD TO DATABASE")
print("=" * 70)
print(f"📅 Semester: {SEMESTER}")
print(f"📄 File: {SURVEY_FILE}")
print(f"👷 Workers: {NUM_WORKERS}")
print("=" * 70)

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')

# ================= NLP CONFIG =================
TAG_KEYWORDS = {
    'Tag_HocPhan': {
        'strong': [
            'nội dung', 'chương trình', 'môn học', 'học phần', 'kiến thức',
            'chuẩn đầu ra', 'mục tiêu', 'đề cương', 'tài liệu', 'giáo trình',
            'bài tập', 'thực hành', 'lý thuyết', 'cấu trúc', 'phân bố'
        ],
        'medium': [
            'trang bị', 'cung cấp', 'đào tạo', 'bám sát', 'phù hợp',
            'rõ ràng', 'đầy đủ', 'hợp lý', 'bổ ích', 'cần thiết',
            'quan trọng', 'chi tiết', 'cụ thể', 'logic', 'hệ thống',
            'cập nhật', 'mới', 'hiện đại', 'thực tế', 'ứng dụng'
        ],
        'weak': [
            'slide', 'ví dụ', 'minh họa', 'phần mềm', 'video', 'hình ảnh'
        ]
    },
    'Tag_DayHoc': {
        'strong': [
            'giảng viên', 'thầy', 'cô', 'dạy', 'giảng dạy', 'truyền đạt',
            'hướng dẫn', 'giải thích', 'phương pháp', 'cách dạy'
        ],
        'medium': [
            'nhiệt tình', 'tận tâm', 'tâm huyết', 'nhiệt huyết', 'truyền cảm hứng',
            'dễ hiểu', 'sinh động', 'linh hoạt', 'đa dạng', 'thu hút',
            'tương tác', 'sôi nổi', 'thú vị', 'hấp dẫn', 'chuyên nghiệp',
            'kinh nghiệm', 'chuyên môn', 'sâu', 'rộng'
        ],
        'weak': [
            'vui vẻ', 'thân thiện', 'gần gũi', 'thoải mái', 'hay', 'tốt', 'ok', 'ổn'
        ]
    },
    'Tag_KiemTra': {
        'strong': [
            'kiểm tra', 'đánh giá', 'thi', 'bài kiểm tra', 'đề thi',
            'chấm điểm', 'cho điểm', 'điểm số', 'kết quả'
        ],
        'medium': [
            'công bằng', 'minh bạch', 'khách quan', 'nghiêm túc', 'chính xác',
            'đánh giá đúng', 'phản ánh đúng', 'công tâm', 'thực lực',
            'công khai', 'kỹ càng', 'chỉnh chu'
        ],
        'weak': [
            'giữa kỳ', 'cuối kỳ', 'bài tập', 'điểm', 'đạt', 'qua'
        ]
    },
    'Tag_Khac': {
        'strong': [
            'cơ sở vật chất', 'phòng học', 'máy chiếu', 'điều hòa',
            'bàn ghế', 'thư viện', 'wifi', 'internet', 'hỗ trợ',
            'tư vấn', 'đăng ký', 'lịch học', 'thời khóa biểu'
        ],
        'medium': [
            'góp ý', 'đề xuất', 'kiến nghị', 'mong muốn', 'cải thiện',
            'nâng cao', 'bổ sung', 'điều chỉnh', 'thay đổi'
        ],
        'weak': [
            'không', 'không có', 'ko', 'cảm ơn', 'tốt'
        ]
    }
}

SENTIMENT_WORDS = {
    'POSITIVE': {
        'strong': [
            'tuyệt vời', 'xuất sắc', 'rất tốt', 'rất hay', 'hoàn hảo',
            'tuyệt', 'quá tốt', 'rất hài lòng', 'rất thích', 'rất bổ ích',
            'rất hiệu quả', 'rất ấn tượng', 'rất chất lượng', 'rất chuyên nghiệp'
        ],
        'medium': [
            'tốt', 'hay', 'hài lòng', 'thích', 'bổ ích', 'hiệu quả',
            'ấn tượng', 'chất lượng', 'chuyên nghiệp', 'hữu ích',
            'phù hợp', 'hợp lý', 'rõ ràng', 'dễ hiểu', 'nhiệt tình',
            'tận tâm', 'sinh động', 'thú vị', 'hấp dẫn', 'công bằng'
        ],
        'weak': [
            'ok', 'ổn', 'được', 'tạm được', 'khá', 'cảm ơn', 'cố gắng', 'nỗ lực'
        ]
    },
    'NEGATIVE': {
        'strong': [
            'rất tệ', 'rất kém', 'rất chán', 'rất dở', 'thất vọng',
            'không hài lòng', 'không thích', 'lãng phí', 'vô ích',
            'không hiệu quả', 'tệ hại', 'kinh khủng', 'tồi tệ'
        ],
        'medium': [
            'tệ', 'kém', 'chán', 'dở', 'không tốt', 'không hay',
            'không phù hợp', 'không hợp lý', 'không rõ ràng',
            'khó hiểu', 'nhàm chán', 'thiếu', 'chưa tốt', 'hạn chế',
            'bất cập', 'không công bằng', 'thiên vị', 'qua loa'
        ],
        'weak': [
            'cần cải thiện', 'nên cải thiện', 'mong cải thiện',
            'chưa được', 'chưa ổn', 'chưa tốt lắm', 'hơi'
        ]
    },
    'NEUTRAL': {
        'strong': [
            'không có góp ý', 'không ý kiến', 'không có ý kiến',
            'không góp ý', 'không có gì', 'không biết'
        ],
        'medium': [
            'không', 'ko', 'không có', 'cũng được'
        ],
        'weak': [
            'bình thường', 'tạm được', 'tàm tạm'
        ]
    }
}

# ================= MASTER DATA CACHE =================
_master_cn = None  # Cache cho TenChuyenNganh-Khoa.csv
_master_hp = None  # Cache cho HP-Khoa.csv

# ================= UTILITY FUNCTIONS =================
def is_date_format(value):
    return isinstance(value, str) and bool(_date_pattern.match(str(value).strip()))

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = str(value).strip()
    return (len(v) == 7 and v.isdigit()) or (v.startswith("TG") and len(v) == 7) or v == "gvDacThu_TKTH"

def derive_ma_hoc_ky():
    file_number = SURVEY_FILE.replace('.csv', '').split('_')[-1]
    year_code = int(file_number[:-1])
    hoc_ky = int(file_number[-1])
    nam_bat_dau = 2000 + (year_code - 1)
    nam_ket_thuc = nam_bat_dau + 1
    nam_hoc = f"{nam_bat_dau}-{nam_ket_thuc}"
    year_part = f"{nam_bat_dau % 100}{nam_ket_thuc % 100}"
    ma_hoc_ky = f"HK{hoc_ky}_{year_part}"
    return ma_hoc_ky, nam_hoc, hoc_ky

def normalize_lop(lop):
    """Chuẩn hóa mã lớp"""
    if not isinstance(lop, str):
        return ""
    if lop.upper().startswith('CTS-'):
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip()

def create_ma_khoa(ten_khoa):
    """Tạo MaKhoa từ TenKhoa"""
    if not isinstance(ten_khoa, str) or not ten_khoa:
        return "UNKNOWN"
    
    # Mapping đặc biệt
    special_map = {
        'trường đhsp': 'TĐHSP',
        'trường đhkt': 'TĐHKT',
        'trường đhnn': 'TĐHNN',
        'phòng đào tạo': 'PĐT',
    }
    
    ten_lower = ten_khoa.lower().strip()
    for key, value in special_map.items():
        if key in ten_lower:
            return value
    
    # Tạo từ chữ cái đầu
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = ''.join([w[0].upper() for w in words if w and w[0].isalpha()])
    return initials if initials else "UNKNOWN"


# ================= LOAD MASTER DATA =================
def load_master_data(blob_service):
    """Load dữ liệu master từ Azure Blob"""
    global _master_cn, _master_hp
    
    print("\n📚 LOAD MASTER DATA")
    
    # Load TenChuyenNganh-Khoa.csv
    cn_path = "TenChuyenNganh-Khoa.csv"
    print(f"  -> Loading {cn_path}...")
    cn_content = download_blob(blob_service, TAILIEU_CONTAINER, cn_path)
    
    if cn_content:
        df_cn = pd.read_csv(io.StringIO(cn_content))
        # Chuẩn hóa tên cột
        df_cn.columns = [c.strip() for c in df_cn.columns]
        
        # Tìm cột chứa thông tin
        col_map = {}
        for col in df_cn.columns:
            col_lower = col.lower()
            if 'khoa' in col_lower and 'mã' not in col_lower:
                col_map['TenKhoa'] = col
            elif 'ngành' in col_lower and 'chuyên' not in col_lower and 'khối' not in col_lower:
                col_map['TenNganh'] = col
            elif 'chuyên ngành' in col_lower or 'chuyên' in col_lower:
                col_map['TenChuyenNganh'] = col
            elif 'mã cn' in col_lower or 'mã' in col_lower:
                col_map['MaChuyenNganh'] = col
        
        # Rename columns
        df_cn = df_cn.rename(columns={v: k for k, v in col_map.items()})
        
        # Tạo MaKhoa và MaNganh
        df_cn['MaKhoa'] = df_cn['TenKhoa'].apply(create_ma_khoa)
        df_cn['MaNganh'] = df_cn['TenNganh'].apply(
            lambda x: ''.join([w[0].upper() for w in re.split(r'[\s\-]+', str(x)) if w and w[0].isalpha()])
        )
        
        _master_cn = df_cn
        print(f"     -> Loaded {len(df_cn)} rows")
        print(f"     -> Columns: {list(df_cn.columns)}")
    else:
        print("     -> File not found!")
    
    # Load HP-Khoa.csv
    hp_path = "HP-Khoa.csv"
    print(f"  -> Loading {hp_path}...")
    hp_content = download_blob(blob_service, TAILIEU_CONTAINER, hp_path)
    
    if hp_content:
        df_hp = pd.read_csv(io.StringIO(hp_content))
        df_hp.columns = [c.strip() for c in df_hp.columns]
        
        # Tìm cột
        col_map = {}
        for col in df_hp.columns:
            col_lower = col.lower()
            if 'mã học phần' in col_lower or 'mã hp' in col_lower or col_lower == 'mã':
                col_map['MaHP'] = col
            elif 'tên học phần' in col_lower or 'tên hp' in col_lower or 'tên' in col_lower:
                col_map['TenHP'] = col
            elif 'khoa' in col_lower:
                col_map['TenKhoa'] = col
        
        df_hp = df_hp.rename(columns={v: k for k, v in col_map.items()})
        
        # Xử lý đặc biệt: Ngữ Văn - Truyền thông và Toán - Tin -> Trường ĐHSP
        df_hp['TenKhoa_Original'] = df_hp['TenKhoa']
        mask_dhsp = df_hp['TenKhoa'].str.contains('Ngữ Văn|Toán', case=False, na=False)
        df_hp.loc[mask_dhsp, 'TenKhoa'] = 'Trường ĐHSP'
        
        # Tạo MaKhoa
        df_hp['MaKhoa'] = df_hp['TenKhoa'].apply(create_ma_khoa)
        
        _master_hp = df_hp
        print(f"     -> Loaded {len(df_hp)} rows")
        print(f"     -> Columns: {list(df_hp.columns)}")
    else:
        print("     -> File not found!")
    
    return _master_cn is not None or _master_hp is not None


# ================= LOOKUP FUNCTIONS =================
def lookup_chuyen_nganh(lop):
    """Tra cứu thông tin Chuyên ngành, Ngành, Khoa từ mã lớp"""
    lop_normalized = normalize_lop(lop)
    
    # Trường hợp 1: Lớp khớp pattern XXKXX
    if _lop_pattern.match(lop_normalized):
        ma_cn = f"K{lop_normalized[3:5]}"
        
        if _master_cn is not None and not _master_cn.empty:
            # Tìm trong master data
            match = _master_cn[_master_cn['MaChuyenNganh'].astype(str).str.strip() == ma_cn]
            if not match.empty:
                row = match.iloc[0]
                return {
                    'MaChuyenNganh': str(row['MaChuyenNganh']).strip(),
                    'TenChuyenNganh': str(row['TenChuyenNganh']).strip() if pd.notna(row.get('TenChuyenNganh')) else f"Chuyên ngành {ma_cn}",
                    'MaNganh': str(row['MaNganh']).strip() if pd.notna(row.get('MaNganh')) else f"N{ma_cn}",
                    'TenNganh': str(row['TenNganh']).strip() if pd.notna(row.get('TenNganh')) else f"Ngành {ma_cn}",
                    'MaKhoa': str(row['MaKhoa']).strip() if pd.notna(row.get('MaKhoa')) else "TĐHKT",
                    'TenKhoa': str(row['TenKhoa']).strip() if pd.notna(row.get('TenKhoa')) else "Trường ĐHKT"
                }
        
        # Fallback nếu không tìm thấy trong master
        return {
            'MaChuyenNganh': ma_cn,
            'TenChuyenNganh': f"Chuyên ngành {ma_cn}",
            'MaNganh': f"N{ma_cn}",
            'TenNganh': f"Ngành {ma_cn}",
            'MaKhoa': "TĐHKT",
            'TenKhoa': "Trường ĐHKT"
        }
    
    # Trường hợp 2: Lớp KHÔNG khớp pattern
    else:
        if _master_cn is not None and not _master_cn.empty:
            # Tìm trong master data theo TenKhoa hoặc tên khác
            match = _master_cn[
                _master_cn['TenKhoa'].str.contains(lop_normalized, case=False, na=False) |
                _master_cn['TenChuyenNganh'].str.contains(lop_normalized, case=False, na=False)
            ]
            if not match.empty:
                row = match.iloc[0]
                return {
                    'MaChuyenNganh': str(row['MaChuyenNganh']).strip() if pd.notna(row.get('MaChuyenNganh')) else lop_normalized,
                    'TenChuyenNganh': str(row['TenChuyenNganh']).strip() if pd.notna(row.get('TenChuyenNganh')) else lop,
                    'MaNganh': str(row['MaNganh']).strip() if pd.notna(row.get('MaNganh')) else lop_normalized,
                    'TenNganh': str(row['TenNganh']).strip() if pd.notna(row.get('TenNganh')) else lop,
                    'MaKhoa': str(row['MaKhoa']).strip() if pd.notna(row.get('MaKhoa')) else "TĐHKT",
                    'TenKhoa': str(row['TenKhoa']).strip() if pd.notna(row.get('TenKhoa')) else lop
                }
        
        # Fallback
        return {
            'MaChuyenNganh': lop_normalized if lop_normalized else lop,
            'TenChuyenNganh': lop,
            'MaNganh': lop_normalized if lop_normalized else lop,
            'TenNganh': lop,
            'MaKhoa': "TĐHKT",
            'TenKhoa': "Trường ĐHKT"
        }


def lookup_hoc_phan(ma_hp):
    """Tra cứu thông tin Học phần và Khoa quản lý học phần"""
    if not ma_hp or pd.isna(ma_hp):
        return {
            'TenHP': '',
            'TenKhoa_HP': 'Trường ĐHKT',
            'MaKhoa_HP': 'TĐHKT'
        }
    
    ma_hp_str = str(ma_hp).strip()
    
    if _master_hp is not None and not _master_hp.empty:
        match = _master_hp[_master_hp['MaHP'].astype(str).str.strip() == ma_hp_str]
        if not match.empty:
            row = match.iloc[0]
            return {
                'TenHP': str(row['TenHP']).strip() if pd.notna(row.get('TenHP')) else '',
                'TenKhoa_HP': str(row['TenKhoa']).strip() if pd.notna(row.get('TenKhoa')) else 'Trường ĐHKT',
                'MaKhoa_HP': str(row['MaKhoa']).strip() if pd.notna(row.get('MaKhoa')) else 'TĐHKT'
            }
    
    return {
        'TenHP': '',
        'TenKhoa_HP': 'Trường ĐHKT',
        'MaKhoa_HP': 'TĐHKT'
    }


# ================= BLOB FUNCTIONS =================
def download_blob(blob_service, container, path):
    try:
        container_client = blob_service.get_container_client(container)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            return blob.download_blob().readall().decode('utf-8-sig')
        return ""
    except Exception as e:
        print(f"  ❌ Lỗi download {path}: {e}")
        return ""

# ================= NLP FUNCTIONS =================
def count_keywords_fast(keyword_dict, text_lower):
    score = 0
    for level, keywords in keyword_dict.items():
        weight = {'strong': 3, 'medium': 2, 'weak': 1}[level]
        for keyword in keywords:
            count = text_lower.count(keyword)
            if count > 0:
                score += weight * count
    return min(score / 10.0, 1.0)

def process_essay_nlp(essay_text):
    default_result = {
        'Tag_HocPhan': 0.0, 'Tag_DayHoc': 0.0, 'Tag_KiemTra': 0.0, 'Tag_Khac': 0.0,
        'Sentiment': 'NEUTRAL', 'Is_Valid': 0
    }
    
    if not essay_text or not isinstance(essay_text, str) or essay_text.strip() == '':
        return default_result
    
    text_lower = essay_text.lower().strip()
    
    # Kiểm tra valid
    is_valid = 0
    if len(text_lower) > 10:
        invalid_patterns = [
            r'^(không|ko|k|không có|không có gì|\.|\,|\s)+$',
            r'^[\s\.\,\;\:\!\?\-]+$',
        ]
        is_valid = 0 if any(re.match(p, text_lower) for p in invalid_patterns) else 1
    elif len(text_lower) > 5:
        is_valid = 1
    
    if not is_valid:
        default_result['Is_Valid'] = 0
        return default_result
    
    # Tính tag scores
    tag_scores = {}
    for tag_name in ['Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac']:
        tag_scores[tag_name] = count_keywords_fast(TAG_KEYWORDS[tag_name], text_lower)
    
    # Tính sentiment
    sentiment_scores = {}
    for sentiment in ['POSITIVE', 'NEGATIVE', 'NEUTRAL']:
        sentiment_scores[sentiment] = count_keywords_fast(SENTIMENT_WORDS[sentiment], text_lower)
    
    # Xử lý negation
    negation_words = ['không', 'chẳng', 'chưa', 'đừng', 'không phải']
    has_negation = any(neg_word in text_lower for neg_word in negation_words)
    if has_negation:
        sentiment_scores['POSITIVE'] *= 0.7
        sentiment_scores['NEGATIVE'] *= 1.3
    
    total = sum(sentiment_scores.values())
    if total > 0:
        max_sent = max(sentiment_scores, key=sentiment_scores.get)
    else:
        max_sent = 'NEUTRAL'
    
    return {
        'Tag_HocPhan': round(tag_scores['Tag_HocPhan'], 2),
        'Tag_DayHoc': round(tag_scores['Tag_DayHoc'], 2),
        'Tag_KiemTra': round(tag_scores['Tag_KiemTra'], 2),
        'Tag_Khac': round(tag_scores['Tag_Khac'], 2),
        'Sentiment': max_sent,
        'Is_Valid': 1
    }

# ================= PARSE FUNCTIONS =================
def parse_lines_batch(args):
    """Parse một batch lines với NLP tích hợp"""
    lines_batch, _ = args  # Unpack nếu cần thêm params
    
    results = []
    
    for line in lines_batch:
        if not line or not line.strip():
            continue
            
        original_line = line.strip()
        
        try:
            # Tách theo mốc NULL
            upper_line = original_line.upper()
            if 'NULL' in upper_line:
                null_idx = upper_line.find('NULL')
                left_str = original_line[:null_idx].rstrip(', \t')
                right_str = original_line[null_idx + 4:].lstrip(', \t')
            else:
                left_str = original_line
                right_str = ''
            
            # Split phần trái
            row = [x.strip() for x in left_str.split(',') if x.strip()]
            row_len = len(row)
            
            if row_len < 10:
                continue
            
            # Trích xuất các trường
            lop = row[0]
            ma_sv = row[1]
            
            ngay_sinh_index = next(
                (i for i in range(2, min(12, row_len)) if is_date_format(row[i])), -1
            )
            if ngay_sinh_index == -1:
                continue
            
            ngay_sinh = row[ngay_sinh_index]
            
            # Họ tên SV
            name_parts = row[2:ngay_sinh_index]
            ten = name_parts[-1] if name_parts else ''
            ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            # MaGV
            ma_gv_index = next(
                (i for i in range(ngay_sinh_index + 1, min(ngay_sinh_index + 25, row_len))
                 if is_ma_gv_format(row[i])), -1
            )
            if ma_gv_index == -1:
                ma_gv_index = min(row_len - 1, ngay_sinh_index + 8)
            
            ma_gv = row[ma_gv_index] if ma_gv_index < row_len else ''
            
            # Các trường khác
            ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else ''
            ten_hp_raw = ' '.join(row[ngay_sinh_index + 2:ma_gv_index])
            
            ho_dem_gv = row[ma_gv_index + 1] if ma_gv_index + 1 < row_len else ''
            ten_gv = row[ma_gv_index + 2] if ma_gv_index + 2 < row_len else ''
            lop_hp = row[ma_gv_index + 3] if ma_gv_index + 3 < row_len else ''
            cau_hoi = row[ma_gv_index + 4] if ma_gv_index + 4 < row_len else ''
            gia_tri = row[ma_gv_index + 5] if ma_gv_index + 5 < row_len else ''
            
            # Essay text
            essay_text = right_str.replace(' , ', ', ').strip() if right_str else ''
            
            # NLP
            nlp_result = process_essay_nlp(essay_text) if essay_text else {
                'Tag_HocPhan': 0.0, 'Tag_DayHoc': 0.0,
                'Tag_KiemTra': 0.0, 'Tag_Khac': 0.0,
                'Sentiment': 'NEUTRAL', 'Is_Valid': 0
            }
            
            # ===== LOOKUP MASTER DATA =====
            # Chuyên ngành, Ngành từ Lop
            cn_info = lookup_chuyen_nganh(lop)
            
            # Học phần, Khoa quản lý học phần từ MaHP
            hp_info = lookup_hoc_phan(ma_hp)
            
            # Dùng TenHP từ master nếu có, ngược lại dùng từ raw
            ten_hp = hp_info['TenHP'] if hp_info['TenHP'] else ten_hp_raw
            
            # Khoa quản lý ngành (từ chuyên ngành)
            ma_khoa_cn = cn_info['MaKhoa']
            ten_khoa_cn = cn_info['TenKhoa']
            
            # Khoa quản lý học phần (từ HP)
            ma_khoa_hp = hp_info['MaKhoa_HP']
            ten_khoa_hp = hp_info['TenKhoa_HP']
            
            # Mã lớp, lớp HP
            ma_lop = normalize_lop(lop)
            ma_lop_hp = lop_hp if lop_hp else f"{ma_hp}_{ma_gv}"
            
            # SubmissionID
            submission_id = f"{ma_sv}_{ma_lop_hp}_{ma_gv}_{FILE_NAME}"
            
            results.append({
                # Sinh viên
                'SubmissionID': submission_id,
                'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten, 'NgaySinh': ngay_sinh,
                
                # Lớp sinh viên
                'MaLop': ma_lop, 'Lop': lop,
                
                # Chuyên ngành (từ Lop)
                'MaChuyenNganh': cn_info['MaChuyenNganh'],
                'TenChuyenNganh': cn_info['TenChuyenNganh'],
                
                # Ngành (từ Chuyên ngành)
                'MaNganh': cn_info['MaNganh'],
                'TenNganh': cn_info['TenNganh'],
                
                # Khoa quản lý ngành (từ Chuyên ngành)
                'MaKhoa_CN': ma_khoa_cn,
                'TenKhoa_CN': ten_khoa_cn,
                
                # Học phần
                'MaHP': ma_hp, 'TenHP': ten_hp,
                
                # Khoa quản lý học phần (từ HP)
                'MaKhoa_HP': ma_khoa_hp,
                'TenKhoa_HP': ten_khoa_hp,
                
                # Giảng viên
                'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
                
                # Lớp học phần
                'MaLopHP': ma_lop_hp, 'LopHP': lop_hp,
                
                # Khảo sát
                'CauHoi': cau_hoi, 'GiaTri': gia_tri,
                'EssayText': essay_text,
                
                # NLP
                'Tag_HocPhan': nlp_result['Tag_HocPhan'],
                'Tag_DayHoc': nlp_result['Tag_DayHoc'],
                'Tag_KiemTra': nlp_result['Tag_KiemTra'],
                'Tag_Khac': nlp_result['Tag_Khac'],
                'Sentiment': nlp_result['Sentiment'],
                'Is_Valid': nlp_result['Is_Valid']
            })
            
        except Exception:
            continue
    
    return results


def parse_survey_parallel(content):
    """Parse toàn bộ survey content"""
    print("  -> Parsing với NLP...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    batch_args = [(batch, None) for batch in batches]
    print(f"  -> Số batches: {len(batches)}")
    
    all_results = []
    with mp.Pool(NUM_WORKERS) as pool:
        for i, batch_results in enumerate(pool.imap_unordered(parse_lines_batch, batch_args)):
            all_results.extend(batch_results)
            if (i + 1) % 10 == 0 or i == len(batches) - 1:
                print(f"    -> Đã xong {i+1}/{len(batches)} batches, {len(all_results):,} dòng")
    
    df = pd.DataFrame(all_results)
    print(f"  -> Đã parse {len(df):,} dòng ({time.time()-start:.2f}s)")
    
    return df


# ================= DATABASE LOAD FUNCTIONS =================
def load_dimension(cursor, table, df, columns, id_col):
    """Load dữ liệu vào Dimension table"""
    if df.empty:
        return 0
    
    df = df.fillna('')
    df = df.drop_duplicates(id_col)
    
    cursor.execute(f"SELECT {id_col} FROM {table}")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = df[~df[id_col].isin(existing)]
    if new_data.empty:
        return 0
    
    print(f"    -> Inserting {len(new_data)} new records into {table}...")
    
    placeholders = ', '.join(['?'] * len(columns))
    query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    data = []
    for _, row in new_data.iterrows():
        tuple_data = []
        for c in columns:
            val = row[c]
            if c == 'NgaySinh':
                if pd.isna(val) or val == '':
                    tuple_data.append(None)
                else:
                    try:
                        dt = pd.to_datetime(val, format='%d/%m/%Y', errors='coerce')
                        tuple_data.append(dt.strftime('%Y-%m-%d') if pd.notna(dt) else None)
                    except:
                        tuple_data.append(None)
            elif c == 'HocKy':
                tuple_data.append(int(val) if pd.notna(val) and val != '' else 1)
            else:
                tuple_data.append(str(val)[:500] if val else '')
        data.append(tuple(tuple_data))
    
    cursor.fast_executemany = True
    cursor.executemany(query, data)
    cursor.connection.commit()
    
    return len(new_data)


def load_all_dimensions(cursor, df):
    """Load tất cả Dimension tables"""
    print("\n  --- LOADING DIMENSIONS ---")
    total = 0
    
    # ===== DIM_KHOA: Gộp cả khoa quản lý ngành và khoa quản lý học phần =====
    df_khoa_cn = df[['MaKhoa_CN', 'TenKhoa_CN']].drop_duplicates('MaKhoa_CN')
    df_khoa_cn.columns = ['MaKhoa', 'TenKhoa']
    
    df_khoa_hp = df[['MaKhoa_HP', 'TenKhoa_HP']].drop_duplicates('MaKhoa_HP')
    df_khoa_hp.columns = ['MaKhoa', 'TenKhoa']
    
    df_khoa = pd.concat([df_khoa_cn, df_khoa_hp]).drop_duplicates('MaKhoa')
    df_khoa = df_khoa[df_khoa['MaKhoa'] != '']
    
    # Thêm các khoa mặc định
    df_khoa_default = pd.DataFrame([
        {'MaKhoa': 'TĐHKT', 'TenKhoa': 'Trường Đại học Kinh tế'},
        {'MaKhoa': 'TĐHSP', 'TenKhoa': 'Trường Đại học Sư phạm'},
    ])
    df_khoa = pd.concat([df_khoa, df_khoa_default]).drop_duplicates('MaKhoa')
    
    count = load_dimension(cursor, 'DIM_KHOA', df_khoa, ['MaKhoa', 'TenKhoa'], 'MaKhoa')
    print(f"  ✅ DIM_KHOA: {count} new")
    total += count
    
    # ===== DIM_NGANH =====
    df_nganh = df[['MaNganh', 'TenNganh', 'MaKhoa_CN']].drop_duplicates('MaNganh')
    df_nganh.columns = ['MaNganh', 'TenNganh', 'MaKhoa']
    df_nganh = df_nganh[df_nganh['MaNganh'] != '']
    
    count = load_dimension(cursor, 'DIM_NGANH', df_nganh,
                           ['MaNganh', 'TenNganh', 'MaKhoa'], 'MaNganh')
    print(f"  ✅ DIM_NGANH: {count} new")
    total += count
    
    # ===== DIM_CHUYEN_NGANH =====
    df_cn = df[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    df_cn = df_cn[df_cn['MaChuyenNganh'] != '']
    
    count = load_dimension(cursor, 'DIM_CHUYEN_NGANH', df_cn,
                           ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh'], 'MaChuyenNganh')
    print(f"  ✅ DIM_CHUYEN_NGANH: {count} new")
    total += count
    
    # ===== DIM_LOP_SINH_VIEN =====
    df_lop = df[['MaLop', 'Lop', 'MaChuyenNganh']].drop_duplicates('MaLop')
    df_lop = df_lop[df_lop['MaLop'] != '']
    
    count = load_dimension(cursor, 'DIM_LOP_SINH_VIEN', df_lop,
                           ['MaLop', 'Lop', 'MaChuyenNganh'], 'MaLop')
    print(f"  ✅ DIM_LOP_SINH_VIEN: {count} new")
    total += count
    
    # ===== DIM_SINH_VIEN =====
    df_sv = df[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop']].drop_duplicates('MaSV')
    df_sv = df_sv[df_sv['MaSV'] != '']
    
    count = load_dimension(cursor, 'DIM_SINH_VIEN', df_sv,
                           ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], 'MaSV')
    print(f"  ✅ DIM_SINH_VIEN: {count} new")
    total += count
    
    # ===== DIM_GIANG_VIEN =====
    df_gv = df[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
    df_gv = df_gv[df_gv['MaGV'] != '']
    
    count = load_dimension(cursor, 'DIM_GIANG_VIEN', df_gv,
                           ['MaGV', 'HoDemGV', 'TenGV'], 'MaGV')
    print(f"  ✅ DIM_GIANG_VIEN: {count} new")
    total += count
    
    # ===== DIM_HOC_PHAN =====
    df_hp = df[['MaHP', 'TenHP', 'MaKhoa_HP']].drop_duplicates('MaHP')
    df_hp.columns = ['MaHP', 'TenHP', 'MaKhoa']
    df_hp = df_hp[df_hp['MaHP'] != '']
    
    count = load_dimension(cursor, 'DIM_HOC_PHAN', df_hp,
                           ['MaHP', 'TenHP', 'MaKhoa'], 'MaHP')
    print(f"  ✅ DIM_HOC_PHAN: {count} new")
    total += count
    
    # ===== DIM_HOC_KY =====
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    df_hk = pd.DataFrame([{'MaHocKy': ma_hoc_ky, 'NamHoc': nam_hoc, 'HocKy': hoc_ky}])
    
    count = load_dimension(cursor, 'DIM_HOC_KY', df_hk,
                           ['MaHocKy', 'NamHoc', 'HocKy'], 'MaHocKy')
    print(f"  ✅ DIM_HOC_KY: {count} new")
    total += count
    
    # ===== DIM_LOP_HOC_PHAN =====
    df_lhp = df[['MaLopHP', 'LopHP', 'MaHP', 'MaGV']].drop_duplicates('MaLopHP')
    df_lhp = df_lhp[df_lhp['MaLopHP'] != '']
    df_lhp['MaHocKy'] = ma_hoc_ky
    
    count = load_dimension(cursor, 'DIM_LOP_HOC_PHAN', df_lhp,
                           ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], 'MaLopHP')
    print(f"  ✅ DIM_LOP_HOC_PHAN: {count} new")
    total += count
    
    print(f"  📊 Total dimensions: {total} new records")
    return total


def load_fact_gop_y(cursor, df):
    """Load FACT_GOP_Y_TU_LUAN"""
    print("\n  --- LOADING FACT_GOP_Y_TU_LUAN ---")
    
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    if df_essay.empty:
        print("  ✅ No essay data")
        return 0
    
    print(f"  -> {len(df_essay):,} essay submissions")
    
    cursor.execute("SELECT SubmissionID FROM FACT_GOP_Y_TU_LUAN")
    existing = {row[0] for row in cursor.fetchall()}
    
    df_new = df_essay[~df_essay['SubmissionID'].isin(existing)]
    
    if df_new.empty:
        print("  ✅ 0 new rows")
        return 0
    
    print(f"  -> Inserting {len(df_new):,} new rows...")
    
    inserted = 0
    for i in range(0, len(df_new), BATCH_SIZE):
        batch = df_new.iloc[i:i+BATCH_SIZE]
        data = []
        for _, row in batch.iterrows():
            data.append((
                str(row['SubmissionID'])[:150],
                str(row['MaSV'])[:20],
                str(row['MaLopHP'])[:50],
                str(row['EssayText']) if pd.notna(row['EssayText']) else '',
                str(row['Sentiment'])[:20] if pd.notna(row['Sentiment']) else 'NEUTRAL',
                int(row['Is_Valid']) if pd.notna(row['Is_Valid']) else 0,
                1 if row['Tag_HocPhan'] >= 0.3 else 0,
                1 if row['Tag_DayHoc'] >= 0.3 else 0,
                1 if row['Tag_KiemTra'] >= 0.3 else 0,
                1 if row['Tag_Khac'] >= 0.3 else 0
            ))
        
        cursor.executemany("""
            INSERT INTO FACT_GOP_Y_TU_LUAN 
            (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid,
             Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, data)
        cursor.connection.commit()
        inserted += len(data)
    
    print(f"  ✅ FACT_GOP_Y_TU_LUAN: {inserted:,} rows")
    return inserted


def load_fact_ket_qua(cursor, df):
    """Load FACT_KET_QUA_DANH_GIA"""
    print("\n  --- LOADING FACT_KET_QUA_DANH_GIA ---")
    
    rows_all = []
    
    # Câu trắc nghiệm (1-12)
    df_tn = df[(df['CauHoi'].notna()) & (df['CauHoi'] != '') &
               (df['GiaTri'].notna()) & (df['GiaTri'] != '')]
    
    for _, row in df_tn.iterrows():
        try:
            ma_cau = int(float(row['CauHoi']))
            diem = int(float(row['GiaTri']))
            if 1 <= ma_cau <= 12 and 1 <= diem <= 5:
                rows_all.append({
                    'SubmissionID': row['SubmissionID'],
                    'MaCauHoi': ma_cau,
                    'Diem': diem
                })
        except:
            pass
    
    # Câu tự luận (13-16)
    df_essay = df[(df['EssayText'].notna()) & (df['EssayText'] != '')].drop_duplicates('SubmissionID')
    
    for _, row in df_essay.iterrows():
        sentiment = row['Sentiment']
        if sentiment == 'POSITIVE':
            diem = 5 if row['Is_Valid'] else 4
        elif sentiment == 'NEGATIVE':
            diem = 2 if row['Is_Valid'] else 1
        else:
            diem = 3
        
        for ma_cau in [13, 14, 15, 16]:
            rows_all.append({
                'SubmissionID': row['SubmissionID'],
                'MaCauHoi': ma_cau,
                'Diem': diem
            })
    
    if not rows_all:
        print("  ✅ No data")
        return 0
    
    df_kq = pd.DataFrame(rows_all)
    print(f"  -> {len(df_kq):,} rows")
    
    inserted = 0
    for i in range(0, len(df_kq), BATCH_SIZE):
        batch = df_kq.iloc[i:i+BATCH_SIZE]
        data = [(str(r['SubmissionID'])[:150], int(r['MaCauHoi']), int(r['Diem']))
                for _, r in batch.iterrows()]
        
        try:
            cursor.executemany("""
                INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem)
                VALUES (?, ?, ?)
            """, data)
            cursor.connection.commit()
            inserted += len(data)
        except:
            for d in data:
                try:
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM FACT_KET_QUA_DANH_GIA 
                                       WHERE SubmissionID = ? AND MaCauHoi = ?)
                        INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem)
                        VALUES (?, ?, ?)
                    """, (d[0], d[1], d[0], d[1], d[2]))
                    cursor.connection.commit()
                    inserted += 1
                except:
                    pass
    
    print(f"  ✅ FACT_KET_QUA_DANH_GIA: {inserted:,} rows")
    return inserted


def load_to_database(df):
    """Load toàn bộ dữ liệu vào database"""
    print("\n💾 LOAD TO DATABASE")
    start = time.time()
    
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.fast_executemany = True
    
    try:
        dim_count = load_all_dimensions(cursor, df)
        fact1_count = load_fact_gop_y(cursor, df)
        fact2_count = load_fact_ket_qua(cursor, df)
        
        print(f"\n  ✅ Load hoàn tất: {time.time()-start:.2f}s")
        print(f"  📊 Dimensions: {dim_count} new records")
        print(f"  📊 FACT_GOP_Y_TU_LUAN: {fact1_count:,} rows")
        print(f"  📊 FACT_KET_QUA_DANH_GIA: {fact2_count:,} rows")
        
    except Exception as e:
        print(f"\n  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        conn.close()


# ================= MAIN =================
def main():
    total_start = time.time()
    
    # 1. KẾT NỐI AZURE
    print("\n📥 1. CONNECT & LOAD MASTER DATA")
    start = time.time()
    
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"❌ Lỗi kết nối Azure: {e}")
        sys.exit(1)
    
    # Load master data
    load_master_data(blob_service)
    
    # Download survey
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    print(f"  ✅ Extract: {time.time()-start:.2f}s")
    
    if not survey_content:
        print("❌ Không thể đọc file survey!")
        sys.exit(1)
    
    # 2. PARSE + NLP
    print("\n📝 2. PARSE + NLP")
    start = time.time()
    df = parse_survey_parallel(survey_content)
    print(f"  ✅ Parse + NLP: {time.time()-start:.2f}s")
    
    if df.empty:
        print("❌ Không có dữ liệu!")
        sys.exit(1)
    
    # 3. SAVE BACKUP
    print("\n💾 3. SAVE BACKUP")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_path = f"/tmp/{FILE_NAME}_parsed_{timestamp}.parquet"
    df.to_parquet(local_path, index=False, compression='snappy')
    print(f"  ✅ Backup: {local_path}")
    
    # 4. LOAD TO DATABASE
    load_to_database(df)
    
    # TỔNG KẾT
    total = time.time() - total_start
    print("\n" + "=" * 70)
    print(f"🎉 HOÀN THÀNH!")
    print(f"⏱️  Tổng thời gian: {total:.1f}s")
    print(f"📊 Tổng số dòng parsed: {len(df):,}")
    print(f"📁 Backup: {local_path}")
    print("=" * 70)


if __name__ == "__main__":
    main()
