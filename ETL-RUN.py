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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
PROCESSED_PATH = "processed-data"

# Số lượng worker
NUM_WORKERS = max(2, mp.cpu_count())
CHUNK_SIZE = 50000
BATCH_SIZE = 100000

# ================= PATTERNS =================
_date_pattern = re.compile(r'^\d{2}/\d{2}/\d{4}$')
_ma_gv_pattern = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
_lop_pattern = re.compile(r'^\d{2}K\d{2}$')


# ================= HÀM TIỆN ÍCH =================
def normalize_lop(lop: str) -> str:
    if not isinstance(lop, str):
        return ""
    if lop.upper().startswith('CTS-'):
        lop = lop[4:]
    for sep in ['.', '-', '_']:
        if sep in lop:
            lop = lop.split(sep)[0]
    return lop.strip()


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


def create_ma_khoa(ten_khoa: str) -> str:
    SPECIAL_MA_KHOA = {
        'Bộ môn NNCN': 'BNNNCN', 'Trường ĐHNN': 'TĐHNN', 'Luật': 'LUAT',
        'Marketing': 'MKT', 'Trường ĐHKT': 'TĐHKT', 'Phòng Đào Tạo': 'PĐT'
    }
    for special_name, special_code in SPECIAL_MA_KHOA.items():
        if special_name.lower() in ten_khoa.lower():
            return special_code
    words = re.split(r'[\s\-]+', ten_khoa)
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def extract_ma_nganh_from_ten_nganh(ten_nganh: str) -> str:
    if not isinstance(ten_nganh, str) or not ten_nganh:
        return "UNKNOWN"
    words = re.split(r'[\s\-]+', ten_nganh.strip())
    initials = [w[0].upper() for w in words if w and w[0].isalpha()]
    return ''.join(initials) if initials else "UNKNOWN"


def determine_ma_chuyen_nganh(lop: str) -> tuple:
    """
    Xác định MaChuyenNganh từ Lop
    - Có ACCA: Kxx-ACCA (tự sinh)
    - Có CTS: trả về CTS (xử lý cứng)
    - Có QT: trả về QT (xử lý cứng)
    - Còn lại: Kxx (tự sinh)
    """
    if not lop or not isinstance(lop, str):
        return None, None, None, None
    
    lop_upper = lop.upper().strip()
    
    # ===== TH1: CÓ ACCA =====
    if 'ACCA' in lop_upper:
        match = re.search(r'K(\d{2})', lop_upper)
        if match:
            ma_cn = f"K{match.group(1)}-ACCA"
            return ma_cn, None, None, None
    
    # ===== TH2: CÓ CTS =====
    if 'CTS' in lop_upper:
        return "CTS", "Chuyên ngành CTS", "Trường ĐHKT", "TĐHKT"
    
    # ===== TH3: CÓ QT =====
    if 'QT' in lop_upper:
        return "QT", "Chuyên ngành QT", "Phòng Đào Tạo", "PĐT"
    
    # ===== TH4: CÒN LẠI - LẤY Kxx =====
    match = re.search(r'K(\d{2})', lop_upper)
    if match:
        ma_cn = f"K{match.group(1)}"
        return ma_cn, None, None, None
    
    return None, None, None, None


# ================= NLP CLASS - ĐÃ SỬA LOGIC TOÁN HỌC =================
class VietnameseNLP:
    """
    NLP Class với logic toán học đúng:
    - Xử lý phủ định (negation) - "không tốt" → negative
    - Xử lý từ tăng cường (boost) - "rất tốt" → điểm cao hơn
    - Tính điểm có trọng số
    - Phân biệt "không có ý kiến" với "không X"
    """
    
    def __init__(self):
        # ===== TỪ ĐIỂN CÓ TRỌNG SỐ =====
        self.positive_words = {
            # Trọng số 2.0 (rất mạnh)
            'tuyệt vời': 2.0, 'xuất sắc': 2.0, 'hoàn hảo': 2.0,
            'quá tuyệt': 2.0, 'mãi yêu': 2.0, 'siêu thích': 2.0,
            
            # Trọng số 1.5 (khá mạnh)
            'rất tốt': 1.5, 'rất hay': 1.5, 'cực kỳ': 1.5,
            'tuyệt': 1.5, 'hào hứng': 1.5,
            
            # Trọng số 1.0 (bình thường)
            'tốt': 1.0, 'hay': 1.0, 'ổn': 1.0, 'hài lòng': 1.0,
            'cảm ơn': 1.0, 'ok': 1.0, 'oke': 1.0, 'oki': 1.0,
            'good': 1.0, 'great': 1.0, 'excellent': 1.0,
            'thoải mái': 1.0, 'vui': 1.0, 'sôi nổi': 1.0, 'hấp dẫn': 1.0,
            'dễ mến': 1.0, 'dễ gần': 1.0, 'thân thiện': 1.0, 'gần gũi': 1.0,
            'tâm lý': 1.0, 'dễ thương': 1.0, 'vui tính': 1.0, 'có tâm': 1.0,
            'tâm huyết': 1.0, 'tận tâm': 1.0, 'tận tụy': 1.0, 'tận tình': 1.0,
            'nhiệt huyết': 1.0, 'chu đáo': 1.0, 'kỹ': 1.0, 'cẩn thận': 1.0,
            'chi tiết': 1.0, 'sâu sắc': 1.0, 'nghiêm túc': 1.0, 'linh hoạt': 1.0,
            'sáng tạo': 1.0, 'mới mẻ': 1.0, 'thực tế': 1.0, 'thực tiễn': 1.0,
            'sát ngành': 1.0, 'bám sát': 1.0, 'đúng trọng tâm': 1.0,
            'hiệu quả': 1.0, 'tiến bộ': 1.0, 'đa dạng': 1.0, 'phong phú': 1.0,
            'hợp lý': 1.0, 'chuẩn': 1.0, 'tạo điều kiện': 1.0, 'hỗ trợ': 1.0,
            'giải đáp thắc mắc': 1.0, 'chỉnh chu': 1.0
        }
        
        self.negative_words = {
            # Trọng số -2.0 (rất tệ)
            'tệ hại': -2.0, 'tồi tệ': -2.0, 'thất vọng': -2.0,
            
            # Trọng số -1.5 (khá tệ)
            'rất khó': -1.5, 'quá khó': -1.5, 'rất chán': -1.5,
            
            # Trọng số -1.0 (bình thường)
            'khó hiểu': -1.0, 'khó tiếp thu': -1.0, 'mông lung': -1.0,
            'lan man': -1.0, 'dài dòng': -1.0, 'qua loa': -1.0,
            'chắp vá': -1.0, 'đọc chép': -1.0, 'phụ thuộc slide': -1.0,
            'thiếu linh hoạt': -1.0, 'cứng nhắc': -1.0, 'nhàm chán': -1.0,
            'đơn điệu': -1.0, 'cũ kỹ': -1.0, 'dạy nhanh': -1.0,
            'dạy lố giờ': -1.0, 'thiếu tương tác': -1.0, 'không tương tác': -1.0,
            'thiếu nhiệt tình': -1.0, 'không tâm huyết': -1.0, 'quá rộng': -1.0,
            'quá khó': -1.0, 'không phù hợp': -1.0, 'không sát': -1.0,
            'thiếu cụ thể': -1.0, 'mơ hồ': -1.0, 'chung chung': -1.0,
            'không rõ': -1.0, 'thiếu tài liệu': -1.0, 'không cập nhật': -1.0,
            'nặng': -1.0, 'quá tải': -1.0, 'không công bằng': -1.0,
            'thiếu minh bạch': -1.0, 'bất tiện': -1.0, 'chưa hoàn thiện': -1.0,
            'tệ': -1.0, 'dở': -1.0, 'kém': -1.0, 'chán': -1.0
        }
        
        # ===== TỪ TĂNG CƯỜNG (BOOST WORDS) =====
        self.boost_words = {
            'rất': 2.0, 'quá': 2.0, 'cực': 2.0, 'vô cùng': 2.0,
            'siêu': 2.0, 'hơi bị': 1.5, 'khá': 1.5, 'hơi': 0.8
        }
        
        # ===== TỪ PHỦ ĐỊNH (NEGATION WORDS) =====
        self.negation_words = {'không', 'chẳng', 'chả', 'chưa', 'không hề', 'chẳng hề'}
        
        # ===== PATTERN PHÁT HIỆN "KHÔNG CÓ Ý KIẾN" =====
        self.no_opinion_patterns = [
            r'^không\s*(có)?\s*(gì)?\s*(ý\s*kiến)?\s*(góp\s*ý)?\s*$',
            r'^(em|dạ)\s*(không|ko|k)\s*(có)?\s*(ý\s*kiến)?\s*(ạ)?$',
            r'^(ko|k|0|\.\.+|n/?a)$',
            r'^không\s*có\s*góp\s*ý$',
            r'^$'
        ]
        
        # ===== TAG KEYWORDS (giữ nguyên logic cũ) =====
        self.tag_keywords = {
            'Tag_HocPhan': [
                'chuẩn đầu ra', 'mục tiêu môn học', 'đáp ứng chương trình',
                'nội dung', 'học phần', 'chương trình', 'môn học', 'trang bị',
                'cung cấp', 'đào tạo', 'bám sát', 'phù hợp', 'rõ ràng', 'đầy đủ'
            ],
            'Tag_DayHoc': [
                'giảng viên', 'thầy giáo', 'cô giáo', 'tận tâm', 'nhiệt tình',
                'tận tình', 'truyền cảm hứng', 'dạy', 'giảng', 'nhiệt huyết',
                'dễ hiểu', 'bài giảng', 'sinh động', 'linh hoạt', 'tương tác'
            ],
            'Tag_KiemTra': [
                'kiểm tra', 'đánh giá', 'công bằng', 'minh bạch', 'đánh giá đúng',
                'thi', 'đề thi', 'cho điểm', 'công khai', 'thực lực', 'công tâm'
            ]
        }
        
        self.tag_khac_keywords = [
            'không có góp ý', 'không ý kiến', 'không góp ý'
        ]
        
        # Compile regex cho tags
        self.tag_hp_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_HocPhan'])
        self.tag_dh_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_DayHoc'])
        self.tag_kt_regex = '|'.join(re.escape(w) for w in self.tag_keywords['Tag_KiemTra'])
    
    # ==========================================
    # HÀM KIỂM TRA "KHÔNG CÓ Ý KIẾN"
    # ==========================================
    def is_no_opinion(self, text: str) -> bool:
        """Kiểm tra câu có phải 'không có ý kiến' không"""
        if not isinstance(text, str):
            return True
        
        text_clean = text.lower().strip()
        
        for pattern in self.no_opinion_patterns:
            if re.match(pattern, text_clean):
                return True
        
        # Câu quá ngắn (1-2 ký tự) và không phải từ có nghĩa đặc biệt
        important_short_words = {'tốt', 'hay', 'ok', 'oke', 'ổn', 'vâng', 'dạ'}
        if len(text_clean) <= 2 and text_clean not in important_short_words:
            return True
        
        return False
    
    # ==========================================
    # HÀM TOKENIZE (TÁCH TỪ)
    # ==========================================
    def tokenize(self, text: str) -> list:
        """Tách câu thành từ, xử lý cụm từ đặc biệt"""
        if not isinstance(text, str):
            return []
        
        text = text.lower()
        
        # Xử lý cụm từ đặc biệt trước (ưu tiên cụm dài hơn)
        special_phrases = [
            'không có ý kiến', 'không góp ý', 'không có góp ý',
            'tuyệt vời', 'rất tốt', 'rất hay', 'quá khó', 'rất khó'
        ]
        
        for phrase in special_phrases:
            if phrase in text:
                text = text.replace(phrase, phrase.replace(' ', '_'))
        
        # Tách từ tiếng Việt (bao gồm dấu)
        words = re.findall(r'[a-zàáâãèéêìíòóôõùúýăđĩũơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỵỷỹ]+', text)
        
        return words
    
    # ==========================================
    # HÀM TÍNH ĐIỂM CẢM XÚC CHO 1 CÂU
    # ==========================================
    def calculate_sentiment_score(self, text: str) -> dict:
        """
        Tính điểm cảm xúc theo công thức:
        S = Σ (w_i × boost_i × (-1)^negation_i)
        
        Returns:
            dict: {
                'score': float,      # Tổng điểm (có thể âm hoặc dương)
                'sentiment': str,    # 'positive', 'negative', 'neutral'
            }
        """
        # Bước 1: Kiểm tra "không có ý kiến"
        if self.is_no_opinion(text):
            return {'score': 0.0, 'sentiment': 'neutral'}
        
        # Bước 2: Tokenize
        words = self.tokenize(text)
        
        total_score = 0.0
        negation = False
        boost = 1.0
        
        i = 0
        while i < len(words):
            word = words[i]
            
            # Kiểm tra từ phủ định
            if word in self.negation_words:
                negation = True
                i += 1
                continue
            
            # Kiểm tra từ tăng cường (ảnh hưởng đến từ tiếp theo)
            if word in self.boost_words:
                boost = self.boost_words[word]
                i += 1
                if i >= len(words):
                    break
                word = words[i]
            
            # Tìm điểm cho từ hiện tại
            word_score = 0.0
            
            # Ưu tiên tìm trong từ điển tích cực trước
            found = False
            for pos_word, score in self.positive_words.items():
                if pos_word in word or word in pos_word:
                    word_score = score
                    found = True
                    break
            
            # Nếu không tìm thấy, tìm trong từ điển tiêu cực
            if not found:
                for neg_word, score in self.negative_words.items():
                    if neg_word in word or word in neg_word:
                        word_score = score
                        break
            
            # Áp dụng phủ định và boost
            if word_score != 0:
                if negation:
                    word_score = -word_score
                word_score = word_score * boost
                total_score += word_score
            
            # Reset flags cho từ tiếp theo
            negation = False
            boost = 1.0
            i += 1
        
        # Xác định sentiment dựa trên ngưỡng 0.5 để tránh nhiễu
        if total_score > 0.5:
            sentiment = 'positive'
        elif total_score < -0.5:
            sentiment = 'negative'
        else:
            sentiment = 'neutral'
        
        return {'score': total_score, 'sentiment': sentiment}
    
    # ==========================================
    # HÀM VECTORIZED CHO NHIỀU DÒNG
    # ==========================================
    def analyze_sentiment_vectorized(self, texts):
        """Phân tích cảm xúc cho nhiều dòng cùng lúc"""
        return [self.calculate_sentiment_score(t)['sentiment'] for t in texts]
    
    def calculate_scores_vectorized(self, texts):
        """Tính điểm số cho nhiều dòng cùng lúc"""
        return [self.calculate_sentiment_score(t)['score'] for t in texts]
    
    # ==========================================
    # HÀM EXTRACT TAGS (GIỮ NGUYÊN LOGIC CŨ)
    # ==========================================
    def extract_tags_vectorized(self, texts):
        """Trích xuất tags cho nhiều dòng cùng lúc (giữ nguyên logic cũ)"""
        series = pd.Series(texts)
        
        tag_hp = series.str.contains(self.tag_hp_regex, na=False, regex=True).astype(int)
        tag_dh = series.str.contains(self.tag_dh_regex, na=False, regex=True).astype(int)
        tag_kt = series.str.contains(self.tag_kt_regex, na=False, regex=True).astype(int)
        
        # Tag_Khac: khi không có tag nào khác
        tag_khac = ((tag_hp + tag_dh + tag_kt) == 0).astype(int)
        
        # Câu "không có ý kiến" vẫn được gán Tag_Khac = 1
        for i, text in enumerate(texts):
            if self.is_no_opinion(text):
                tag_khac.iloc[i] = 1
        
        return list(zip(tag_hp, tag_dh, tag_kt, tag_khac))


_nlp = VietnameseNLP()


# ================= BLOB FUNCTIONS =================
def download_blob(blob_service, container, path):
    try:
        container_client = blob_service.get_container_client(container)
        blob = container_client.get_blob_client(path)
        if blob.exists():
            return blob.download_blob().readall().decode('utf-8-sig')
        return ""
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return ""


def save_processed(blob_service, df, filename):
    path = f"{PROCESSED_PATH}/{filename}"
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    try:
        container = blob_service.get_container_client(CONTAINER_NAME)
        blob = container.get_blob_client(path)
        blob.upload_blob(csv_data, overwrite=True)
        print(f"  ✅ Đã lưu: {path}")
        return True
    except Exception as e:
        print(f"  ❌ Lỗi lưu: {e}")
        return False


# ================= LOAD MASTER DATA =================
def load_hp_master(blob_service):
    content = download_blob(blob_service, TAILIEU_CONTAINER, "HP-Khoa.csv")
    if not content:
        return pd.DataFrame()
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 4:
        df = df.iloc[:, 1:4]
        df.columns = ['MaHP', 'TenKhoa', 'TenHP']
    df['MaKhoa'] = df['TenKhoa'].apply(create_ma_khoa)
    return df


def load_chuyennganh_master(blob_service):
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if not content:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean = df_clean[df_clean['MaChuyenNganh'].astype(str).str.strip() != '']
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(extract_ma_nganh_from_ten_nganh)
    df_clean = df_clean.drop_duplicates(subset=['MaChuyenNganh'])
    
    dim_nganh = df_clean[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
    dim_chuyennganh = df_clean[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    
    mapping = {}
    for _, row in df_clean.iterrows():
        ma_chuyen = row['MaChuyenNganh']
        if ma_chuyen and ma_chuyen not in mapping:
            mapping[ma_chuyen] = {
                'TenChuyenNganh': row['TenChuyenNganh'],
                'MaNganh': row['MaNganh'],
                'TenNganh': row['TenNganh'],
                'MaKhoa': row['MaKhoa'],
                'TenKhoa': row['TenKhoa']
            }
    return dim_nganh, dim_chuyennganh, mapping


# ================= PARSE SURVEY DATA - DẠNG DỌC =================
def is_date_format(value):
    return bool(_date_pattern.match(value.strip())) if isinstance(value, str) else False


def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH"


def parse_lines_batch(lines_batch):
    results = []
    for line in lines_batch:
        if not line or not line.strip():
            continue
        row = [x.strip() for x in line.split(',')]
        row_len = len(row)
        if row_len < 15:
            continue
        try:
            lop = row[0]
            ma_sv = row[1]
            ngay_sinh = ''
            ngay_sinh_index = -1
            for i in range(2, min(row_len, 12)):
                if is_date_format(row[i]):
                    ngay_sinh = row[i]
                    ngay_sinh_index = i
                    break
            if ngay_sinh_index == -1:
                continue
            
            ho_dem = ''
            ten = ''
            if ngay_sinh_index > 1:
                name_parts = [p for p in row[2:ngay_sinh_index] if p]
                if name_parts:
                    ten = name_parts[-1]
                    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else ''
            ma_gv = ''
            ma_gv_index = -1
            
            for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
                if is_ma_gv_format(row[i]):
                    ma_gv = row[i]
                    ma_gv_index = i
                    break
            
            if ma_gv_index == -1:
                ma_gv_index = row_len - 4 if row_len >= 4 else ngay_sinh_index + 2
            
            ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]) if ma_gv_index > ngay_sinh_index + 2 else ''
            ho_dem_gv = row[ma_gv_index + 1] if ma_gv_index + 1 < row_len else ''
            ten_gv = row[ma_gv_index + 2] if ma_gv_index + 2 < row_len else ''
            lop_hp = row[ma_gv_index + 3] if ma_gv_index + 3 < row_len else ''
            cau_hoi = row[ma_gv_index + 4] if ma_gv_index + 4 < row_len else ''
            gia_tri = row[ma_gv_index + 5] if ma_gv_index + 5 < row_len else ''
            
            null_index = -1
            for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
                if row[i].upper() == 'NULL' or row[i] == '':
                    null_index = i
                    break
            
            essay_text = ''
            if null_index != -1 and null_index + 1 < row_len:
                after_null = row[null_index + 1:]
                essay_text = ','.join(after_null).strip()
            
            # Tạo SubmissionID
            submission_id = f"{ma_sv}_{lop_hp}_{ma_gv}_{FILE_NAME}"
            
            results.append({
                'SubmissionID': submission_id,
                'Lop': lop,
                'MaSV': ma_sv,
                'HoDem': ho_dem,
                'Ten': ten,
                'NgaySinh': ngay_sinh,
                'MaHP': ma_hp,
                'TenHP': ten_hp,
                'MaGV': ma_gv,
                'HoDemGV': ho_dem_gv,
                'TenGV': ten_gv,
                'LopHP': lop_hp,
                'CauHoi': cau_hoi,
                'GiaTri': gia_tri,
                'EssayText': essay_text
            })
        except Exception:
            continue
    return results


def parse_survey_to_long_format(content: str) -> pd.DataFrame:
    """Parse CSV trực tiếp thành dạng dọc"""
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    all_rows = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch, batch) for batch in batches]
        for future in as_completed(futures):
            all_rows.extend(future.result())
    
    df = pd.DataFrame(all_rows)
    print(f"  -> Đã parse {len(df):,} dòng câu trả lời ({time.time()-start:.2f}s)")
    return df


# ================= TRANSFORM & NLP - ĐÃ CẬP NHẬT =================
def transform_with_nlp_long_format(df_raw: pd.DataFrame) -> tuple:
    """
    Transform dữ liệu với NLP đã sửa logic toán học
    """
    print("  -> Transform dữ liệu...")
    start = time.time()
    
    # ===== 1. XỬ LÝ CÂU TỰ LUẬN - LOẠI BỎ TRÙNG =====
    text_df = df_raw[df_raw['EssayText'].notna() & (df_raw['EssayText'] != '')].copy()
    
    if text_df.empty:
        print("  ⚠️ Không có dữ liệu tự luận!")
        fact_main = pd.DataFrame()
    else:
        print(f"  -> Dòng tự luận thô: {len(text_df):,}")
        
        # LOẠI BỎ TRÙNG LẶP THEO SubmissionID
        text_df_unique = text_df.drop_duplicates(subset=['SubmissionID'], keep='first')
        print(f"  -> Sau loại bỏ trùng: {len(text_df_unique):,} submissions")
        
        # Kiểm tra conflict
        conflicts = text_df.groupby('SubmissionID')['EssayText'].nunique()
        conflicts = conflicts[conflicts > 1]
        if not conflicts.empty:
            print(f"  ⚠️ Cảnh báo: {len(conflicts)} submissions có nhiều EssayText khác nhau")
        
        # Chuẩn bị dữ liệu
        text_df_unique['NoiDungGopY'] = text_df_unique['EssayText'].str.replace(r'\s+', ' ', regex=True).str.strip()
        
        # Vectorized NLP
        texts = text_df_unique['NoiDungGopY'].tolist()
        print(f"  -> Đang xử lý NLP cho {len(texts):,} bài tự luận...")
        
        # ✅ Phân tích cảm xúc - chỉ lấy sentiment, không lưu score
        text_df_unique['Sentiment'] = _nlp.analyze_sentiment_vectorized(texts)
        
        # ✅ Tính score để thống kê nhưng không đưa vào database
        sentiment_scores = _nlp.calculate_scores_vectorized(texts)
        
        # Extract tags
        tag_vectors = _nlp.extract_tags_vectorized(texts)
        
        text_df_unique['Tag_HocPhan'] = [v[0] for v in tag_vectors]
        text_df_unique['Tag_DayHoc'] = [v[1] for v in tag_vectors]
        text_df_unique['Tag_KiemTra'] = [v[2] for v in tag_vectors]
        text_df_unique['Tag_Khac'] = [v[3] for v in tag_vectors]
        text_df_unique['Is_Valid'] = 1
        
        # ✅ CHỈ LẤY CÁC CỘT CẦN THIẾT CHO DATABASE (KHÔNG CÓ SentimentScore)
        fact_main = text_df_unique[[
            'SubmissionID', 'MaSV', 'LopHP', 'NoiDungGopY',
            'Sentiment', 'Is_Valid',
            'Tag_HocPhan', 'Tag_DayHoc', 'Tag_KiemTra', 'Tag_Khac'
        ]].copy()
        
        # ✅ Lưu sentiment scores riêng để thống kê (không insert vào DB)
        fact_main['SentimentScore'] = sentiment_scores  # Chỉ để thống kê, sẽ bỏ sau
        
        duplicates_removed = len(text_df) - len(text_df_unique)
        if duplicates_removed > 0:
            print(f"  ✅ Đã loại bỏ {duplicates_removed:,} dòng trùng lặp EssayText")
    
    # ===== 2. XỬ LÝ CÂU TRẮC NGHIỆM =====
    mcq_df = df_raw[
        df_raw['CauHoi'].notna() & (df_raw['CauHoi'] != '') &
        df_raw['GiaTri'].notna() & (df_raw['GiaTri'] != '')
    ].copy()
    
    if not mcq_df.empty:
        mcq_df['MaCauHoi'] = mcq_df['CauHoi'].astype(int)
        mcq_df['Diem'] = mcq_df['GiaTri'].astype(int)
        fact_ketqua = mcq_df[['SubmissionID', 'MaCauHoi', 'Diem']].copy()
        print(f"  -> FACT_KET_QUA_DANH_GIA: {len(fact_ketqua):,} dòng")
    else:
        fact_ketqua = pd.DataFrame()
    
    print(f"  ✅ Transform xong ({time.time()-start:.2f}s)")
    return fact_main, fact_ketqua


# ================= LOAD DATABASE OPTIMIZED =================
def batch_insert_optimized(cursor, table, columns, data, batch_size=100000):
    if not data:
        return 0
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(sql, batch)
        total += len(batch)
        if total % 100000 == 0:
            print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
    cursor.connection.commit()
    return total


def load_dimensions_optimized(cursor, df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping):
    print("\n📥 Loading DIMENSION tables...")
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    
    # ==========================================
    # BẢNG 1: DIM_CHUONG_TRINH_DAO_TAO
    # ==========================================
    print("\n  -> 1. DIM_CHUONG_TRINH_DAO_TAO")
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY') 
        INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
    """)
    cursor.execute("SELECT COUNT(*) FROM DIM_CHUONG_TRINH_DAO_TAO")
    count = cursor.fetchone()[0]
    print(f"     ✅ {count} dòng")
    
    # ==========================================
    # BẢNG 2: DIM_KHOA
    # ==========================================
    print("\n  -> 2. DIM_KHOA")
    all_khoa = set()
    if not hp_master.empty:
        all_khoa.update(hp_master['MaKhoa'].unique())
        print(f"     - Từ HP-Khoa.csv: {len(hp_master['MaKhoa'].unique())} khoa")
    if not dim_nganh.empty:
        all_khoa.update(dim_nganh['MaKhoa'].unique())
        print(f"     - Từ TenChuyenNganh-Khoa.csv: {len(dim_nganh['MaKhoa'].unique())} khoa")
    
    default_khoa = {'TĐHKT': 'Trường ĐH Kinh tế', 'PĐT': 'Phòng Đào Tạo'}
    all_khoa.update(default_khoa.keys())
    print(f"     - Giá trị mặc định: {len(default_khoa)} khoa")
    
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing_khoa = {row[0] for row in cursor.fetchall()}
    data_khoa = [(ma, default_khoa.get(ma, ma)) for ma in all_khoa if ma not in existing_khoa]
    if data_khoa:
        batch_insert_optimized(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], data_khoa, 10000)
        print(f"     ✅ Đã insert {len(data_khoa)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    # ==========================================
    # BẢNG 3: DIM_NGANH
    # ==========================================
    print("\n  -> 3. DIM_NGANH")
    
    # ✅ THÊM QT VÀ CTS VÀO DIM_NGANH (THÊM ĐOẠN NÀY)
    default_nganh = [
        ('QT', 'Ngành Quản trị', 'PĐT'),
        ('CTS', 'Ngành Công nghệ thông tin', 'TĐHKT')
    ]
    
    for ma_nganh, ten_nganh, ma_khoa in default_nganh:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH WHERE MaNganh = ?", ma_nganh)
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) 
                VALUES (?, ?, ?)
            """, ma_nganh, ten_nganh, ma_khoa)
            print(f"     ✅ Đã thêm {ma_nganh} vào DIM_NGANH")
    
    if not dim_chuyennganh.empty:
        # Lấy tất cả MaNganh cần có
        all_ma_nganh = set(dim_chuyennganh['MaNganh'].dropna().unique())
        print(f"     - Cần có {len(all_ma_nganh)} MaNganh: {sorted(all_ma_nganh)}")
        
        # Lấy existing
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing_nganh = {row[0] for row in cursor.fetchall()}
        
        # Tìm MaNganh thiếu
        missing_nganh = all_ma_nganh - existing_nganh
        print(f"     - Thiếu {len(missing_nganh)} MaNganh: {sorted(missing_nganh)}")
        
        # Insert các MaNganh thiếu
        if missing_nganh:
            for ma_nganh in missing_nganh:
                # Lấy thông tin từ dim_chuyennganh
                sample = dim_chuyennganh[dim_chuyennganh['MaNganh'] == ma_nganh].iloc[0]
                ten_nganh = sample.get('TenNganh', f'Ngành {ma_nganh}')
                cursor.execute("""
                    INSERT INTO DIM_NGANH (MaNganh, TenNganh, MaKhoa) 
                    VALUES (?, ?, ?)
                """, ma_nganh, ten_nganh, 'TĐHKT')
                print(f"        ✅ Đã insert {ma_nganh} vào DIM_NGANH")
            
            # ✅ COMMIT NGAY SAU KHI INSERT
            cursor.connection.commit()
            print(f"     ✅ Đã commit {len(missing_nganh)} dòng vào DIM_NGANH")
    
    # ==========================================
    # BẢNG 4: DIM_CHUYEN_NGANH (INSERT SAU KHI ĐÃ COMMIT)
    # ==========================================
    print("\n  -> 4. DIM_CHUYEN_NGANH")
    
    # ✅ CẬP NHẬT QT VÀ CTS (THÊM ĐOẠN NÀY)
    # Cập nhật MaNganh cho chuyên ngành QT
    cursor.execute("""
        UPDATE DIM_CHUYEN_NGANH 
        SET MaNganh = 'QT', MaKhoa = 'PĐT'
        WHERE MaChuyenNganh = 'QT'
    """)
    
    # Cập nhật MaNganh cho chuyên ngành CTS
    cursor.execute("""
        UPDATE DIM_CHUYEN_NGANH 
        SET MaNganh = 'CTS', MaKhoa = 'TĐHKT'
        WHERE MaChuyenNganh = 'CTS'
    """)
    cursor.connection.commit()
    print(f"     ✅ Đã cập nhật QT và CTS trong DIM_CHUYEN_NGANH")
    
    # Kiểm tra lại DIM_NGANH sau commit
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    valid_nganh = {row[0] for row in cursor.fetchall()}
    print(f"     - Hiện có {len(valid_nganh)} ngành trong DIM_NGANH")
    
    if not dim_chuyennganh.empty:
        cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
        existing_cn = {row[0] for row in cursor.fetchall()}
        
        data_cn = []
        for _, row in dim_chuyennganh.iterrows():
            ma_chuyen = row['MaChuyenNganh']
            ma_nganh = row['MaNganh']
            
            if ma_chuyen and ma_chuyen not in existing_cn:
                # Kiểm tra ma_nganh có trong DIM_NGANH không
                if ma_nganh not in valid_nganh:
                    print(f"        ❌ LỖI: {ma_chuyen} có MaNganh={ma_nganh} không tồn tại trong DIM_NGANH!")
                    continue
                data_cn.append((ma_chuyen, row['TenChuyenNganh'], ma_nganh, 'CTDT_CHINHQUY'))
                existing_cn.add(ma_chuyen)
        
        if data_cn:
            batch_insert_optimized(cursor, 'DIM_CHUYEN_NGANH', 
                                  ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh', 'MaCTDT'], data_cn, 5000)
            print(f"     ✅ Đã insert {len(data_cn)} dòng mới từ master")
        else:
            print(f"     ✅ Không có dòng mới từ master")
            
    # ==========================================
    # BẢNG 4: DIM_HOC_PHAN
    # ==========================================
    print("\n  -> 4. DIM_HOC_PHAN")
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing_hp = {row[0] for row in cursor.fetchall()}
    
    hp_dict = {}
    if not hp_master.empty:
        hp_master_unique = hp_master.drop_duplicates(subset=['MaHP'], keep='first')
        hp_dict = hp_master_unique.set_index('MaHP')[['TenHP', 'MaKhoa']].to_dict('index')
    
    df_hp_raw = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP').dropna(subset=['MaHP'])
    data_hp = []
    for _, row in df_hp_raw.iterrows():
        ma_hp = row['MaHP']
        if ma_hp not in existing_hp:
            if ma_hp in hp_dict:
                data_hp.append((ma_hp, hp_dict[ma_hp]['TenHP'], hp_dict[ma_hp]['MaKhoa']))
            else:
                ten_hp = row['TenHP'] if pd.notna(row['TenHP']) else f"Học phần {ma_hp}"
                data_hp.append((ma_hp, ten_hp, 'TĐHKT'))
            existing_hp.add(ma_hp)
    
    if data_hp:
        batch_insert_optimized(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], data_hp, 5000)
        print(f"     ✅ Đã insert {len(data_hp)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")

    
    # ==========================================
    # BẢNG 6: DIM_GIANG_VIEN
    # ==========================================
    print("\n  -> 6. DIM_GIANG_VIEN")
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    existing_gv = {row[0] for row in cursor.fetchall()}
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV').dropna(subset=['MaGV'])
    data_gv = [(row['MaGV'], row['HoDemGV'] or '', row['TenGV'] or '') for _, row in df_gv.iterrows() if row['MaGV'] not in existing_gv]
    if data_gv:
        batch_insert_optimized(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], data_gv, 50000)
        print(f"     ✅ Đã insert {len(data_gv)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # BẢNG 7: DIM_HOC_KY
    # ==========================================
    print("\n  -> 7. DIM_HOC_KY")
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", ma_hoc_ky, nam_hoc, hoc_ky)
        print(f"     ✅ Đã thêm {ma_hoc_ky}")
    else:
        print(f"     ✅ {ma_hoc_ky} đã tồn tại")
    
    # ==========================================
    # BẢNG 8: DIM_LOP_SINH_VIEN (DEBUG)
    # ==========================================
    print("\n  -> 8. DIM_LOP_SINH_VIEN")
    
    # Lấy danh sách MaChuyenNganh và MaNganh từ DIM_CHUYEN_NGANH
    cursor.execute("SELECT MaChuyenNganh, MaNganh FROM DIM_CHUYEN_NGANH")
    all_chuyennganh = {}
    for row in cursor.fetchall():
        all_chuyennganh[row[0]] = row[1]
    print(f"     - Tổng số chuyên ngành trong DIM_CHUYEN_NGANH: {len(all_chuyennganh)}")
    
    # Lấy danh sách MaNganh hợp lệ
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    valid_nganh = {row[0] for row in cursor.fetchall()}
    print(f"     - Các MaNganh hợp lệ trong DIM_NGANH: {sorted(valid_nganh)}")
    
    # Kiểm tra những MaChuyenNganh nào có MaNganh không hợp lệ
    invalid_chuyennganh = []
    for ma_cn, ma_nganh in all_chuyennganh.items():
        if ma_nganh not in valid_nganh:
            invalid_chuyennganh.append((ma_cn, ma_nganh))
    
    if invalid_chuyennganh:
        print(f"     ⚠️ CẢNH BÁO: Có {len(invalid_chuyennganh)} chuyên ngành có MaNganh không hợp lệ:")
        for ma_cn, ma_nganh in invalid_chuyennganh[:10]:
            print(f"        - {ma_cn} -> MaNganh={ma_nganh} (KHÔNG TỒN TẠI trong DIM_NGANH)")
    
    # Chỉ lấy những MaChuyenNganh có MaNganh hợp lệ
    valid_chuyennganh = {ma_cn for ma_cn, ma_nganh in all_chuyennganh.items() if ma_nganh in valid_nganh}
    print(f"     - Chỉ có {len(valid_chuyennganh)} chuyên ngành có MaNganh hợp lệ")
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    df_lop_unique = df_raw[['Lop']].drop_duplicates('Lop').dropna()
    print(f"     - Số lớp unique từ dữ liệu: {len(df_lop_unique)}")
    
    data_lop = []
    skipped_lop = []
    
    for _, row in df_lop_unique.iterrows():
        lop = row['Lop']
        if lop not in existing_lop:
            ma_cn, ten_cn, ten_khoa, ma_khoa = determine_ma_chuyen_nganh(lop)
            
            if not ma_cn:
                skipped_lop.append({'lop': lop, 'lydo': 'Không xác định được ma_cn', 'ma_cn': None})
                continue
            
            # Kiểm tra ma_cn có trong danh sách hợp lệ không
            if ma_cn not in valid_chuyennganh:
                skipped_lop.append({'lop': lop, 'lydo': f'MaChuyenNganh={ma_cn} không có MaNganh hợp lệ', 'ma_cn': ma_cn})
                continue
            
            data_lop.append((lop, lop, ma_cn))
            existing_lop.add(lop)
    
    # IN CHI TIẾT LỚP BỊ BỎ QUA
    if skipped_lop:
        print(f"     ⚠️ Bỏ qua {len(skipped_lop)} lớp:")
        for i, item in enumerate(skipped_lop[:20], 1):
            print(f"        {i}. Lớp: '{item['lop']}'")
            print(f"           Lý do: {item['lydo']}")
            if item['ma_cn']:
                print(f"           MaChuyenNganh: {item['ma_cn']}")
        if len(skipped_lop) > 20:
            print(f"        ... và {len(skipped_lop) - 20} lớp khác")
    
    # IN CHI TIẾT LỚP SẼ INSERT
    if data_lop:
        print(f"     - Sẽ insert {len(data_lop)} lớp:")
        for i, (lop, _, ma_cn) in enumerate(data_lop[:10], 1):
            print(f"        {i}. Lớp: '{lop}' -> MaChuyenNganh: {ma_cn}")
        if len(data_lop) > 10:
            print(f"        ... và {len(data_lop) - 10} lớp khác")
    
    if data_lop:
        batch_insert_optimized(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], data_lop, 5000)
        print(f"     ✅ Đã insert {len(data_lop)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # BẢNG 9: DIM_SINH_VIEN (CÓ LOG)
    # ==========================================
    print("\n  -> 9. DIM_SINH_VIEN")
    
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    valid_lop = {row[0] for row in cursor.fetchall()}
    print(f"     - Có {len(valid_lop)} lớp hợp lệ trong DIM_LOP_SINH_VIEN")
    
    cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
    existing_sv = {row[0] for row in cursor.fetchall()}
    
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV').dropna(subset=['MaSV'])
    
    data_sv = []
    skipped_sv_details = []
    
    for _, row in df_sv.iterrows():
        ma_sv = row['MaSV']
        lop = row['Lop']
        
        if ma_sv not in existing_sv:
            if lop not in valid_lop:
                skipped_sv_details.append({'MaSV': ma_sv, 'Lop': lop, 'Ten': row['Ten']})
                continue
            
            ngay_sinh = None
            if row['NgaySinh']:
                try:
                    ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
                except:
                    pass
            
            data_sv.append((ma_sv, row['HoDem'] or '', row['Ten'] or '', ngay_sinh, lop))
            existing_sv.add(ma_sv)
    
    if skipped_sv_details:
        print(f"     ⚠️ Bỏ qua {len(skipped_sv_details)} sinh viên:")
        for i, detail in enumerate(skipped_sv_details[:20], 1):
            print(f"        {i}. MaSV: '{detail['MaSV']}' - Lớp: '{detail['Lop']}' - Tên: {detail['Ten']}")
        if len(skipped_sv_details) > 20:
            print(f"        ... và {len(skipped_sv_details) - 20} sinh viên khác")
    
    if data_sv:
        batch_insert_optimized(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], data_sv, 5000)
        print(f"     ✅ Đã insert {len(data_sv)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    # ==========================================
    # BẢNG 10: DIM_LOP_HOC_PHAN
    # ==========================================
    print("\n  -> 10. DIM_LOP_HOC_PHAN")
    
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    valid_hp = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
    valid_gv = {row[0] for row in cursor.fetchall()}
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY")
    valid_hocky = {row[0] for row in cursor.fetchall()}
    
    print(f"     - Có {len(valid_hp)} học phần hợp lệ")
    print(f"     - Có {len(valid_gv)} giảng viên hợp lệ")
    print(f"     - Có {len(valid_hocky)} học kỳ hợp lệ")
    
    cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
    existing_lhp = {row[0] for row in cursor.fetchall()}
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP').dropna(subset=['LopHP'])
    
    data_lhp = []
    for _, row in df_lhp.iterrows():
        lop_hp = row['LopHP']
        if lop_hp not in existing_lhp:
            if row['MaHP'] not in valid_hp or row['MaGV'] not in valid_gv or ma_hoc_ky not in valid_hocky:
                continue
            data_lhp.append((lop_hp, lop_hp, row['MaHP'], row['MaGV'], ma_hoc_ky))
            existing_lhp.add(lop_hp)
    
    if data_lhp:
        batch_insert_optimized(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], data_lhp, 5000)
        print(f"     ✅ Đã insert {len(data_lhp)} dòng mới")
    else:
        print(f"     ✅ Không có dòng mới")
    
    cursor.connection.commit()
    print("  ✅ All DIMENSION tables loaded!")


def load_fact_tables_optimized(cursor, fact_main, fact_ketqua):
    print("\n📥 Loading FACT tables...")
    start_time = time.time()
    
    # TẮT CONSTRAINTS
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN NOCHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA NOCHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    count_main = 0
    count_kq = 0
    
    try:
        # ✅ BẮT ĐẦU TRANSACTION
        cursor.execute("BEGIN TRANSACTION")
        
        # FACT_GOP_Y_TU_LUAN
        if not fact_main.empty:
            data_main = []
            for _, row in fact_main.iterrows():
                noi_dung = row['NoiDungGopY']
                if isinstance(noi_dung, str) and len(noi_dung) > 4000:
                    noi_dung = noi_dung[:4000]
                data_main.append((
                    row['SubmissionID'], row['MaSV'], row['LopHP'], noi_dung,
                    row['Sentiment'], row['Is_Valid'],
                    row['Tag_HocPhan'], row['Tag_DayHoc'], row['Tag_KiemTra'], row['Tag_Khac']
                ))
            
            placeholders = ', '.join(['?' for _ in range(10)])
            sql = f"INSERT INTO FACT_GOP_Y_TU_LUAN (SubmissionID, MaSV, MaLopHP, NoiDungGopY, Sentiment, Is_Valid, Tag_HocPhan, Tag_DayHoc, Tag_KiemTra, Tag_Khac) VALUES ({placeholders})"
            
            batch_size = 50000
            for i in range(0, len(data_main), batch_size):
                batch = data_main[i:i+batch_size]
                cursor.executemany(sql, batch)
                count_main += len(batch)
                print(f"      -> Đã insert {count_main:,}/{len(data_main):,} dòng vào FACT_GOP_Y_TU_LUAN")
        
        # FACT_KET_QUA_DANH_GIA
        if not fact_ketqua.empty:
            data_kq = [tuple(row) for row in fact_ketqua[['SubmissionID', 'MaCauHoi', 'Diem']].to_numpy()]
            sql2 = "INSERT INTO FACT_KET_QUA_DANH_GIA (SubmissionID, MaCauHoi, Diem) VALUES (?, ?, ?)"
            
            batch_size = 100000
            for i in range(0, len(data_kq), batch_size):
                batch = data_kq[i:i+batch_size]
                cursor.executemany(sql2, batch)
                count_kq += len(batch)
                print(f"      -> Đã insert {count_kq:,}/{len(data_kq):,} dòng vào FACT_KET_QUA_DANH_GIA")
        
        # ✅ CHỈ COMMIT 1 LẦN DUY NHẤT
        cursor.execute("COMMIT")
        
    except Exception as e:
        cursor.execute("ROLLBACK")
        print(f"  ❌ Lỗi: {e}")
        raise
    
    # BẬT LẠI CONSTRAINTS
    cursor.execute("ALTER TABLE FACT_GOP_Y_TU_LUAN CHECK CONSTRAINT ALL")
    cursor.execute("ALTER TABLE FACT_KET_QUA_DANH_GIA CHECK CONSTRAINT ALL")
    cursor.connection.commit()
    
    elapsed = time.time() - start_time
    print(f"  ✅ FACT tables loaded: {count_main:,} submissions, {count_kq:,} answers in {elapsed:.1f}s")
    return count_main, count_kq

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - OPTIMIZED (LOẠI BỎ TRÙNG LẶP)")
    print("=" * 60)
    print(f"SEMESTER: {SEMESTER}")
    print(f"SURVEY_FILE: {SURVEY_FILE}")
    print(f"WORKERS: {NUM_WORKERS}")
    print("=" * 60)
    
    # 1. Kết nối Azure
    print("\n📥 1. Kết nối Azure...")
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        print("  ✅ Thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    # 2. Đọc dữ liệu master
    print("\n📥 2. Đọc dữ liệu master...")
    hp_master = load_hp_master(blob_service)
    dim_nganh, dim_chuyennganh, mapping = load_chuyennganh_master(blob_service)
    print(f"  ✅ HP-Khoa: {len(hp_master)} dòng")
    print(f"  ✅ DIM_NGANH: {len(dim_nganh)} dòng")
    print(f"  ✅ DIM_CHUYEN_NGANH: {len(dim_chuyennganh)} dòng")
    
    # 3. Đọc dữ liệu survey
    print(f"\n📥 3. Đọc dữ liệu survey...")
    survey_path = f"{RAWDATA_PATH}/{SURVEY_FILE}"
    survey_content = download_blob(blob_service, CONTAINER_NAME, survey_path)
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    # 4. Parse dữ liệu
    print("\n📝 4. Parse dữ liệu...")
    parse_start = time.time()
    df_raw = parse_survey_to_long_format(survey_content)
    parse_time = time.time() - parse_start
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    print(f"  ✅ Parse: {len(df_raw):,} dòng câu trả lời trong {parse_time:.1f}s")
    
    # 5. Transform & NLP
    print("\n🔄 5. Transform & NLP...")
    transform_start = time.time()
    fact_main, fact_ketqua = transform_with_nlp_long_format(df_raw)
    transform_time = time.time() - transform_start
    print(f"  ✅ Transform: {transform_time:.1f}s")
    
    # 6. Lưu backup
    print("\n💾 6. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    if not fact_main.empty:
        save_processed(blob_service, fact_main, f"{FILE_NAME}_main_{timestamp}.csv")
    if not fact_ketqua.empty:
        save_processed(blob_service, fact_ketqua, f"{FILE_NAME}_ketqua_{timestamp}.csv")
    
    # 7. Kết nối Database - SỬA LỖI Ở ĐÂY
    print("\n💾 7. Kết nối SQL Database...")
    try:
        # Tạo connection trước
        conn = pyodbc.connect(CONN_STR, autocommit=False)
        
        # Tạo cursor và set fast_executemany cho CURSOR
        cursor = conn.cursor()
        cursor.fast_executemany = True  # ✅ ĐÚNG - gán cho cursor
        
        print("  ✅ Kết nối SQL thành công")
    except Exception as e:
        print(f"  ❌ Lỗi kết nối SQL: {e}")
        return
    
    # 8. Load to database
    db_start = time.time()
    count_main = 0
    count_kq = 0
    try:
        load_dimensions_optimized(cursor, df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping)
        count_main, count_kq = load_fact_tables_optimized(cursor, fact_main, fact_ketqua)
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        conn.close()
    
    db_time = time.time() - db_start
    
    # 9. Thống kê
    total_time = time.time() - total_start
    print("\n📊 9. KẾT QUẢ:")
    print(f"   - Dòng dữ liệu thô: {len(df_raw):,}")
    print(f"   - Số phiếu tự luận: {len(fact_main):,}")
    print(f"   - Số câu trắc nghiệm: {count_kq:,}")
    
    if not fact_main.empty:
        print("\n   - Tag phân bố:")
        print(f"      Tag_HocPhan: {fact_main['Tag_HocPhan'].sum():,}")
        print(f"      Tag_DayHoc: {fact_main['Tag_DayHoc'].sum():,}")
        print(f"      Tag_KiemTra: {fact_main['Tag_KiemTra'].sum():,}")
        print(f"      Tag_Khac: {fact_main['Tag_Khac'].sum():,}")
        
        print("\n   - Sentiment phân bố:")
        for sent, cnt in fact_main['Sentiment'].value_counts().items():
            pct = cnt/len(fact_main)*100
            print(f"      {sent}: {cnt:,} ({pct:.1f}%)")
        
        # Thống kê SentimentScore (nếu có cột)
        if 'SentimentScore' in fact_main.columns:
            print("\n   - SentimentScore thống kê:")
            print(f"      Min: {fact_main['SentimentScore'].min():.2f}")
            print(f"      Max: {fact_main['SentimentScore'].max():.2f}")
            print(f"      Mean: {fact_main['SentimentScore'].mean():.2f}")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH! Thời gian: {total_time:.1f}s")
    print(f"   - Parse: {parse_time:.1f}s")
    print(f"   - Transform: {transform_time:.1f}s")
    print(f"   - Database: {db_time:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
