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
NUM_WORKERS = max(2, (mp.cpu_count() - 1) // 2)
CHUNK_SIZE = 10000 

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
    """Trích xuất Mã Ngành từ Tên Ngành (lấy các chữ cái đầu viết hoa)"""
    if not isinstance(ten_nganh, str) or not ten_nganh:
        return "UNKNOWN"
    words = re.split(r'[\s\-]+', ten_nganh.strip())
    initials = []
    for w in words:
        if w:
            first_char = w[0].upper() if w[0].isalpha() else ''
            if first_char:
                initials.append(first_char)
    return ''.join(initials) if initials else "UNKNOWN"

def determine_ma_chuyen_nganh(lop: str) -> tuple:
    lop_upper = lop.upper()
    lop_normalized = normalize_lop(lop)
    
    if _lop_pattern.match(lop_normalized):
        ma_cn = f"K{lop_normalized[3:5]}"
        return ma_cn, f"Chuyên ngành {ma_cn}", None, None
    if 'QT' in lop_upper:
        return "QT", "Chuyên ngành QT", "Phòng Đào Tạo", "PĐT"
    if 'CTS' in lop_upper or lop_upper.startswith('CTS-') or lop_upper.startswith('CTS'):
        return "CTS", "Chuyên ngành CTS", "Trường ĐHKT", "TĐHKT"
    return None, None, None, None

# ================= RULE-BASED NLP (CẬP NHẬT TỪ ĐIỂN) =================
class VietnameseNLPRuleBased:
    __slots__ = ['positive_set', 'negative_set', 'negations', 'intensifiers', 'tag_keywords']
    
    def __init__(self):
        # ===== POSITIVE WORDS =====
        positive_words = {
            # Cảm thán & Yêu thích
            'tuyệt vời', 'tuyệt vờiii', 'quá tuyệt vời', 'tuyệtt vời', 'tuyệt',
            'mãi yêu', 'yêu cô', 'yêu thầy', 'siêu thích', 'siêu dễ thương',
            'hào hứng', 'thoải mái', 'vui', 'vui vẻ', 'vui nhộn', 'vui tươi',
            'sôi nổi', 'sôi động', 'năng động', 'hoạt bát', 'hấp dẫn', 'thu hút',
            
            # Tính cách & Phong cách giảng viên
            'dễ mến', 'dễ gần', 'thân thiện', 'gần gũi', 'tâm lý', 'thấu hiểu',
            'dễ thương', 'đẹp trai', 'đẹp gái', 'vui tính', 'hài hước',
            'có tâm', 'tâm huyết', 'tận tâm', 'tận tụy', 'tận tình', 'nhiệt huyết',
            'chu đáo', 'kĩ', 'kỹ', 'cẩn thận', 'chi tiết', 'sâu sắc', 'sâu',
            'nghiêm túc', 'khắt khe', 'linh hoạt', 'sáng tạo', 'sáng tạp', 'mới mẻ',
            
            # Chất lượng nội dung & Phương pháp
            'thực tế', 'thực tiễn', 'thực tiến', 'sát ngành', 'sát chương trình',
            'bám sát', 'đúng trọng tâm', 'trọng tâm', 'hiệu quả', 'tiến bộ',
            'mở mang tầm mắt', 'tiếp thu nhanh', 'nắm rõ kiến thức',
            'có hoạt động nhóm', 'có bài tập nhóm', 'có thuyết trình', 'có mini game',
            'đa dạng', 'phong phú', 'hợp lý', 'hợp lí', 'chuẩn', 'chuẩn mực',
            'tạo điều kiện', 'hỗ trợ', 'giải đáp thắc mắc', 'chỉnh chu',
            
            # Từ viết tắt & Tiếng Anh
            'ok', 'good', 'tot', 'vuiiii', 'inspiring', 'dedicated', 'clear voice',
            'none'
        }
        
        # ===== NEGATIVE WORDS =====
        negative_words = {
            # Phương pháp giảng dạy kém
            'khó hiểu', 'khó tiếp thu', 'mông lung', 'lan man', 'dài dòng',
            'qua loa', 'chắp vá', 'đọc chép', 'đọc theo slide', 'phụ thuộc slide',
            'thiếu linh hoạt', 'không linh hoạt', 'cứng nhắc', 'nhàm chán',
            'đơn điệu', 'cũ kỹ', 'dạy nhanh', 'dạy lố giờ', 'thiếu tương tác',
            'không tương tác', 'thiếu nhiệt tình', 'không tâm huyết',
            
            # Nội dung & Tài liệu
            'quá rộng', 'quá khó', 'không phù hợp', 'không sát', 'thiếu cụ thể',
            'mơ hồ', 'chung chung', 'không rõ', 'thiếu tài liệu', 'hạn chế',
            'không cập nhật', 'nặng', 'quá tải',
            
            # Đánh giá & Cơ sở vật chất
            'không công bằng', 'thiếu minh bạch', 'bất tiện', 'chưa hoàn thiện',
            'tệ', 'dở', 'kém', 'chán', 'thất vọng', 'không hài lòng',
            'không có ích', 'mất thời gian'
        }
        
        # ===== NEGATIONS =====
        negations = {
            'không', 'chẳng', 'chả', 'đâu có', 'chưa', 'chẳng hề',
            'ko', 'k', 'khộng', 'khoong', 'k phải', 'đâu'
        }
        
        # ===== INTENSIFIERS =====
        intensifiers = {
            'rất', 'quá', 'cực kỳ', 'vô cùng', 'hơi bị', 'siêu', 'cực',
            'lắm', 'quá là', 'cực kì', 'hơi', 'khá', 'nhất'
        }
        
        self.positive_set = positive_words
        self.negative_set = negative_words
        self.negations = negations
        self.intensifiers = intensifiers
        
        # ===== TAG KEYWORDS ====
        self.tag_keywords = {
            'TAG_HP': {
                # Học phần
                'chuẩn đầu ra', 'nội dung', 'mục tiêu', 'chương trình', 'học phần',
                'giáo trình', 'tài liệu', 'khối lượng', 'kiến thức', 'tin chỉ',
                'môn học', 'đầu ra', 'cấu trúc', 'phân bổ', 'sách', 'slide bài giảng',
                'video học liệu', 'thực tế', 'lý thuyết', 'đề cương', 'sát ngành học',
                'bám sát mục tiêu đào tạo', 'phù hợp năng lực', 'nội dung rộng',
                'nội dung hay', 'đầy đủ', 'cập nhật', 'thực tiến', 'trọng tâm',
                'đúng với cam kết', 'phù hợp', 'đảm bảo', 'cung cấp', 'đáp ứng'
            },
            
            'TAG_DH': {
                # Dạy và Học
                'giảng viên', 'dạy', 'giảng', 'phương pháp', 'truyền đạt',
                'thầy', 'cô', 'giáo viên', 'bài giảng', 'slide', 'giảng dạy',
                'hướng dẫn', 'giải thích', 'hoạt động dạy học', 'truyền cảm hứng',
                'tương tác', 'phát biểu', 'xây dựng bài', 'làm việc nhóm', 'hoạt động nhóm',
                'thuyết trình', 'thực hành', 'ví dụ', 'minh họa', 'luyện tập',
                'bài tập', 'rèn luyện', 'giải lao', 'môi trường học', 'không khí lớp học',
                'tiếp thu', 'nắm bắt', 'hiểu bài', 'dễ hiểu', 'khó hiểu', 'truyền đạt dễ hiểu',
                'nhiệt tình', 'tận tâm', 'dạy chậm', 'dạy kỹ', 'dạy nhanh', 'đọc chép'
            },
            
            'TAG_KT': {
                # Kiểm tra - Đánh giá
                'kiểm tra', 'đánh giá', 'thi', 'bài tập', 'điểm', 'chấm', 'đề thi',
                'giữa kỳ', 'cuối kỳ', 'điểm danh', 'bài kiểm tra', 'bài thi',
                'công bằng', 'công tâm', 'công khai', 'minh bạch', 'khách quan',
                'nghiêm túc', 'nghiêm ngặt', 'sát chương trình', 'đúng năng lực',
                'công bằng giữa các sinh viên', 'đánh giá quá trình', 'bài tập nhóm',
                'bài tập cá nhân', 'phản hồi', 'sửa bài', 'chỉnh sửa', 'nhận xét',
                'chấm chữa', 'kiểm tra online', 'thi online', 'làm bài nhóm'
            },
            
            'TAG_K': {
                # Khác (Cơ sở vật chất / Góp ý tổ chức)
                'cơ sở vật chất', 'phòng học', 'máy chiếu', 'điều hòa', 'loa', 'mic',
                'thời gian', 'lịch học', 'phòng thực hành', 'app', 'phần mềm',
                'website trường', 'website môn học', 'đăng ký học phần', 'trải nghiệm',
                'dữ liệu chung', 'bất tiện', 'giờ giấc', 'thời khóa biểu',
                'nghỉ giải lao', 'dạy lố giờ', 'canteen', 'bãi xe', 'wifi'
            }
        }
    
    def analyze_sentiment(self, text):
        if not text or len(text) < 5:
            return 'neutral'
        
        text_lower = text.lower()
        pos_count = sum(1 for w in self.positive_set if w in text_lower)
        neg_count = sum(1 for w in self.negative_set if w in text_lower)
        
        # Xử lý phủ định (đảo ngược)
        if any(neg in text_lower for neg in self.negations):
            pos_count, neg_count = neg_count, pos_count
        
        # Xử lý tăng cường
        intensifier = 1.5 if any(inten in text_lower for inten in self.intensifiers) else 1
        
        total = pos_count + neg_count
        if total == 0:
            return 'neutral'
        
        score = (pos_count - neg_count) * intensifier / total
        
        if score > 0.1:
            return 'positive'
        elif score < -0.1:
            return 'negative'
        return 'neutral'
    
    def extract_tags(self, text):
        if not text or len(text) < 3:
            return ['TAG_K']
        
        text_lower = text.lower()
        tags = []
        for tag, keywords in self.tag_keywords.items():
            for kw in keywords:
                if kw in text_lower:
                    tags.append(tag)
                    break
        if not tags:
            tags.append('TAG_K')
        return list(set(tags))  # Loại bỏ trùng

_nlp = VietnameseNLPRuleBased()


# ================= HÀM KIỂM TRA DỮ LIỆU RÁC =================
def is_valid_essay(text):
    if not text or not isinstance(text, str):
        return 0
    text = text.strip()
    if len(text) < 10:
        return 0
    if re.match(r'^[0-9\W_]+$', text):
        return 0
    if re.match(r'^[,;\.\s]+$', text):
        return 0
    letter_count = sum(1 for c in text if c.isalpha())
    if len(text) > 0 and letter_count / len(text) < 0.3:
        return 0
    return 1



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
        return pd.DataFrame(), pd.DataFrame()
    
    df = pd.read_csv(io.StringIO(content))
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame(), pd.DataFrame()
    
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    # Tạo MaNganh từ TenNganh: lấy 2 chữ cái đầu
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(
        lambda x: ''.join([w[0].upper() for w in str(x).split() if w])[:10] if pd.notna(x) else 'UNKNOWN'
    )
    
    dim_nganh = df_clean[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
    dim_chuyennganh = df_clean[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    return dim_nganh, dim_chuyennganh


# ================= PARSE SURVEY DATA (STREAMING + TỐI ƯU) =================
def is_date_format(value):
    return bool(_date_pattern.match(value.strip())) if isinstance(value, str) else False

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    v = value.strip()
    return (len(v) == 7 and v.isdigit()) or (len(v) == 7 and v.startswith("TG")) or v == "gvDacThu_TKTH"

def parse_lines_batch_fast(lines_batch):
    """Parse batch dòng - TỐI ƯU với ít cấp phát memory"""
    results = []
    for line in lines_batch:
        if not line or not line.strip():
            continue
        
        row = [x.strip() for x in line.split(',')]
        row_len = len(row)
        
        if row_len < 15:
            continue
        
        try:
            # Lấy 2 cột đầu
            lop = row[0]
            ma_sv = row[1]
            
            # Tìm ngày sinh (tối ưu: dừng sớm)
            ngay_sinh = ''
            ngay_sinh_index = -1
            for i in range(2, min(row_len, 12)):
                if is_date_format(row[i]):
                    ngay_sinh = row[i]
                    ngay_sinh_index = i
                    break
            
            if ngay_sinh_index == -1:
                continue
            
            # Lấy họ tên
            ho_dem = ''
            ten = ''
            if ngay_sinh_index > 1:
                name_parts = [p for p in row[2:ngay_sinh_index] if p]
                if name_parts:
                    ten = name_parts[-1]
                    ho_dem = ' '.join(name_parts[:-1]) if len(name_parts) > 1 else ''
            
            ma_hp = row[ngay_sinh_index + 1] if ngay_sinh_index + 1 < row_len else ''
            
            # Tìm mã GV
            ma_gv = ''
            ma_gv_index = -1
            for i in range(ngay_sinh_index + 2, min(row_len, ngay_sinh_index + 25)):
                if is_ma_gv_format(row[i]):
                    ma_gv = row[i]
                    ma_gv_index = i
                    break
            
            if ma_gv_index == -1:
                ma_gv_index = row_len - 4 if row_len >= 4 else ngay_sinh_index + 2
            
            # Tên học phần
            ten_hp = ' '.join(row[ngay_sinh_index + 2:ma_gv_index]) if ma_gv_index > ngay_sinh_index + 2 else ''
            
            # Thông tin GV
            ho_dem_gv = row[ma_gv_index + 1] if ma_gv_index + 1 < row_len else ''
            ten_gv = row[ma_gv_index + 2] if ma_gv_index + 2 < row_len else ''
            lop_hp = row[ma_gv_index + 3] if ma_gv_index + 3 < row_len else ''
            
            # Câu hỏi và giá trị
            cau_hoi = row[ma_gv_index + 4] if ma_gv_index + 4 < row_len else ''
            gia_tri = row[ma_gv_index + 5] if ma_gv_index + 5 < row_len else ''
            
            # Tìm NULL
            null_index = -1
            for i in range(ma_gv_index + 6, min(row_len, ma_gv_index + 20)):
                if row[i].upper() == 'NULL' or row[i] == '':
                    null_index = i
                    break
            
            # Lấy essay text
            essay_text = ''
            if null_index != -1 and null_index + 1 < row_len:
                after_null = row[null_index + 1:]
                essay_text = ','.join(after_null).strip()
            
            results.append({
                'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
                'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
                'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
                'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri,
                'EssayText': essay_text
            })
        except Exception:
            continue
    
    return results

def parse_survey_data_parallel_fast(content: str) -> pd.DataFrame:
    """Parse dữ liệu song song - TỐI ƯU, nhóm trực tiếp không qua DF trung gian"""
    print(f"  -> Đang parse với {NUM_WORKERS} workers...")
    start = time.time()
    
    lines = [l for l in content.strip().split('\n') if l.strip()]
    print(f"  -> Tổng số dòng: {len(lines):,}")
    
    # Chia batch
    batches = [lines[i:i+CHUNK_SIZE] for i in range(0, len(lines), CHUNK_SIZE)]
    
    # Xử lý song song
    all_results = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = [executor.submit(parse_lines_batch_fast, batch) for batch in batches]
        for future in as_completed(futures):
            all_results.extend(future.result())
    
    print(f"  -> Đã parse {len(all_results):,} dòng ({time.time()-start:.2f}s)")
    
    # NHÓM TRỰC TIẾP - KHÔNG QUA DATAFRAME
    print("  -> Đang nhóm thành phiếu...")
    start_group = time.time()
    
    surveys = {}
    for row in all_results:
        key = f"{row['MaSV']}_{row['LopHP']}_{row['MaGV']}"
        
        if key not in surveys:
            surveys[key] = {
                'Lop': row['Lop'], 'MaSV': row['MaSV'], 'HoDem': row['HoDem'],
                'Ten': row['Ten'], 'NgaySinh': row['NgaySinh'], 'MaHP': row['MaHP'],
                'TenHP': row['TenHP'], 'MaGV': row['MaGV'], 'HoDemGV': row['HoDemGV'],
                'TenGV': row['TenGV'], 'LopHP': row['LopHP'],
                'EssayText': row['EssayText'], 'DiemTracNghiem': {}
            }
        
        cau_hoi = row['CauHoi']
        gia_tri = row['GiaTri']
        if cau_hoi and gia_tri and cau_hoi.isdigit() and gia_tri.isdigit():
            surveys[key]['DiemTracNghiem'][int(cau_hoi)] = int(gia_tri)
    
    # Chuyển thành list các dict
    results = []
    for key, survey in surveys.items():
        results.append(survey)
    
    df_grouped = pd.DataFrame(results)
    print(f"  -> Đã nhóm {len(df_grouped):,} phiếu ({time.time()-start_group:.2f}s)")
    
    return df_grouped


# ================= TRANSFORM & NLP (TỐI ƯU) =================
def transform_with_nlp_fast(df):
    """Transform dữ liệu - TỐI ƯU"""
    print("  -> Transform dữ liệu với NLP...")
    start = time.time()
    
    # Tạo SubmissionID - vectorized
    df['SubmissionID'] = df['MaSV'] + '_' + df['LopHP'] + '_' + df['MaGV'] + '_' + FILE_NAME
    
    # Làm sạch text - vectorized
    df['NoiDungGopY'] = df['EssayText'].fillna('').astype(str).str.replace(r'\s+', ' ', regex=True).str.strip()
    
    # Xử lý NLP song song
    print(f"  -> Đang xử lý NLP với {NUM_WORKERS} workers...")
    texts = df['NoiDungGopY'].tolist()
    batches = [texts[i:i+CHUNK_SIZE] for i in range(0, len(texts), CHUNK_SIZE)]
    
    all_nlp_results = []
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for batch_results in executor.map(process_nlp_batch_fast, batches):
            all_nlp_results.extend(batch_results)
    
    df['Sentiment'] = [r[0] for r in all_nlp_results]
    df['Tags'] = [r[1] for r in all_nlp_results]
    df['Is_Valid'] = [r[2] for r in all_nlp_results]
    
    # Tạo FACT_KET_QUA_DANH_GIA - XÂY DỰNG TRỰC TIẾP (nhanh hơn explode)
    ketqua_data = []
    for _, row in df.iterrows():
        sub_id = row['SubmissionID']
        for cau, diem in row['DiemTracNghiem'].items():
            ketqua_data.append((sub_id, cau, diem))
    df_ketqua = pd.DataFrame(ketqua_data, columns=['SubmissionID', 'MaCauHoi', 'Diem'])
    
    # Tạo FACT_TAG_MAPPING - XÂY DỰNG TRỰC TIẾP
    tag_data = []
    for _, row in df.iterrows():
        sub_id = row['SubmissionID']
        for tag in row['Tags']:
            tag_data.append((sub_id, tag))
    df_tag = pd.DataFrame(tag_data, columns=['SubmissionID', 'MaTag'])
    
    print(f"  ✅ Transform xong ({time.time()-start:.2f}s)")
    return df, df_ketqua, df_tag

def process_nlp_batch_fast(texts):
    """Xử lý NLP batch"""
    results = []
    for text in texts:
        if not text or len(text) < 5:
            results.append(('neutral', ['TAG_K'], 0))
        else:
            sentiment = _nlp.analyze_sentiment(text)
            tags = _nlp.extract_tags(text)
            is_valid = is_valid_essay(text)
            results.append((sentiment, tags, is_valid))
    return results


# ================= LOAD DATABASE (BATCH LỚN HƠN) =================
def batch_insert_fast(cursor, table, columns, data, batch_size=20000):
    """Batch insert - TỐI ƯU với batch lớn hơn"""
    if not data:
        return 0
    
    placeholders = ', '.join(['?' for _ in columns])
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
    
    total = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]
        cursor.executemany(sql, batch)
        cursor.connection.commit()
        total += len(batch)
        if total % 50000 == 0:
            print(f"      -> Đã insert {total:,}/{len(data):,} dòng vào {table}")
    
    return total

batch_insert = batch_insert_fast

def load_dim_khoa(cursor, df_hp, df_nganh):
    all_khoa = set()
    if not df_hp.empty:
        all_khoa.update(df_hp['MaKhoa'].unique())
    if not df_nganh.empty:
        all_khoa.update(df_nganh['MaKhoa'].unique())
    all_khoa.update(['UNKNOWN', 'TĐHKT', 'PĐT'])
    
    # Lấy existing
    cursor.execute("SELECT MaKhoa FROM DIM_KHOA")
    existing = {row[0] for row in cursor.fetchall()}
    
    new_data = [(ma, ma) for ma in all_khoa if ma not in existing]
    if new_data:
        batch_insert(cursor, 'DIM_KHOA', ['MaKhoa', 'TenKhoa'], new_data, 1000)
    print(f"  ✅ DIM_KHOA: {len(new_data)} dòng mới")
    return len(new_data)


def load_chuyennganh_master(blob_service):
    """
    Đọc file TenChuyenNganh-Khoa.csv
    Trả về:
        - dim_nganh: DataFrame cho DIM_NGANH
        - dim_chuyennganh: DataFrame cho DIM_CHUYEN_NGANH
        - mapping: dict {MaChuyenNganh: {TenChuyenNganh, MaNganh, TenNganh, MaKhoa, TenKhoa}}
    """
    content = download_blob(blob_service, TAILIEU_CONTAINER, "TenChuyenNganh-Khoa.csv")
    if not content:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    df = pd.read_csv(io.StringIO(content))
    
    # Cấu trúc file: TT, Khoa, Ngành, Khối ngành, Chuyên ngành, Mã CN, Ghi chú
    if len(df.columns) >= 6:
        df_clean = df.iloc[:, [1, 2, 4, 5]].copy()
        df_clean.columns = ['TenKhoa', 'TenNganh', 'TenChuyenNganh', 'MaChuyenNganh']
    else:
        return pd.DataFrame(), pd.DataFrame(), {}
    
    # Loại bỏ dòng trống
    df_clean = df_clean.dropna(subset=['MaChuyenNganh'])
    df_clean = df_clean[df_clean['MaChuyenNganh'].astype(str).str.strip() != '']
    
    # Tạo MaKhoa từ TenKhoa (viết tắt)
    df_clean['MaKhoa'] = df_clean['TenKhoa'].apply(create_ma_khoa)
    
    # Tạo MaNganh từ TenNganh (lấy chữ cái đầu viết hoa)
    df_clean['MaNganh'] = df_clean['TenNganh'].apply(extract_ma_nganh_from_ten_nganh)
    
    # ===== BẢNG DIM_NGANH =====
    dim_nganh = df_clean[['MaNganh', 'TenNganh', 'MaKhoa']].drop_duplicates('MaNganh')
    
    # ===== BẢNG DIM_CHUYEN_NGANH =====
    dim_chuyennganh = df_clean[['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh']].drop_duplicates('MaChuyenNganh')
    
    # ===== TẠO MAPPING =====
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
    
    print(f"  -> Đã tạo mapping cho {len(mapping)} Mã Chuyên Ngành")
    return dim_nganh, dim_chuyennganh, mapping
    

def load_dim_nganh(cursor, df_nganh):
    """Load DIM_NGANH CHỈ từ file master, KHÔNG từ Lop"""
    count = 0
    
    if not df_nganh.empty:
        cursor.execute("SELECT MaNganh FROM DIM_NGANH")
        existing = {row[0] for row in cursor.fetchall()}
        
        data = []
        for _, row in df_nganh.iterrows():
            ma_nganh = row['MaNganh']
            if ma_nganh and ma_nganh not in existing:
                data.append((ma_nganh, row['TenNganh'], row['MaKhoa']))
                existing.add(ma_nganh)
        
        if data:
            count = batch_insert(cursor, 'DIM_NGANH', ['MaNganh', 'TenNganh', 'MaKhoa'], data, 1000)
    
    print(f"  ✅ DIM_NGANH: {count} dòng mới")
    return count

def load_dim_chuyennganh(cursor, df_chuyennganh, df_raw, mapping):
    """Load DIM_CHUYEN_NGANH từ file master + từ Lop (nếu có trong mapping)"""
    
    # Đảm bảo CTDT mặc định
    cursor.execute("""
        IF NOT EXISTS (SELECT 1 FROM DIM_CHUONG_TRINH_DAO_TAO WHERE MaCTDT = 'CTDT_CHINHQUY') 
        INSERT INTO DIM_CHUONG_TRINH_DAO_TAO (MaCTDT, TenCTDT) VALUES ('CTDT_CHINHQUY', N'Chính quy')
    """)
    cursor.connection.commit()
    
    count = 0
    
    # Lấy existing MaChuyenNganh trong DB
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing = {row[0] for row in cursor.fetchall()}
    
    # Lấy danh sách MaNganh đã có trong DIM_NGANH
    cursor.execute("SELECT MaNganh FROM DIM_NGANH")
    existing_nganh = {row[0] for row in cursor.fetchall()}
    
    # ===== 1. Từ file master =====
    if not df_chuyennganh.empty:
        data = []
        for _, row in df_chuyennganh.iterrows():
            ma_chuyen = row['MaChuyenNganh']
            if ma_chuyen and ma_chuyen not in existing:
                ma_nganh = row['MaNganh']
                if ma_nganh not in existing_nganh:
                    print(f"      ⚠️ MaNganh '{ma_nganh}' chưa có trong DIM_NGANH, bỏ qua {ma_chuyen}")
                    continue
                data.append((ma_chuyen, row['TenChuyenNganh'], ma_nganh, 'CTDT_CHINHQUY'))
                existing.add(ma_chuyen)
        
        if data:
            count += batch_insert(cursor, 'DIM_CHUYEN_NGANH', 
                                 ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh', 'MaCTDT'], 
                                 data, 1000)
    
    # ===== 2. Từ dữ liệu Lop (chỉ những mã có trong mapping) =====
    df_lop = df_raw[['Lop']].drop_duplicates('Lop')
    df_lop = df_lop[df_lop['Lop'].notna() & (df_lop['Lop'] != '')]
    
    data_lop = []
    for _, row in df_lop.iterrows():
        # Xác định MaChuyenNganh từ Lop
        ma_cn, _, _, _ = determine_ma_chuyen_nganh(row['Lop'])
        
        if ma_cn and ma_cn not in existing:
            # Kiểm tra xem ma_cn có trong mapping từ file master không
            if ma_cn in mapping:
                info = mapping[ma_cn]
                ma_nganh = info['MaNganh']
                ten_chuyen_nganh = info['TenChuyenNganh']
                
                # Kiểm tra MaNganh đã có trong DIM_NGANH chưa
                if ma_nganh not in existing_nganh:
                    print(f"      ⚠️ MaNganh '{ma_nganh}' chưa có trong DIM_NGANH, bỏ qua {ma_cn}")
                    continue
                
                data_lop.append((ma_cn, ten_chuyen_nganh, ma_nganh, 'CTDT_CHINHQUY'))
                existing.add(ma_cn)
            else:
                print(f"      ⚠️ MaChuyenNganh '{ma_cn}' không có trong file master, bỏ qua")
    
    if data_lop:
        count += batch_insert(cursor, 'DIM_CHUYEN_NGANH',
                             ['MaChuyenNganh', 'TenChuyenNganh', 'MaNganh', 'MaCTDT'],
                             data_lop, 1000)
    
    print(f"  ✅ DIM_CHUYEN_NGANH: {count} dòng mới")
    return count

def load_dim_lop_sinh_vien(cursor, df_raw, mapping):
    """Load DIM_LOP_SINH_VIEN - chỉ insert khi MaChuyenNganh có trong mapping và đã có trong DIM_CHUYEN_NGANH"""
    
    # Lấy danh sách MaChuyenNganh đã có trong DIM_CHUYEN_NGANH
    cursor.execute("SELECT MaChuyenNganh FROM DIM_CHUYEN_NGANH")
    existing_chuyennganh = {row[0] for row in cursor.fetchall()}
    
    # Lấy danh sách MaLop đã có
    cursor.execute("SELECT MaLop FROM DIM_LOP_SINH_VIEN")
    existing_lop = {row[0] for row in cursor.fetchall()}
    
    df_lop = df_raw[['Lop']].drop_duplicates('Lop')
    df_lop = df_lop[df_lop['Lop'].notna() & (df_lop['Lop'] != '')]
    
    data = []
    for _, row in df_lop.iterrows():
        # Xác định MaChuyenNganh từ Lop
        ma_cn, _, _, _ = determine_ma_chuyen_nganh(row['Lop'])
        
        if ma_cn and ma_cn in existing_chuyennganh:
            if row['Lop'] not in existing_lop:
                data.append((row['Lop'], row['Lop'], ma_cn))
                existing_lop.add(row['Lop'])
    
    if data:
        batch_insert(cursor, 'DIM_LOP_SINH_VIEN', ['MaLop', 'Lop', 'MaChuyenNganh'], data, 5000)
        print(f"  ✅ DIM_LOP_SINH_VIEN: {len(data)} dòng mới")
        return len(data)
    
    print(f"  ✅ DIM_LOP_SINH_VIEN: 0 dòng mới")
    return 0

def load_dim_sinh_vien(cursor, df_raw):
    df_sv = df_raw[['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'Lop']].drop_duplicates('MaSV')
    df_sv = df_sv[df_sv['MaSV'].notna() & (df_sv['MaSV'] != '')]
    
    data = []
    for _, row in df_sv.iterrows():
        ngay_sinh = None
        if row['NgaySinh'] and row['NgaySinh'] != '':
            try:
                ngay_sinh = datetime.strptime(row['NgaySinh'], '%d/%m/%Y').date()
            except:
                pass
        data.append((row['MaSV'], row['HoDem'], row['Ten'], ngay_sinh, row['Lop']))
    
    if data:
        cursor.execute("SELECT MaSV FROM DIM_SINH_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [d for d in data if d[0] not in existing]
        if new_data:
            batch_insert(cursor, 'DIM_SINH_VIEN', ['MaSV', 'HoDem', 'Ten', 'NgaySinh', 'MaLop'], new_data, 5000)
            print(f"  ✅ DIM_SINH_VIEN: {len(new_data)} dòng mới")
            return len(new_data)
    
    print(f"  ✅ DIM_SINH_VIEN: 0 dòng mới")
    return 0

def load_dim_giang_vien(cursor, df_raw):
    df_gv = df_raw[['MaGV', 'HoDemGV', 'TenGV']].drop_duplicates('MaGV')
    df_gv = df_gv[df_gv['MaGV'].notna() & (df_gv['MaGV'] != '')]
    
    data = [(row['MaGV'], row['HoDemGV'], row['TenGV']) for _, row in df_gv.iterrows()]
    
    if data:
        cursor.execute("SELECT MaGV FROM DIM_GIANG_VIEN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [d for d in data if d[0] not in existing]
        if new_data:
            batch_insert(cursor, 'DIM_GIANG_VIEN', ['MaGV', 'HoDemGV', 'TenGV'], new_data, 5000)
            print(f"  ✅ DIM_GIANG_VIEN: {len(new_data)} dòng mới")
            return len(new_data)
    
    print(f"  ✅ DIM_GIANG_VIEN: 0 dòng mới")
    return 0

def load_dim_hoc_phan(cursor, df_hp_master, df_raw):
    count = 0
    
    # Từ HP-Khoa.csv
    if not df_hp_master.empty:
        data = [(row['MaHP'], row['TenHP'], row['MaKhoa']) for _, row in df_hp_master.iterrows()]
        cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [d for d in data if d[0] not in existing]
        if new_data:
            count += batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data, 5000)
    
    # Từ dữ liệu raw
    df_hp_raw = df_raw[['MaHP', 'TenHP']].drop_duplicates('MaHP')
    df_hp_raw = df_hp_raw[df_hp_raw['MaHP'].notna() & (df_hp_raw['MaHP'] != '')]
    
    data_raw = [(row['MaHP'], row['TenHP'], 'UNKNOWN') for _, row in df_hp_raw.iterrows()]
    cursor.execute("SELECT MaHP FROM DIM_HOC_PHAN")
    existing = {row[0] for row in cursor.fetchall()}
    new_data = [d for d in data_raw if d[0] not in existing]
    if new_data:
        count += batch_insert(cursor, 'DIM_HOC_PHAN', ['MaHP', 'TenHP', 'MaKhoa'], new_data, 5000)
    
    print(f"  ✅ DIM_HOC_PHAN: {count} dòng mới")
    return count

def load_dim_hoc_ky(cursor):
    ma_hoc_ky, nam_hoc, hoc_ky = derive_ma_hoc_ky()
    cursor.execute("SELECT MaHocKy FROM DIM_HOC_KY WHERE MaHocKy = ?", ma_hoc_ky)
    if not cursor.fetchone():
        cursor.execute("INSERT INTO DIM_HOC_KY (MaHocKy, NamHoc, HocKy) VALUES (?, ?, ?)", ma_hoc_ky, nam_hoc, hoc_ky)
        cursor.connection.commit()
        print(f"  ✅ DIM_HOC_KY: {ma_hoc_ky} (mới)")
    else:
        print(f"  ✅ DIM_HOC_KY: {ma_hoc_ky} (đã tồn tại)")
    return 1

def load_dim_lop_hoc_phan(cursor, df_raw, ma_hoc_ky):
    df_lhp = df_raw[['LopHP', 'MaHP', 'MaGV']].drop_duplicates('LopHP')
    df_lhp = df_lhp[df_lhp['LopHP'].notna() & (df_lhp['LopHP'] != '')]
    
    data = [(row['LopHP'], row['LopHP'], row['MaHP'], row['MaGV'], ma_hoc_ky) for _, row in df_lhp.iterrows()]
    
    if data:
        cursor.execute("SELECT MaLopHP FROM DIM_LOP_HOC_PHAN")
        existing = {row[0] for row in cursor.fetchall()}
        new_data = [d for d in data if d[0] not in existing]
        if new_data:
            batch_insert(cursor, 'DIM_LOP_HOC_PHAN', ['MaLopHP', 'LopHP', 'MaHP', 'MaGV', 'MaHocKy'], new_data, 5000)
            print(f"  ✅ DIM_LOP_HOC_PHAN: {len(new_data)} dòng mới")
            return len(new_data)
    
    print(f"  ✅ DIM_LOP_HOC_PHAN: 0 dòng mới")
    return 0

def load_all_dimensions(cursor, df_raw, df_hp_master, df_nganh, df_chuyennganh, mapping):
    print("\n📥 Loading DIMENSION tables...")
    
    ma_hoc_ky, _, _ = derive_ma_hoc_ky()
    
    # 1. DIM_KHOA
    load_dim_khoa(cursor, df_hp_master, df_nganh)
    
    # 2. DIM_NGANH (CHỈ từ file master, KHÔNG từ Lop)
    load_dim_nganh(cursor, df_nganh)
    
    # 3. DIM_CHUONG_TRINH_DAO_TAO (mặc định)
    
    # 4. DIM_CHUYEN_NGANH (từ file master + Lop có trong mapping)
    load_dim_chuyennganh(cursor, df_chuyennganh, df_raw, mapping)
    
    # 5. DIM_LOP_SINH_VIEN
    load_dim_lop_sinh_vien(cursor, df_raw, mapping)
    
    # 6. Các bảng còn lại
    load_dim_sinh_vien(cursor, df_raw)
    load_dim_giang_vien(cursor, df_raw)
    load_dim_hoc_phan(cursor, df_hp_master, df_raw)
    load_dim_hoc_ky(cursor)
    load_dim_lop_hoc_phan(cursor, df_raw, ma_hoc_ky)
    
    print("  ✅ All DIMENSION tables loaded!")


# ================= LOAD FACT TABLES =================
def load_fact_tables(cursor, df_main, df_ketqua, df_tag):
    print("\n📥 Loading FACT tables (LOAD TẤT CẢ)...")
    
    # FACT_GOP_Y_TU_LUAN
    data_main = [(row['SubmissionID'], row['MaSV'], row['LopHP'], 
                  row['NoiDungGopY'][:4000] if row['NoiDungGopY'] else '',
                  row['Sentiment'], row['Is_Valid']) 
                 for _, row in df_main.iterrows()]
    count_main = batch_insert(cursor, 'FACT_GOP_Y_TU_LUAN',
                              ['SubmissionID', 'MaSV', 'MaLopHP', 'NoiDungGopY', 'Sentiment', 'Is_Valid'],
                              data_main, 10000)
    print(f"    ✅ FACT_GOP_Y_TU_LUAN: {count_main} dòng")
    
    # FACT_KET_QUA_DANH_GIA
    data_kq = [tuple(x) for x in df_ketqua[['SubmissionID', 'MaCauHoi', 'Diem']].values]
    count_kq = batch_insert(cursor, 'FACT_KET_QUA_DANH_GIA',
                            ['SubmissionID', 'MaCauHoi', 'Diem'],
                            data_kq, 20000)
    print(f"    ✅ FACT_KET_QUA_DANH_GIA: {count_kq} dòng ({count_kq//12} phiếu)")
    
    # FACT_TAG_MAPPING
    data_tag = [tuple(x) for x in df_tag[['SubmissionID', 'MaTag']].values]
    count_tag = batch_insert(cursor, 'FACT_TAG_MAPPING',
                             ['SubmissionID', 'MaTag'],
                             data_tag, 20000)
    print(f"    ✅ FACT_TAG_MAPPING: {count_tag} dòng")
    
    return count_main, count_kq, count_tag

# ================= MAIN =================
def main():
    total_start = time.time()
    print("=" * 60)
    print("🚀 ETL PIPELINE - PHIÊN BẢN TỐI ƯU")
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
    survey_content = download_blob(blob_service, CONTAINER_NAME, f"{RAWDATA_PATH}/{SURVEY_FILE}")
    
    if not survey_content:
        print("  ❌ Không đọc được file survey!")
        return
    
    # 4. Parse - PHIÊN BẢN TỐI ƯU
    print("\n📝 4. Parse dữ liệu...")
    df_raw = parse_survey_data_parallel_fast(survey_content)
    
    if df_raw.empty:
        print("  ❌ Không có dữ liệu!")
        return
    
    # 5. Transform & NLP - PHIÊN BẢN TỐI ƯU
    print("\n🔄 5. Transform & NLP...")
    df_main, df_ketqua, df_tag = transform_with_nlp_fast(df_raw)
    
    # Giải phóng memory
    del df_raw
    
    # 6. Lưu CSV backup
    print("\n💾 6. Lưu CSV backup...")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    save_processed(blob_service, df_main, f"{FILE_NAME}_main_{timestamp}.csv")
    
    # 7. Kết nối SQL
    print("\n💾 7. Kết nối SQL Database...")
    try:
        conn = pyodbc.connect(CONN_STR)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        print("  ✅ Kết nối SQL thành công")
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        return
    
    try:
        # 8. Load DIMENSION
        load_all_dimensions(cursor, df_raw, hp_master, dim_nganh, dim_chuyennganh, mapping)
        
        # 9. Load FACT
        count_main, count_kq, count_tag = load_fact_tables(cursor, df_main, df_ketqua, df_tag)
        
    except Exception as e:
        print(f"  ❌ Lỗi: {e}")
        raise
    finally:
        cursor.close()
        conn.close()
    
    # 10. Thống kê
    print("\n📊 10. KẾT QUẢ:")
    print(f"   - Số phiếu: {len(df_main):,}")
    print(f"   - Hợp lệ: {df_main['Is_Valid'].sum():,} ({df_main['Is_Valid'].mean()*100:.1f}%)")
    print(f"   - Điểm TN: {count_kq:,} dòng")
    print(f"   - Tag: {count_tag:,} dòng")
    
    print("\n   - Sentiment:")
    for sent, cnt in df_main['Sentiment'].value_counts().items():
        print(f"      {sent}: {cnt:,} ({cnt/len(df_main)*100:.1f}%)")
    
    print("\n" + "=" * 60)
    print(f"✅ HOÀN THÀNH! Thời gian: {time.time()-total_start:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
