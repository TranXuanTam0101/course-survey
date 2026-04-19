import os
import sys
import re
import hashlib
import pymssql
import pandas as pd
import io
from azure.storage.blob import BlobServiceClient

CONNECTION_STRING = os.environ.get("CONNECTION_STRING")
SEMESTER = os.environ.get("SEMESTER")
SURVEY_FILE = os.environ.get("SURVEY_FILE")

if not SEMESTER or not SURVEY_FILE:
    print("Thiếu biến môi trường SEMESTER hoặc SURVEY_FILE")
    sys.exit(1)

FILE_NAME = os.path.splitext(os.path.basename(SURVEY_FILE))[0]

# ========== TỪ KHÓA CHO CÁC CỘT ==========

KEYWORDS_CAU13 = [
    'nội dung', 'chuẩn đầu ra', 'chương trình', 'học phần', 'môn học',
    'đáp ứng', 'phù hợp', 'bám sát', 'rõ ràng', 'đầy đủ', 'hợp lý', 'hợp lí',
    'sát chương trình', 'dễ tiếp cận', 'kiến thức cơ bản', 'trang bị',
    'cung cấp', 'đào tạo', 'mục tiêu', 'chất lượng', 'đảm bảo',
    'bổ ích', 'cần thiết', 'quan trọng', 'trọng tâm', 'chi tiết',
    'cụ thể', 'đúng', 'chuẩn', 'ổn', 'hay', 'được', 'phương pháp', 'tệ','không','ko','k','rõ rãng'
]

KEYWORDS_CAU14 = [
    'thầy', 'cô', 'giảng viên', 'gv', 'thầy giáo', 'cô giáo',
    'dạy', 'giảng', 'bài giảng', 'dễ hiểu', 'nhiệt tình', 
    'tận tâm', 'tận tình', 'vui vẻ', 'thân thiện', 'hấp dẫn', 
    'thú vị', 'tương tác', 'sôi nổi', 'truyền đạt', 'giải thích', 
    'hướng dẫn', 'phương pháp', 'nhiệt huyết', 'rõ', 'kỹ',
    'sinh động', 'linh hoạt', 'đa dạng', 'thu hút',
    'ví dụ thực tế', 'dẫn dắt', 'tạo hứng thú', 'năng động', 
    'đáng yêu', 'dễ thương', 'dễ mến', 'dễ gần', 'tâm lý', 
    'thấu hiểu', 'quan tâm', 'chu đáo', 'tận tụy', 'sẵn sàng giúp đỡ',
    'giải đáp thắc mắc', 'hỗ trợ', 'chỉ bảo', 'năng nổ', 'có tâm',
    'truyền cảm hứng', 'gần gũi', 'thoải mái', 'hào hứng', 'vui',
    'dui dẻ', 'hòa đồng', 'thương học trò','hay','tâm huyết'
]

KEYWORDS_CAU15 = [
    'kiểm tra', 'đánh giá', 'thi', 'bài tập', 'điểm', 'chấm',
    'đề thi', 'công bằng', 'minh bạch', 'nghiêm túc', 'phù hợp',
    'thực lực', 'khách quan', 'công tâm', 'đề kiểm tra',
    'giữa kỳ', 'cuối kỳ', 'bài kiểm tra', 'cho điểm',
    'công khai', 'rõ ràng', 'đảm bảo tính công bằng',
    'nghiêm ngặt', 'đánh giá đúng', 'phản ánh đúng', 'thuyết phục',
    'chính xác', 'kỹ càng', 'chỉnh chu', 'đa dạng hình thức', 'tài liệu', 'đọc thêm','không','ko','k','công tác'
]

KEYWORDS_CAU16 = [
    'không', 'ko', 'ok', 'oki', 'ổn', 'được',
    'không có', 'không ạ', 'dạ không', 'không có ý kiến',
    'hết', 'xong', 'cảm ơn', 'thanks', 'k', 'không góp ý',
    'không có góp ý', 'không góp ý gì', 'không ý kiến',
    'em không có', 'dạ không có', 'không có ạ', 'ko có',
    'không gì', 'không có gì', 'không còn góp ý',
    'mãi yêu cô', 'yêu cô', 'cảm ơn cô', 'cảm ơn thầy',
    'tuyệt vời', 'quá ok', 'rất ok', 'ổn hết', 'tốt hơn'
]

# ========== LỚP XỬ LÝ DATABASE ==========

class DatabaseLoader:
    def __init__(self, blob_service, semester, survey_file):
        self.blob_service = blob_service
        self.semester = semester
        self.survey_file = survey_file
        
        # Trích xuất năm học và học kỳ
        self.nam_hoc = semester
        self.hoc_ky = self._extract_hocky(survey_file)
        self.ma_hoc_ky = f"HK{self.hoc_ky}-{self.nam_hoc}"
        
        # Năm học cho mapping
        if '-' in semester:
            year = semester.split('-')[0]
            self.school_year = f"{year}-{int(year)+1}"
        else:
            self.school_year = semester
    
    def _extract_hocky(self, filename):
        match = re.search(r'(\d{3})', filename)
        if match:
            return int(match.group(1)[0])
        return 1
    
    def _create_ma_khoa(self, ten_khoa):
        if not ten_khoa:
            return 'UNK'
        words = ten_khoa.split()
        return ''.join([word[0].upper() for word in words])
    
    def _parse_ma_lop(self, ma_lop):
        if not ma_lop:
            return None
        match = re.search(r'K([A-Z0-9\-]+)', ma_lop)
        if match:
            return 'K' + match.group(1).split('-')[0]
        return None
    

    def connect(self):
        return pymssql.connect(
            server='course-survey.database.windows.net',
            user='sqladmin',
            password='Due@2026',
            database='course-survey-db'
        )
    
    def load_mapping(self):
        mapping = {'hoc_phan': {}, 'chuyen_nganh': {}}
        try:
            container = self.blob_service.get_container_client("tailieu")
            
            # HP-Khoa.csv
            path = f"tailieu/{self.school_year}/HP-Khoa.csv"
            data = container.get_blob_client(path).download_blob().readall()
            df = pd.read_csv(io.BytesIO(data), encoding='utf-8-sig')
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                mapping['hoc_phan'][row['Mã học phần']] = {
                    'TenHP': row['Tên học phần'],
                    'Khoa': row['Khoa']
                }
            
            # TenChuyenNganh-Khoa
            path = f"tailieu/{self.school_year}/TenChuyenNganh-Khoa"
            data = container.get_blob_client(path).download_blob().readall()
            df = pd.read_csv(io.BytesIO(data), encoding='utf-8-sig')
            df.columns = df.columns.str.strip()
            for _, row in df.iterrows():
                mapping['chuyen_nganh'][row['MaChuyenNganh']] = {
                    'TenChuyenNganh': row['TenChuyenNganh'],
                    'Khoa': row['Khoa']
                }
            
            return mapping
        except Exception as e:
            print(f"Lỗi mapping: {e}")
            return mapping
    
    def insert(self, rows):
        conn = self.connect()
        cursor = conn.cursor()
        mapping = self.load_mapping()
        
        # Bảng độc lập
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM DIM_HOC_KY WHERE MaHocKy = ?)
            INSERT INTO DIM_HOC_KY VALUES (?, ?, ?)
        """, self.ma_hoc_ky, self.ma_hoc_ky, self.nam_hoc, self.hoc_ky)
        
        # 16 câu hỏi
        cau_hoi_list = [
            (1, 1, 'I', 'Giảng viên giới thiệu rõ ràng, đầy đủ về đề cương chi tiết học phần', 'so'),
            (2, 2, 'I', 'Nội dung của học phần phù hợp với năng lực của người học', 'so'),
            (3, 3, 'I', 'Phương pháp dạy - học phù hợp với chuẩn đầu ra và nội dung của học phần', 'so'),
            (4, 4, 'I', 'Giảng viên thực hiện đầy đủ kế hoạch dạy - học đã công bố', 'so'),
            (5, 5, 'I', 'Giảng viên có cập nhật kiến thức mới và thực tế trong bài giảng', 'so'),
            (6, 6, 'I', 'Hoạt động dạy - học khơi gợi đam mê khám phá và giúp phát triển khả năng tự học', 'so'),
            (7, 7, 'I', 'Giảng viên khuyến khích người học chủ động tham gia thảo luận', 'so'),
            (8, 8, 'I', 'Giảng viên tận tụy, sẵn sàng giúp đỡ, giải đáp thắc mắc của người học', 'so'),
            (9, 9, 'I', 'Giảng viên sử dụng hiệu quả Elearning và các phương tiện công nghệ', 'so'),
            (10, 10, 'I', 'Phương pháp kiểm tra, đánh giá phù hợp với chuẩn đầu ra và nội dung', 'so'),
            (11, 11, 'I', 'Việc đánh giá được thực hiện công bằng, khách quan và đảm bảo độ tin cậy', 'so'),
            (12, 12, 'I', 'Anh/Chị hài lòng về chất lượng và hiệu quả giảng dạy của giảng viên', 'so'),
            (13, 13, 'II', 'Về chuẩn đầu ra và nội dung của học phần', 'text'),
            (14, 14, 'II', 'Về hoạt động dạy - học', 'text'),
            (15, 15, 'II', 'Về công tác kiểm tra – đánh giá', 'text'),
            (16, 16, 'II', 'Các góp ý khác', 'text')
        ]
        
        for ma, tt, phan, nd, loai in cau_hoi_list:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM DIM_CAU_HOI WHERE MaCauHoi = ?)
                INSERT INTO DIM_CAU_HOI VALUES (?, ?, ?, ?, ?)
            """, ma, ma, tt, phan, nd, loai)
        
        success = 0
        for row in rows:
            try:
                cursor.execute("BEGIN TRANSACTION")
                
                # Xử lý chuyên ngành từ mã lớp
                ma_lop = row.get('Lop', '')
                ma_chuyen_nganh = self._parse_ma_lop(ma_lop)
                
                if ma_chuyen_nganh and ma_chuyen_nganh in mapping['chuyen_nganh']:
                    cn = mapping['chuyen_nganh'][ma_chuyen_nganh]
                    ten_khoa = cn['Khoa']
                    ma_khoa = self._create_ma_khoa(ten_khoa)
                    
                    # DIM_KHOA
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_KHOA WHERE MaKhoa = ?)
                        INSERT INTO DIM_KHOA VALUES (?, ?)
                    """, ma_khoa, ma_khoa, ten_khoa)
                    
                    # DIM_CHUYEN_NGANH
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_CHUYEN_NGANH WHERE MaChuyenNganh = ?)
                        INSERT INTO DIM_CHUYEN_NGANH VALUES (?, ?, ?, ?)
                    """, ma_chuyen_nganh, ma_chuyen_nganh, cn['TenChuyenNganh'], ma_khoa, 'CTDT001')
                    
                    # DIM_LOP_SINH_VIEN
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_LOP_SINH_VIEN WHERE MaLop = ?)
                        INSERT INTO DIM_LOP_SINH_VIEN VALUES (?, ?, ?)
                    """, ma_lop, ma_lop, ma_lop, ma_chuyen_nganh)
                
                # DIM_SINH_VIEN
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DIM_SINH_VIEN WHERE MaSV = ?)
                    INSERT INTO DIM_SINH_VIEN VALUES (?, ?, ?, ?, ?)
                """, row['MaSV'], row['MaSV'], row.get('HoDem', ''), row.get('Ten', ''), row.get('NgaySinh'), row.get('Lop', ''))
                
                # DIM_GIANG_VIEN
                if row.get('MaGV'):
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_GIANG_VIEN WHERE MaGV = ?)
                        INSERT INTO DIM_GIANG_VIEN VALUES (?, ?, ?)
                    """, row['MaGV'], row['MaGV'], row.get('HoDemGV', ''), row.get('TenGV', ''))
                
                # DIM_HOC_PHAN
                ma_khoa_hp = None
                if row.get('MaHP') and row['MaHP'] in mapping['hoc_phan']:
                    hp = mapping['hoc_phan'][row['MaHP']]
                    ma_khoa_hp = self._create_ma_khoa(hp['Khoa'])
                    
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_KHOA WHERE MaKhoa = ?)
                        INSERT INTO DIM_KHOA VALUES (?, ?)
                    """, ma_khoa_hp, ma_khoa_hp, hp['Khoa'])
                    
                    cursor.execute("""
                        IF NOT EXISTS (SELECT 1 FROM DIM_HOC_PHAN WHERE MaHP = ?)
                        INSERT INTO DIM_HOC_PHAN VALUES (?, ?, ?)
                    """, row['MaHP'], row['MaHP'], hp['TenHP'], ma_khoa_hp)
                
                # DIM_LOP_HOC_PHAN
                ma_lop_hp = f"{row['MaHP']}_{row['LopHP']}_{self.ma_hoc_ky}"
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM DIM_LOP_HOC_PHAN WHERE MaLopHP = ?)
                    INSERT INTO DIM_LOP_HOC_PHAN VALUES (?, ?, ?, ?, ?)
                """, ma_lop_hp, ma_lop_hp, row['LopHP'], row['MaHP'], row.get('MaGV'), self.ma_hoc_ky)
                
                # FACT_TRA_LOI_KHAO_SAT
                submission_id = hashlib.md5(
                    f"{row['MaSV']}_{ma_lop_hp}_{row.get('MaGV', '')}_{self.ma_hoc_ky}_{self.survey_file}".encode()
                ).hexdigest()[:50]
                
                for ma_cau, col in [(13, 'Cau13'), (14, 'Cau14'), (15, 'Cau15'), (16, 'Cau16')]:
                    if row.get(col):
                        cursor.execute("""
                            IF NOT EXISTS (SELECT 1 FROM FACT_TRA_LOI_KHAO_SAT WHERE SubmissionID = ? AND MaCauHoi = ?)
                            INSERT INTO FACT_TRA_LOI_KHAO_SAT VALUES (?, ?, ?, ?, ?, ?)
                        """, submission_id, ma_cau, submission_id, ma_cau, row['MaSV'], ma_lop_hp, None, row[col])
                
                cursor.execute("COMMIT")
                success += 1
                
            except Exception as e:
                cursor.execute("ROLLBACK")
                print(f"Lỗi dòng {row.get('MaSV', '?')}: {e}")
        
        cursor.close()
        conn.close()
        print(f"Đã chèn {success}/{len(rows)} dòng")
        return success

# ========== CÁC HÀM XỬ LÝ CSV ==========

def download_from_blob(blob_service):
    try:
        blob_client = blob_service.get_container_client("rawdata").get_blob_client(f"{SEMESTER}/{SURVEY_FILE}")
        data = blob_client.download_blob().readall()
        with open(SURVEY_FILE, "wb") as f:
            f.write(data)
        return True
    except Exception as e:
        print(f"Lỗi tải file: {e}")
        sys.exit(1)

def upload_to_blob(blob_service, df, output_path):
    try:
        output = df.to_csv(index=False, encoding='utf-8-sig')
        container = blob_service.get_container_client("processed-data")
        if not container.exists():
            container.create_container()
        container.get_blob_client(output_path).upload_blob(output, overwrite=True)
        return True
    except Exception as e:
        print(f"Lỗi upload: {e}")
        return False

def is_date_format(value):
    if not isinstance(value, str):
        return False
    return bool(re.match(r'^\d{2}/\d{2}/\d{4}$', value.strip()))

def is_ma_gv_format(value):
    if not isinstance(value, str):
        return False
    value = value.strip()
    if len(value) == 7 and value.isdigit():
        return True
    if len(value) == 7 and value.startswith("TG"):
        return True
    if value == "gvDacThu_TKTH":
        return True
    return False

def has_keyword(text, keywords):
    if not text or not isinstance(text, str):
        return False
    text_lower = text.lower()
    return any(kw in text_lower for kw in keywords)

def clean_special_characters(parts):
    cleaned = []
    for part in parts:
        if part and part.strip():
            cleaned.append(part)
    return cleaned

def split_by_condition_1(text):
    parts = []
    current = []
    i = 0
    while i < len(text):
        if text[i] == ',':
            has_space_before = (i > 0 and text[i-1] == ' ')
            has_space_after = (i + 1 < len(text) and text[i+1] == ' ')
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
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text) and text[i+1] == ' ':
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
    while i < len(text):
        if text[i] == ',':
            if i + 1 < len(text):
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

def classify_general_parts(parts):
    valid_parts = clean_special_characters(parts)
    if not valid_parts:
        return "", "", "", ""
    
    current_col = "Cau13"
    cau13 = valid_parts[0]
    cau14 = ""
    cau15 = ""
    cau16 = ""
    
    if len(valid_parts) == 1:
        return cau13, cau14, cau15, cau16
    
    remaining_parts = valid_parts[1:]
    last_part = remaining_parts[-1]
    is_special_last = has_keyword(last_part, KEYWORDS_CAU16) and last_part.lower() in ['không', 'k', 'không có', 'ko']
    
    if is_special_last:
        cau16 = last_part
        remaining_parts = remaining_parts[:-1]
    else:
        cau16 = last_part
        remaining_parts = remaining_parts[:-1]
    
    for part in remaining_parts:
        if current_col == "Cau13":
            if has_keyword(part, KEYWORDS_CAU13):
                cau13 = f"{cau13}, {part}"
            elif has_keyword(part, KEYWORDS_CAU14):
                current_col = "Cau14"
                cau14 = part
            else:
                cau13 = f"{cau13}, {part}"
        elif current_col == "Cau14":
            if has_keyword(part, KEYWORDS_CAU14):
                cau14 = f"{cau14}, {part}"
            elif has_keyword(part, KEYWORDS_CAU15):
                current_col = "Cau15"
                cau15 = part
            else:
                current_col = "Cau15"
                cau15 = part
        elif current_col == "Cau15":
            if has_keyword(part, KEYWORDS_CAU15):
                cau15 = f"{cau15}, {part}"
            elif has_keyword(part, KEYWORDS_CAU16):
                current_col = "Cau16"
                cau16 = f"{cau16}, {part}" if cau16 else part
            else:
                current_col = "Cau16"
                cau16 = f"{cau16}, {part}" if cau16 else part
        else:
            cau16 = f"{cau16}, {part}" if cau16 else part
    
    return cau13, cau14, cau15, cau16

def classify_by_position_and_keywords(parts):
    num_parts = len(parts)
    if num_parts == 5:
        return classify_5_parts(parts)
    elif num_parts == 6:
        return classify_6_parts(parts)
    else:
        return classify_general_parts(parts)

def classify_5_parts(parts):
    valid_parts = clean_special_characters(parts)
    if len(valid_parts) < 5:
        return classify_general_parts(valid_parts)
    
    P1, P2, P3, P4, P5 = valid_parts
    cau13 = P1
    cau16 = P5
    cau14 = ""
    cau15 = ""
    
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14 = P2
        if has_keyword(P3, KEYWORDS_CAU14):
            cau14 = f"{cau14}, {P3}"
            cau15 = P4
        else:
            cau15 = P3
            cau15 = f"{cau15}, {P4}" if P4 else cau15
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14 = P3
        cau13 = f"{cau13}, {P2}"
        cau15 = P4
    else:
        cau13 = f"{cau13}, {P2}"
        if has_keyword(P4, KEYWORDS_CAU15):
            cau15 = P4
        else:
            cau14 = P3
            cau15 = P4
    
    return cau13, cau14, cau15, cau16

def classify_6_parts(parts):
    valid_parts = clean_special_characters(parts)
    if len(valid_parts) < 6:
        return classify_general_parts(valid_parts)
    
    P1, P2, P3, P4, P5, P6 = valid_parts
    cau13 = P1
    cau16 = P6
    cau14 = ""
    cau15 = ""
    
    if has_keyword(P2, KEYWORDS_CAU14):
        cau14 = P2
        if len(valid_parts) >= 4:
            cau14 = f"{cau14}, {P3}"
        if has_keyword(P4, KEYWORDS_CAU15):
            cau15 = P4
            cau15 = f"{cau15}, {P5}" if P5 else cau15
        else:
            cau14 = f"{cau14}, {P4}"
            cau15 = P5
    elif has_keyword(P3, KEYWORDS_CAU14):
        cau14 = P3
        cau14 = f"{cau14}, {P4}"
        cau13 = f"{cau13}, {P2}"
        cau15 = P5
    else:
        cau13 = f"{cau13}, {P2}"
        if has_keyword(P3, KEYWORDS_CAU15):
            cau15 = P3
            cau15 = f"{cau15}, {P4}" if P4 else cau15
            cau15 = f"{cau15}, {P5}" if P5 else cau15
        else:
            cau14 = P3
            cau15 = P4
            cau15 = f"{cau15}, {P5}" if P5 else cau15
    
    return cau13, cau14, cau15, cau16

def split_after_null_by_rules(after_null_list, row_number=None):
    if not after_null_list:
        return ['', '', '', ''], None
    
    original_text = ','.join(after_null_list)
    
    parts_level1 = split_by_condition_1(original_text)
    if len(parts_level1) == 4:
        return parts_level1[:4], None
    if len(parts_level1) == 3:
        success, new_parts = try_create_4th_column(parts_level1)
        if success:
            return new_parts[:4], None
    
    parts_level2 = split_by_condition_2(original_text)
    if len(parts_level2) == 4:
        return parts_level2[:4], None
    if len(parts_level2) == 3:
        success, new_parts = try_create_4th_column(parts_level2)
        if success:
            return new_parts[:4], None
    
    parts_level3 = split_by_condition_3(original_text)
    if len(parts_level3) == 4:
        return parts_level3[:4], None
    if len(parts_level3) == 3:
        success, new_parts = try_create_4th_column(parts_level3)
        if success:
            return new_parts[:4], None
    
    best_parts = parts_level3 if len(parts_level3) >= len(parts_level2) else parts_level2
    best_parts = best_parts if len(best_parts) >= len(parts_level1) else parts_level1
    
    if len(best_parts) >= 2:
        cau13, cau14, cau15, cau16 = classify_by_position_and_keywords(best_parts)
        if cau13 or cau14 or cau15 or cau16:
            return [cau13, cau14, cau15, cau16], None
    
    error_info = {'row_number': row_number, 'original_after_null': original_text}
    return [original_text, '', '', ''], error_info

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
            split_result, error = split_after_null_by_rules(after_null, row_number)
            if len(split_result) >= 4:
                cau13 = split_result[0]
                cau14 = split_result[1]
                cau15 = split_result[2]
                cau16 = split_result[3]
            if error:
                split_errors.append(error)
        
        result = {
            'Lop': lop, 'MaSV': ma_sv, 'HoDem': ho_dem, 'Ten': ten,
            'NgaySinh': ngay_sinh, 'MaHP': ma_hp, 'TenHP': ten_hp,
            'MaGV': ma_gv, 'HoDemGV': ho_dem_gv, 'TenGV': ten_gv,
            'LopHP': lop_hp, 'CauHoi': cau_hoi, 'GiaTri': gia_tri,
            'NULL': null_value, 'Cau13': cau13, 'Cau14': cau14,
            'Cau15': cau15, 'Cau16': cau16
        }
        return result, None, split_errors
    except Exception as e:
        return None, str(e), []

def read_csv_manual(filename):
    rows = []
    with open(filename, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = line.split(',')
            row = [col.strip() for col in row]
            rows.append(row)
    return rows, []

# ========== MAIN ==========

def main():
    try:
        blob_service = BlobServiceClient.from_connection_string(CONNECTION_STRING)
    except Exception as e:
        print(f"Lỗi kết nối blob: {e}")
        sys.exit(1)
    
    download_from_blob(blob_service)
    
    rows, _ = read_csv_manual(SURVEY_FILE)
    if not rows:
        print("Không có dữ liệu")
        sys.exit(1)
    
    processed_rows = []
    for idx, row in enumerate(rows, 1):
        result, _, _ = process_row(row, idx)
        if result:
            processed_rows.append(result)
    
    result_df = pd.DataFrame(processed_rows)
    
    print(f"Đã xử lý: {len(processed_rows)}/{len(rows)} dòng")
    
    if len(processed_rows) > 0:
        # Tên file không có datetime
        output_filename = f"{FILE_NAME}_processed.csv"
        output_path = f"{SEMESTER}/{output_filename}"
        
        if upload_to_blob(blob_service, result_df, output_path):
            print(f"Đã upload: {output_path}")
            
            # Chèn database
            loader = DatabaseLoader(blob_service, SEMESTER, SURVEY_FILE)
            loader.insert(processed_rows)
        else:
            sys.exit(1)
    else:
        print("Không có dòng nào được xử lý")
        sys.exit(1)

if __name__ == "__main__":
    main()
