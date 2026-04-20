"""
Tập trung tất cả cấu hình và constants
"""
import os
import re
from pathlib import Path
from dotenv import load_dotenv

# Load .env file
env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

class Settings:
    """Cấu hình tập trung cho toàn bộ pipeline"""
    
    # ========== AZURE CONFIG ==========
    CONNECTION_STRING = os.getenv("CONNECTION_STRING")
    
    # ========== DATABASE CONFIG ==========
    DB_CONFIG = {
        'server': os.getenv("DB_SERVER", "course-survey.database.windows.net"),
        'user': os.getenv("DB_USER", "sqladmin"),
        'password': os.getenv("DB_PASSWORD", "Due@2026"),
        'database': os.getenv("DB_NAME", "course-survey-db"),
        'timeout': 60,  # Giảm timeout để tăng tốc
        'autocommit': False,
        'as_dict': False
    }
    
    # ========== BATCH CONFIG (Tối ưu performance) ==========
    BATCH_SIZE = 5000  # Bulk insert batch size
    PARSE_CHUNK_SIZE = 10000  # Parse file theo chunk
    
    # ========== WEIGHTS CONFIG ==========
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
    
    # ========== REGEX PATTERNS ==========
    DATE_PATTERN = re.compile(r'^\d{2}/\d{2}/\d{4}$')
    MA_GV_PATTERN = re.compile(r'^(\d{7}|TG\d{5}|gvDacThu_TKTH)$')
    LOP_PATTERN = re.compile(r'^(\d{2})K(\d{2})$')
    CTS_PATTERN = re.compile(r'^CTS-', re.IGNORECASE)
    
    @classmethod
    def validate(cls):
        """Validate required environment variables"""
        required = ['CONNECTION_STRING', 'DB_PASSWORD']
        missing = [var for var in required if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing environment variables: {missing}")
