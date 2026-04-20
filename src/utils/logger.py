"""
Cấu hình logging với loguru
"""
import sys
from pathlib import Path
from datetime import datetime
from loguru import logger

def setup_logger(semester: str, survey_file: str):
    """Setup structured logging"""
    
    # Tạo thư mục logs nếu chưa có
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Xóa handler mặc định
    logger.remove()
    
    # Handler cho console
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    # Handler cho file
    log_filename = log_dir / f"etl_{semester.replace('-', '_')}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.add(
        str(log_filename),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",
        rotation="50 MB",
        retention="7 days"
    )
    
    return logger
