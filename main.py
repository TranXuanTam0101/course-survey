#!/usr/bin/env python3
"""
Survey ETL Pipeline - Main Entry Point
Thời gian xử lý mục tiêu: < 1 phút
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from src.config.settings import Settings
from src.utils.logger import setup_logger
from src.extract.azure_blob import AzureBlobExtractor
from src.extract.master_data import MasterDataExtractor
from src.transform.parser import SurveyParser
from src.transform.transformer import DataTransformer
from src.load.db_connection import DBConnection
from src.load.dimension_loader import DimensionLoader
from src.load.fact_loader import FactLoader

def main():
    """Main ETL orchestration"""
    
    # Lấy tham số từ environment
    semester = sys.argv[1] if len(sys.argv) > 1 else Settings.SEMESTER
    survey_file = sys.argv[2] if len(sys.argv) > 2 else Settings.SURVEY_FILE
    
    if not semester or not survey_file:
        print("ERROR: Thiếu SEMESTER hoặc SURVEY_FILE")
        print("Usage: python main.py <semester> <survey_file>")
        sys.exit(1)
    
    # Setup logging
    logger = setup_logger(semester, survey_file)
    
    # Start timer
    start_time = time.time()
    
    logger.info("=" * 60)
    logger.info("SURVEY ETL PIPELINE - BẮT ĐẦU")
    logger.info(f"Semester: {semester}")
    logger.info(f"File: {survey_file}")
    logger.info("=" * 60)
    
    try:
        # Validate config
        Settings.validate()
        
        # ========== 1. EXTRACT ==========
        logger.info("[EXTRACT] Đang đọc dữ liệu...")
        extract_start = time.time()
        
        # Khởi tạo extractor
        blob_extractor = AzureBlobExtractor(Settings.CONNECTION_STRING)
        master_extractor = MasterDataExtractor(blob_extractor)
        
        # Extract survey file
        survey_content = blob_extractor.extract_survey_file(semester, survey_file)
        
        # Extract master data
        master_data = master_extractor.extract_all(semester)
        
        extract_time = time.time() - extract_start
        logger.success(f"[EXTRACT] Hoàn tất trong {extract_time:.2f}s")
        
        # ========== 2. TRANSFORM ==========
        logger.info("[TRANSFORM] Đang xử lý dữ liệu...")
        transform_start = time.time()
        
        # Parse survey
        parser = SurveyParser()
        df = parser.parse(survey_content)
        
        if df.empty:
            logger.error("Không có dữ liệu sau khi parse!")
            return
        
        # Transform
        transformer = DataTransformer(semester, survey_file)
        dimensions, fact_df = transformer.transform(df, master_data)
        
        transform_time = time.time() - transform_start
        logger.success(f"[TRANSFORM] Hoàn tất trong {transform_time:.2f}s")
        logger.info(f"  - Số dòng: {len(df)}")
        logger.info(f"  - Số sinh viên CTS: {df['IsCTS'].sum()}/{len(df)}")
        
        # ========== 3. LOAD ==========
        logger.info("[LOAD] Đang ghi vào database...")
        load_start = time.time()
        
        # Khởi tạo loaders
        db = DBConnection()
        dim_loader = DimensionLoader(db)
        fact_loader = FactLoader(db)
        
        # Load dimensions
        dim_loader.load_all(dimensions, transformer.ma_hoc_ky)
        
        # Load fact
        fact_loader.load(fact_df)
        
        load_time = time.time() - load_start
        logger.success(f"[LOAD] Hoàn tất trong {load_time:.2f}s")
        
        # ========== TỔNG KẾT ==========
        total_time = time.time() - start_time
        logger.info("=" * 60)
        logger.success(f"✅ PIPELINE HOÀN TẤT - Tổng thời gian: {total_time:.2f}s")
        
        # Cảnh báo nếu > 1 phút
        if total_time > 60:
            logger.warning(f"⚠️  Thời gian xử lý > 1 phút ({total_time:.0f}s)")
        else:
            logger.success(f"🎯 Đạt mục tiêu < 1 phút!")
        
        logger.info("=" * 60)
        
        # In breakdown thời gian
        logger.info("Breakdown thời gian:")
        logger.info(f"  - Extract:  {extract_time:.2f}s ({extract_time/total_time*100:.1f}%)")
        logger.info(f"  - Transform: {transform_time:.2f}s ({transform_time/total_time*100:.1f}%)")
        logger.info(f"  - Load:      {load_time:.2f}s ({load_time/total_time*100:.1f}%)")
        
    except Exception as e:
        logger.error(f"❌ Pipeline thất bại: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
