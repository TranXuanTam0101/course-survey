"""
Extract: Đọc dữ liệu từ Azure Blob Storage
"""
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from loguru import logger

class AzureBlobExtractor:
    """Extract dữ liệu từ Azure Blob Storage"""
    
    def __init__(self, connection_string: str):
        self.blob_service = BlobServiceClient.from_connection_string(connection_string)
    
    def extract_survey_file(self, semester: str, filename: str) -> str:
        """
        Đọc file survey CSV từ container rawdata
        Returns: string content của file
        """
        logger.info(f"Đang đọc file survey: {semester}/{filename}")
        
        blob_path = f"{semester}/{filename}"
        blob_client = self.blob_service.get_container_client("rawdata").get_blob_client(blob_path)
        
        data = blob_client.download_blob().readall()
        content = data.decode('utf-8-sig')
        
        logger.success(f"Đã đọc file survey: {len(content)} bytes")
        return content
    
    def extract_csv_file(self, container: str, blob_path: str) -> pd.DataFrame:
        """
        Đọc file CSV bất kỳ từ Azure Blob
        """
        try:
            blob_client = self.blob_service.get_container_client(container).get_blob_client(blob_path)
            
            if not blob_client.exists():
                logger.warning(f"File không tồn tại: {container}/{blob_path}")
                return pd.DataFrame()
            
            data = blob_client.download_blob().readall()
            content = data.decode('utf-8')
            df = pd.read_csv(io.StringIO(content))
            
            logger.debug(f"Đã đọc {len(df)} dòng từ {blob_path}")
            return df
            
        except Exception as e:
            logger.error(f"Lỗi đọc file {blob_path}: {e}")
            return pd.DataFrame()
