"""
Transform: Tính điểm cho các câu trả lời
"""
from typing import Optional
from ..config.settings import Settings

class ScoreCalculator:
    """Tính weighted score cho câu trả lời"""
    
    @staticmethod
    def calculate(text: str, column_name: str) -> Optional[float]:
        """
        Tính điểm cho một câu trả lời
        """
        if not text or not isinstance(text, str):
            return None
        
        text_lower = text.lower()
        total_score = 0.0
        weights = Settings.ALL_WEIGHTS.get(column_name, {})
        
        for keyword, weight in weights.items():
            if keyword in text_lower:
                total_score += weight
        
        return total_score if total_score > 0 else None
