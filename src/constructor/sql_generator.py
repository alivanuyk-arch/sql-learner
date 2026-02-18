from typing import Dict, Any
from .utils import TextProcessor
from .models import Pattern
import logging

logger = logging.getLogger(__name__)

class SQLGenerator:
    """Генерация SQL из шаблонов и fallback"""
    
    def __init__(self):
        self.text_processor = TextProcessor()
    
    def fill_template(self, pattern: Pattern, query: str) -> str:
        """Заполняет шаблон параметрами"""
        sql = pattern.template
        params = self.text_processor.extract_parameters(query)
        
        # Заменяем плейсхолдеры
        for placeholder, value in params.items():
            if placeholder in sql:
                sql = sql.replace(placeholder, str(value))
        
        return sql
    
    def generate_fallback(self, query: str) -> str:
        """Генерация fallback SQL запроса"""
        query_lower = query.lower()
        
        # Пытаемся понять, что хочет пользователь
        if any(word in query_lower for word in ['сколько', 'количество', 'число']):
            # Подсчет
            if 'видео' in query_lower:
                return "SELECT COUNT(*) FROM videos"
            elif 'снимк' in query_lower or 'снапшот' in query_lower:
                return "SELECT COUNT(*) FROM video_snapshots"
            else:
                return "SELECT COUNT(*) FROM videos"
        
        elif any(word in query_lower for word in ['сумма', 'суммарн', 'итого']):
            # Сумма
            if 'просмотр' in query_lower:
                return "SELECT SUM(views_count) FROM videos"
            elif 'лайк' in query_lower:
                return "SELECT SUM(likes_count) FROM videos"
            elif 'комментар' in query_lower:
                return "SELECT SUM(comments_count) FROM videos"
        
        # Дефолтный запрос
        return "SELECT * FROM videos LIMIT 10"
    
    def compute_diff(self, sql1: str, sql2: str) -> Dict[str, Any]:
        """Вычисление различий между SQL запросами"""
        import re
        
        return {
            'same_structure': sql1.strip().lower() == sql2.strip().lower(),
            'length_diff': len(sql2) - len(sql1),
            'tables_changed': len(
                set(re.findall(r'FROM\s+(\w+)', sql2, re.IGNORECASE)) - 
                set(re.findall(r'FROM\s+(\w+)', sql1, re.IGNORECASE))
            )
        }