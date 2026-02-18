from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class ExactCache:
    """Кэш точных совпадений вопрос→SQL"""
    
    def __init__(self, max_size: int = 1000):
        self._cache: Dict[str, str] = {}
        self.max_size = max_size
        self.hits = 0
    
    def get(self, question: str) -> Optional[str]:
        """Получить SQL из кэша"""
        sql = self._cache.get(question)
        if sql:
            self.hits += 1
        return sql
    
    def set(self, question: str, sql: str):
        """Сохранить в кэш"""
        if len(self._cache) >= self.max_size:
            # Простая стратегия - удаляем первый (можно улучшить до LRU)
            first_key = next(iter(self._cache))
            del self._cache[first_key]
        
        self._cache[question] = sql
    
    def clear(self):
        """Очистить кэш"""
        self._cache.clear()
        self.hits = 0
    
    def to_dict(self) -> Dict:
        return self._cache.copy()
    
    def from_dict(self, data: Dict):
        self._cache = data
    
    @property
    def size(self) -> int:
        return len(self._cache)