"""
core.py - Главный конструктор запросов
Логика: кэш → шаблоны → LLM
"""

import logging
from typing import Dict, Optional, Any, Tuple
from pathlib import Path
from datetime import datetime

from .cache import ExactCache
from .pattern_matcher import PatternMatcher
from .sql_generator import SQLGenerator
from .storage import DataStorage
from .models import Pattern, ConstructorStats
from .utils import TextProcessor

logger = logging.getLogger(__name__)

class QueryConstructor:
    """Самообучающийся конструктор SQL запросов"""
    
    def __init__(self, llm_client=None, db_manager=None, config=None):
        self.llm = llm_client
        self.db = db_manager
        self.config = config
        
        # Инициализация компонентов
        self.text_processor = TextProcessor()
        self.cache = ExactCache(max_size=1000)
        self.pattern_matcher = PatternMatcher()
        self.sql_generator = SQLGenerator()
        
        # Статистика
        self.stats = {
            'total_queries': 0,
            'exact_hits': 0,
            'pattern_hits': 0,
            'llm_calls': 0,
            'corrections': 0,
            'auto_learned': 0
        }
        
        # Лог исправлений
        self.corrections_log = []
        
        # Хранилище данных
        if config and hasattr(config, 'CACHE_FILE'):
            self.storage = DataStorage(
                cache_file=Path(config.CACHE_FILE),
                patterns_file=Path(config.PATTERNS_FILE),
                corrections_file=Path(config.CORRECTIONS_FILE)
            )
            self._load_data()
        
        logger.info(f"Конструктор инициализирован. Паттернов: {len(self.pattern_matcher.patterns)}")
    
    async def process_query(self, user_query: str, user_id: str = None) -> Dict:
        """
        Обработка запроса: кэш → шаблоны → LLM
        """
        self.stats['total_queries'] += 1
        logger.info(f"Processing query: '{user_query}'")
        
        # 1. ТОЧНЫЙ КЭШ
        cached_sql = self.cache.get(user_query)
        if cached_sql:
            logger.info(f"Exact cache hit")
            self.stats['exact_hits'] += 1
            return {
                'sql': cached_sql,
                'source': 'cache',
                'success': True,
                'needs_llm': False
            }
        
        # 2. ПОИСК ШАБЛОНОВ
        words = self.text_processor.extract_words(user_query)
        pattern, score = self.pattern_matcher.find_best_match_with_score(words)
        
        if pattern and score >= 0.7:  # Хорошее совпадение
            logger.info(f"Pattern found with score: {score}")
            self.stats['pattern_hits'] += 1
            
            sql = self.sql_generator.fill_template(pattern, user_query)
            
            # Если шаблон заполнен - в кэш
            if '{' not in sql:
                self.cache.set(user_query, sql)
            
            return {
                'sql': sql,
                'source': 'pattern',
                'success': True,
                'needs_llm': False,
                'confidence': score
            }
        
        # 3. НИЧЕГО НЕ НАШЛИ - НУЖЕН LLM
        logger.info(f"No pattern found, need LLM")
        self.stats['llm_calls'] += 1
        
        return {
            'sql': None,
            'source': 'none',
            'success': False,
            'needs_llm': True,
            'user_query': user_query,
            'words': list(words) if words else []
        }
    
    async def learn_from_correction(self, original_query: str, 
                                  llm_sql: str, 
                                  corrected_sql: str,
                                  user_id: int = None):
        """
        Обучение на исправлении пользователя
        """
        self.stats['corrections'] += 1
        
        # Сохраняем точный запрос в кэш
        self.cache.set(original_query, corrected_sql)
        
        # Создаем новый шаблон
        words = self.text_processor.extract_words(original_query)
        generalized_sql = self.text_processor.generalize_sql(corrected_sql)
        
        pattern = Pattern(
            words=list(words),
            template=generalized_sql,
            source='correction',
            examples=[original_query]
        )
        self.pattern_matcher.add_pattern(pattern)
        
        # Логируем исправление
        self.corrections_log.append({
            'timestamp': datetime.now().isoformat(),
            'query': original_query,
            'llm_sql': llm_sql,
            'corrected': corrected_sql,
            'user_id': user_id
        })
        
        # Сохраняем данные
        self._save_data()
        
        logger.info(f"Learned from correction. Patterns: {len(self.pattern_matcher.patterns)}")
    
    def get_stats(self) -> ConstructorStats:
        """Статистика работы"""
        total = self.stats['total_queries'] or 1
        hits = self.stats['exact_hits'] + self.stats['pattern_hits']
        
        return ConstructorStats(
            total_patterns=len(self.pattern_matcher.patterns),
            exact_hits=self.stats['exact_hits'],
            pattern_hits=self.stats['pattern_hits'],
            llm_calls=self.stats['llm_calls'],
            corrections=self.stats['corrections'],
            auto_learned=self.stats['auto_learned'],
            learning_rate=hits / total
        )
    
    def _load_data(self):
        """Загрузка сохранённых данных"""
        if not hasattr(self, 'storage') or not self.storage:
            return
        
        cache_data = self.storage.load_cache()
        self.cache.from_dict(cache_data)
        
        patterns_data = self.storage.load_patterns()
        self.pattern_matcher.from_dict(patterns_data)
        
        self.corrections_log = self.storage.load_corrections()
    
    def _save_data(self):
        """Сохранение данных"""
        if hasattr(self, 'storage') and self.storage:
            self.storage.save_cache(self.cache.to_dict())
            self.storage.save_patterns(self.pattern_matcher.to_dict())
            self.storage.save_corrections(self.corrections_log)