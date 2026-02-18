import json
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class DataStorage:
    """Сохранение и загрузка данных"""
    
    def __init__(self, cache_file: Path, patterns_file: Path, corrections_file: Path):
        self.cache_file = cache_file
        self.patterns_file = patterns_file
        self.corrections_file = corrections_file
    
    def save_cache(self, cache_data: Dict):
        """Сохраняет кэш"""
        try:
            data = {
                'exact_cache': cache_data,
                'updated_at': datetime.now().isoformat(),
                'cache_size': len(cache_data)
            }
            
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Cache saved: {len(cache_data)} items")
        except Exception as e:
            logger.error(f"Error saving cache: {e}")
    
    def load_cache(self) -> Dict:
        """Загружает кэш"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('exact_cache', {})
        except Exception as e:
            logger.error(f"Error loading cache: {e}")
        return {}
    
    def save_patterns(self, patterns_data: Dict):
        """Сохраняет паттерны"""
        try:
            data = {
                'patterns': patterns_data,
                'updated_at': datetime.now().isoformat(),
                'total_patterns': len(patterns_data)
            }
            
            with open(self.patterns_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Patterns saved: {len(patterns_data)} patterns")
        except Exception as e:
            logger.error(f"Error saving patterns: {e}")
    
    def load_patterns(self) -> Dict:
        """Загружает паттерны"""
        try:
            if self.patterns_file.exists():
                with open(self.patterns_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('patterns', {})
        except Exception as e:
            logger.error(f"Error loading patterns: {e}")
        return {}
    
    def save_corrections(self, corrections: List[Dict]):
        """Сохраняет исправления"""
        try:
            data = {
                'corrections': corrections[-100:],  # Последние 100 исправлений
                'total_corrections': len(corrections),
                'updated_at': datetime.now().isoformat()
            }
            
            with open(self.corrections_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Corrections saved: {len(corrections)} total")
        except Exception as e:
            logger.error(f"Error saving corrections: {e}")
    
    def load_corrections(self) -> List[Dict]:
        """Загружает исправления"""
        try:
            if self.corrections_file.exists():
                with open(self.corrections_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('corrections', [])
        except Exception as e:
            logger.error(f"Error loading corrections: {e}")
        return []