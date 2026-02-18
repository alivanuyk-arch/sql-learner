from typing import Set, Optional, Dict, List
from collections import defaultdict
from .models import Pattern
import hashlib
import logging

logger = logging.getLogger(__name__)

class PatternMatcher:
    """Поиск похожих паттернов"""
    
    def __init__(self):
        self.patterns: Dict[str, Pattern] = {}
        self.word_index: Dict[str, Set[str]] = defaultdict(set)
        self.matcher_hits = 0
    
    def add_pattern(self, pattern: Pattern) -> str:
        """Добавить паттерн"""
        # Создаём ключ из слов
        sorted_words = sorted(pattern.words)
        key_str = " ".join(sorted_words)
        pattern_key = hashlib.md5(key_str.encode()).hexdigest()[:16]
        
        self.patterns[pattern_key] = pattern
        
        # Индексируем слова
        for word in pattern.words:
            self.word_index[word].add(pattern_key)
        
        return pattern_key
    
    def find_best_match(self, words: Set[str], min_score: float = 0.5) -> Optional[Pattern]:
        """Найти лучший паттерн"""
        if not words:
            return None
        
        best_pattern = None
        best_score = 0
        
        for pattern in self.patterns.values():
            pattern_words = set(pattern.words)
            common = words.intersection(pattern_words)
            
            if not common:
                continue
            
            # Оценка схожести
            coverage = len(common) / len(pattern_words)
            recall = len(common) / len(words) if words else 0
            
            # Комбинированная оценка
            score = (coverage * 0.6) + (recall * 0.4)
            
            if score > best_score and score > min_score:
                best_score = score
                best_pattern = pattern
        
        if best_pattern:
            self.matcher_hits += 1
            best_pattern.count += 1
            best_pattern.last_used = datetime.now().isoformat()
        
        return best_pattern
    
    def remove_unused(self, max_age_days: int = 30, min_uses: int = 3):
        """Удалить неиспользуемые паттерны"""
        from datetime import datetime, timedelta
        
        cutoff = datetime.now() - timedelta(days=max_age_days)
        to_remove = []
        
        for key, pattern in self.patterns.items():
            last_used = datetime.fromisoformat(pattern.last_used)
            if last_used < cutoff and pattern.count < min_uses:
                to_remove.append(key)
        
        for key in to_remove:
            del self.patterns[key]
        
        if to_remove:
            logger.info(f"Removed {len(to_remove)} unused patterns")
    
    def to_dict(self) -> Dict:
        return {
            key: {
                'words': pattern.words,
                'template': pattern.template,
                'count': pattern.count,
                'examples': pattern.examples,
                'source': pattern.source,
                'created_at': pattern.created_at,
                'last_used': pattern.last_used
            }
            for key, pattern in self.patterns.items()
        }
    
    def from_dict(self, data: Dict):
        self.patterns.clear()
        self.word_index.clear()
        
        for key, pattern_data in data.items():
            pattern = Pattern(
                words=pattern_data['words'],
                template=pattern_data['template'],
                count=pattern_data.get('count', 0),
                examples=pattern_data.get('examples', []),
                source=pattern_data.get('source', 'manual'),
                created_at=pattern_data.get('created_at', datetime.now().isoformat()),
                last_used=pattern_data.get('last_used', datetime.now().isoformat())
            )
            self.patterns[key] = pattern
            
            for word in pattern.words:
                self.word_index[word].add(key)