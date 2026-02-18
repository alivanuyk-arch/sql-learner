from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime

@dataclass
class Pattern:
    """Паттерн SQL запроса"""
    words: List[str]
    template: str
    count: int = 0
    examples: List[str] = field(default_factory=list)
    source: str = 'manual'
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_used: str = field(default_factory=lambda: datetime.now().isoformat())

@dataclass
class CorrectionRecord:
    """Запись об исправлении"""
    original_query: str
    llm_sql: str
    corrected_sql: str
    user_feedback: Optional[str]
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    diff: Dict[str, Any] = field(default_factory=dict)
    user_id: Optional[int] = None

@dataclass
class ConstructorStats:
    """Статистика конструктора"""
    total_patterns: int
    exact_hits: int
    pattern_hits: int
    llm_calls: int
    corrections: int
    auto_learned: int
    learning_rate: float