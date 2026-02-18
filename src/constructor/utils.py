import re
from typing import Set, Dict, Any, List

class TextProcessor:
    """Обработка текста - выделение слов и параметров"""
    
    STOP_WORDS = {'и', 'в', 'с', 'по', 'за', 'у', 'о', 'от', 'есть', 'всего', 'x', 'id', 'для', 'на'}
    MONTH_MAP = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    }
    
    @classmethod
    def extract_words(cls, query: str) -> Set[str]:
        """Извлекаем нормализованные слова"""
        query_lower = query.lower()
        
        # Заменяем знаки препинания на пробелы
        for char in '?.,!;:()[]{}"\'«»':
            query_lower = query_lower.replace(char, ' ')
        
        # Сохраняем специальные идентификаторы
        query_lower = re.sub(r'[a-f0-9]{32}', ' IDCREATOR ', query_lower)
        query_lower = re.sub(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}', ' IDVIDEO ', query_lower)
        
        # Удаляем даты и числа
        query_lower = re.sub(r'\d{4}-\d{2}-\d{2}', ' ', query_lower)
        query_lower = re.sub(r'\d{1,2}\s+\w+\s+\d{4}', ' ', query_lower)
        query_lower = re.sub(r'\b\d{1,4}\b', ' ', query_lower)
        
        # Разбиваем на слова и фильтруем
        words = query_lower.split()
        return {word for word in words if word not in cls.STOP_WORDS and len(word) >= 2}
    
    @classmethod
    def extract_parameters(cls, query: str) -> Dict[str, Any]:
        """Извлечение параметров из запроса"""
        params = {}
        query_lower = query.lower()
        
        # Ищем даты
        date_patterns = [
            (r'(\d{1,2})\s+(\w+)\s+(\d{4})', cls._parse_russian_date),
            (r'(\d{4}-\d{2}-\d{2})', lambda m: f"'{m.group(1)}'"),
        ]
        
        for pattern, converter in date_patterns:
            match = re.search(pattern, query_lower)
            if match:
                params['{DATE}'] = converter(match)
                break
        
        # Ищем числа
        numbers = re.findall(r'\b(\d+)\b', query)
        if numbers:
            params['{NUMBER}'] = numbers[0]
            for i, num in enumerate(numbers[:3], 1):
                params[f'{{NUMBER{i}}}'] = num
        
        # Ищем ID
        id_patterns = [
            (r'[a-f0-9]{32}', '{ID}'),
            (r'креатор[ауе]?\s+([a-f0-9]{32})', '{CREATOR_ID}'),
            (r'видео\s+([a-f0-9\-]{36})', '{VIDEO_ID}'),
        ]
        
        for pattern, placeholder in id_patterns:
            match = re.search(pattern, query_lower)
            if match:
                params[placeholder] = f"'{match.group(0)}'"
                break
        
        return params
    
    @classmethod
    def _parse_russian_date(cls, match):
        """Парсит русскую дату вида '15 марта 2024'"""
        day, month_name, year = match.groups()
        month = cls.MONTH_MAP.get(month_name.lower(), 1)
        return f"'{year}-{month:02d}-{int(day):02d}'"
    
    @classmethod
    def generalize_sql(cls, sql: str) -> str:
        """Создаёт шаблон из конкретного SQL"""
        template = sql
        
        # Заменяем конкретные значения на плейсхолдеры
        template = re.sub(r"'[^']*'", "'{VALUE}'", template)  # Строки
        template = re.sub(r'\b\d+\b', '{NUMBER}', template)   # Числа
        
        # Сохраняем специальные конструкции
        template = re.sub(r"'\d{4}-\d{2}-\d{2}'", "'{DATE}'", template)
        template = re.sub(r"'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'", "'{TIMESTAMP}'", template)
        
        return template