#!/usr/bin/env python3
"""
test_full_pipeline.py - Тестирование всего пайплайна:
data/ → PostgreSQL → детектор схемы → конструктор → LLM (заглушка)
"""

import os
import sys
import json
import asyncio
from pathlib import Path

# Добавляем пути
sys.path.append(str(Path(__file__).parent))
sys.path.append(str(Path(__file__).parent / 'scripts'))

# Импорты
from scripts.load_to_postgres import load_all_to_postgres
from scripts.universal_schema_detector import get_full_schema
from src.constructor.core import QueryConstructor
from dotenv import load_dotenv

# Заглушка для LLM
class MockLLM:
    """Заглушка LLM для тестов"""
    async def generate_sql(self, query: str, schema: dict) -> str:
        print(f"  🤖 LLM заглушка: получил запрос '{query}'")
        print(f"  📋 Схема: {len(schema.get('tables', {}))} таблиц")
        
        # Простая заглушка - возвращает тестовый SQL
        if 'сколько' in query.lower():
            return "SELECT COUNT(*) FROM videos"
        elif 'сумма' in query.lower():
            return "SELECT SUM(views_count) FROM videos"
        else:
            return "SELECT * FROM videos LIMIT 10"

# Конфиг БД из .env
load_dotenv()
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '123'),
    'database': os.getenv('DB_NAME', 'video_analytics')
}

class MockConfig:
    """Заглушка конфига для конструктора"""
    CACHE_FILE = 'storage/test_cache.json'
    PATTERNS_FILE = 'storage/test_patterns.json'
    CORRECTIONS_FILE = 'storage/test_corrections.json'

async def test_pipeline():
    """Тестирование всего пайплайна"""
    
    print("="*70)
    print("🧪 ТЕСТИРОВАНИЕ ПОЛНОГО ПАЙПЛАЙНА")
    print("="*70)
    
    # ШАГ 1: Загрузка данных из /data в PostgreSQL
    print("\n📦 ШАГ 1: Загрузка данных в PostgreSQL")
    print("-"*50)
    
    if not os.path.exists('data'):
        os.makedirs('data')
        print("📁 Создана папка data/")
    
    # Проверяем наличие файлов в data
    data_files = list(Path('data').glob('*'))
    if not data_files:
        print("⚠️ Папка data пуста. Создаю тестовый файл...")
        
        # Создаем тестовый JSON
        test_data = {
            "videos": [
                {"id": 1, "title": "Видео 1", "views": 15000, "date": "2025-11-15"},
                {"id": 2, "title": "Видео 2", "views": 8000, "date": "2025-11-16"}
            ]
        }
        
        with open('data/test_videos.json', 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)
        print("✅ Создан тестовый файл: data/test_videos.json")
    
    # Загружаем все файлы в БД
    load_result = load_all_to_postgres('data')
    print(f"\n📊 Результат загрузки: {load_result}")
    
    # ШАГ 2: Получение схемы из БД
    print("\n📋 ШАГ 2: Получение схемы из PostgreSQL")
    print("-"*50)
    
    try:
        schema = get_full_schema(DB_CONFIG)
        print(f"✅ Получена схема: {len(schema.get('tables', {}))} таблиц")
        
        for table_name, table_info in schema.get('tables', {}).items():
            print(f"  - {table_name}: {len(table_info)} колонок")
    except Exception as e:
        print(f"❌ Ошибка получения схемы: {e}")
        schema = {"tables": {}, "aliases": {}}
    
    # ШАГ 3: Инициализация конструктора
    print("\n🔧 ШАГ 3: Инициализация конструктора")
    print("-"*50)
    
    mock_llm = MockLLM()
    mock_config = MockConfig()
    
    constructor = QueryConstructor(
        llm_client=mock_llm,
        db_manager=None,  # пока без БД
        config=mock_config
    )
    
    # Передаем схему в конструктор
    await constructor.initialize_with_schema(schema)
    print(f"✅ Конструктор инициализирован. Паттернов: {len(constructor.pattern_matcher.patterns)}")
    
    # ШАГ 4: Тестовые запросы
    print("\n🔍 ШАГ 4: Тестирование запросов")
    print("-"*50)
    
    test_queries = [
        "сколько видео",
        "сумма просмотров",
        "видео с 1000 просмотров",
        "последние видео"
    ]
    
    for i, query in enumerate(test_queries, 1):
        print(f"\n📝 Запрос {i}: '{query}'")
        
        result = await constructor.process_query(query)
        
        print(f"  Источник: {result.get('source', '?')}")
        print(f"  Успех: {result.get('success', False)}")
        print(f"  Нужен LLM: {result.get('needs_llm', False)}")
        
        if result.get('sql'):
            print(f"  SQL: {result['sql']}")
        else:
            print(f"  SQL не найден")
        
        # Если нужен LLM - вызываем заглушку
        if result.get('needs_llm'):
            print(f"  → Вызов LLM...")
            llm_sql = await mock_llm.generate_sql(query, schema)
            print(f"  → LLM вернул: {llm_sql}")
            
            # Имитация подтверждения пользователя
            print(f"  → Обучаем на успехе")
            await constructor.learn_from_success(query, llm_sql)
    
    # ШАГ 5: Статистика
    print("\n📊 ШАГ 5: Статистика работы")
    print("-"*50)
    
    stats = constructor.get_stats()
    print(f"  Всего запросов: {stats.total_queries}")
    print(f"  Точных совпадений: {stats.exact_hits}")
    print(f"  Паттернов: {stats.total_patterns}")
    print(f"  Вызовов LLM: {stats.llm_calls}")
    print(f"  Исправлений: {stats.corrections}")
    print(f"  Коэффициент обучения: {stats.learning_rate:.2%}")
    
    # ШАГ 6: Проверка кэша
    print("\n⚡ ШАГ 6: Проверка кэша (повторный запрос)")
    print("-"*50)
    
    if test_queries:
        repeat_query = test_queries[0]
        print(f"📝 Повторный запрос: '{repeat_query}'")
        
        result = await constructor.process_query(repeat_query)
        
        print(f"  Источник: {result.get('source', '?')}")
        if result.get('from_cache'):
            print(f"  ✅ ВЗЯТО ИЗ КЭША!")
        print(f"  SQL: {result.get('sql', 'None')}")
    
    print("\n" + "="*70)
    print("✅ ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("="*70)

if __name__ == "__main__":
    asyncio.run(test_pipeline())