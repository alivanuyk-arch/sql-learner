#!/usr/bin/env python3
"""
test_load_to_postgres.py - Тестирование загрузчика данных в PostgreSQL
"""

import os
import sys
import json
import tempfile
from pathlib import Path

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent.parent))

# Импортируем тестируемый модуль
from load_to_postgres import load_csv, load_json, load_sql, load_excel, DB_CONFIG

def create_test_csv():
    """Создает тестовый CSV файл"""
    content = """id,name,age,score
1,Иван,30,85.5
2,Мария,25,92.3
3,Петр,35,78.0
"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(content)
        return f.name

def create_test_json():
    """Создает тестовый JSON файл"""
    data = {
        "users": [
            {"id": 1, "name": "Иван", "age": 30, "active": True},
            {"id": 2, "name": "Мария", "age": 25, "active": False},
            {"id": 3, "name": "Петр", "age": 35, "active": True}
        ]
    }
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False, encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
        return f.name

def create_test_sql():
    """Создает тестовый SQL файл"""
    content = """
    CREATE TABLE IF NOT EXISTS test_sql (
        id INTEGER,
        value TEXT
    );
    INSERT INTO test_sql VALUES (1, 'test1');
    INSERT INTO test_sql VALUES (2, 'test2');
    """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False, encoding='utf-8') as f:
        f.write(content)
        return f.name

def test_csv_loading():
    """Тест загрузки CSV"""
    print("\n📄 ТЕСТ CSV:")
    csv_file = create_test_csv()
    
    try:
        result = load_csv(Path(csv_file))
        
        if result.get('status') == 'ok':
            print(f"  ✅ Успешно: {result['rows']} строк в таблицу {result['table']}")
        else:
            print(f"  ❌ Ошибка: {result.get('error', 'неизвестная ошибка')}")
        
        return result
    finally:
        os.unlink(csv_file)

def test_json_loading():
    """Тест загрузки JSON"""
    print("\n📄 ТЕСТ JSON:")
    json_file = create_test_json()
    
    try:
        result = load_json(Path(json_file))
        
        if result.get('status') == 'ok':
            print(f"  ✅ Успешно: {result['rows']} строк в таблицу {result['table']}")
        else:
            print(f"  ❌ Ошибка: {result.get('error', 'неизвестная ошибка')}")
        
        return result
    finally:
        os.unlink(json_file)

def test_sql_loading():
    """Тест загрузки SQL"""
    print("\n📄 ТЕСТ SQL:")
    sql_file = create_test_sql()
    
    try:
        result = load_sql(Path(sql_file))
        
        if result.get('status') == 'ok':
            print(f"  ✅ Успешно: {result.get('rows', 0)} строк затронуто")
        else:
            print(f"  ❌ Ошибка: {result.get('error', 'неизвестная ошибка')}")
        
        return result
    finally:
        os.unlink(sql_file)

def test_excel_loading():
    """Тест загрузки Excel (если есть pandas)"""
    print("\n📄 ТЕСТ EXCEL:")
    
    try:
        import pandas as pd
        import openpyxl
        
        # Создаем тестовый Excel
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Иван', 'Мария', 'Петр'],
            'value': [100, 200, 300]
        })
        
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as f:
            excel_file = f.name
            df.to_excel(excel_file, index=False)
        
        result = load_excel(Path(excel_file))
        
        if result.get('status') == 'ok':
            print(f"  ✅ Успешно: {result['rows']} строк в таблицу {result['table']}")
        elif result.get('status') == 'skipped':
            print(f"  ⏭️  Пропущено: {result.get('reason', 'неизвестная причина')}")
        else:
            print(f"  ❌ Ошибка: {result.get('error', 'неизвестная ошибка')}")
        
        os.unlink(excel_file)
        return result
        
    except ImportError:
        print("  ⏭️  Excel тест пропущен (нет pandas/openpyxl)")
        return {'status': 'skipped'}

def test_connection():
    """Тест подключения к БД"""
    print("\n🔌 ТЕСТ ПОДКЛЮЧЕНИЯ К БД:")
    
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("  ✅ Подключение успешно")
        return True
    except Exception as e:
        print(f"  ❌ Ошибка подключения: {e}")
        return False

def cleanup_test_tables():
    """Очистка тестовых таблиц"""
    print("\n🧹 ОЧИСТКА ТЕСТОВЫХ ТАБЛИЦ:")
    
    try:
        import psycopg2
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        tables = ['test', 'users', 'test_sql']
        for table in tables:
            try:
                cur.execute(f'DROP TABLE IF EXISTS {table} CASCADE')
                print(f"  ✅ Удалена таблица {table}")
            except:
                pass
        
        conn.commit()
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"  ❌ Ошибка очистки: {e}")

def run_all_tests():
    """Запуск всех тестов"""
    print("="*60)
    print("🧪 ТЕСТИРОВАНИЕ ЗАГРУЗЧИКА DATA → POSTGRESQL")
    print("="*60)
    
    # Проверяем подключение
    if not test_connection():
        print("\n❌ Нет подключения к БД. Тесты прерваны.")
        return
    
    # Очищаем перед тестами
    cleanup_test_tables()
    
    # Запускаем тесты
    results = []
    
    print("\n" + "="*60)
    print("🚀 ЗАПУСК ТЕСТОВ")
    print("="*60)
    
    tests = [
        ("CSV", test_csv_loading),
        ("JSON", test_json_loading),
        ("SQL", test_sql_loading),
        ("Excel", test_excel_loading)
    ]
    
    for name, test_func in tests:
        try:
            result = test_func()
            results.append({
                'test': name,
                'status': result.get('status', 'error'),
                'rows': result.get('rows', 0)
            })
        except Exception as e:
            print(f"  💥 Ошибка теста {name}: {e}")
            results.append({
                'test': name,
                'status': 'error',
                'error': str(e)
            })
    
    # Итоги
    print("\n" + "="*60)
    print("📊 ИТОГИ ТЕСТИРОВАНИЯ:")
    
    success = sum(1 for r in results if r['status'] == 'ok')
    skipped = sum(1 for r in results if r['status'] == 'skipped')
    failed = sum(1 for r in results if r['status'] == 'error')
    
    for r in results:
        status_icon = "✅" if r['status'] == 'ok' else "⏭️" if r['status'] == 'skipped' else "❌"
        print(f"{status_icon} {r['test']}: {r['status']}")
    
    print("-"*40)
    print(f"✅ Успешно: {success}")
    print(f"⏭️  Пропущено: {skipped}")
    print(f"❌ Ошибок: {failed}")
    
    # Очищаем после тестов
    cleanup_test_tables()
    
    return results

if __name__ == '__main__':
    run_all_tests()