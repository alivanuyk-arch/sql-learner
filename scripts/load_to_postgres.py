#!/usr/bin/env python3
"""
load_to_postgres.py - Универсальный загрузчик данных в PostgreSQL
Поддерживает: .csv, .json, .sql, .xlsx, .xls
Автоматически определяет типы: int, float, date, str
"""

import os
import sys
import json
import csv
import re
from pathlib import Path
from typing import Dict, List, Any, Optional

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent.parent))

import psycopg2
from dotenv import load_dotenv

# Загружаем .env
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Конфиг БД
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'postgres')
}


def detect_type(value: Any) -> str:
    """
    Определяет тип одного значения
    Возвращает: 'int', 'float', 'date', 'str'
    """
    if value is None or value == '':
        return None
    
    # Если уже число
    if isinstance(value, int):
        return 'int'
    if isinstance(value, float):
        return 'float'
    
    # Если строка
    if isinstance(value, str):
        # Пустая строка
        if not value.strip():
            return None
        
        # Целое число
        try:
            int(value)
            return 'int'
        except:
            pass
        
        # Дробное число
        try:
            float(value)
            return 'float'
        except:
            pass
        
        # Дата YYYY-MM-DD
        if re.match(r'^\d{4}-\d{2}-\d{2}$', value):
            return 'date'
        
        # Дата DD.MM.YYYY
        if re.match(r'^\d{2}\.\d{2}\.\d{4}$', value):
            return 'date'
    
    return 'str'


def detect_column_types(rows: List[Dict], columns: List[str]) -> Dict[str, str]:
    """
    Определяет тип каждой колонки по всем значениям
    """
    col_types = {}
    
    for col in columns:
        types_found = set()
        
        for row in rows:
            val = row.get(col)
            val_type = detect_type(val)
            if val_type:
                types_found.add(val_type)
        
        # Приоритет типов (от более строгого к менее)
        if 'int' in types_found and len(types_found) == 1:
            col_types[col] = 'int'
        elif 'float' in types_found:
            col_types[col] = 'float'
        elif 'date' in types_found:
            col_types[col] = 'date'
        else:
            col_types[col] = 'str'
    
    return col_types


def pg_type(our_type: str) -> str:
    """Конвертирует наш тип в PostgreSQL тип"""
    mapping = {
        'int': 'INTEGER',
        'float': 'FLOAT',
        'date': 'DATE',
        'str': 'TEXT'
    }
    return mapping.get(our_type, 'TEXT')


def create_table(table_name: str, columns: List[str], col_types: Dict[str, str]) -> None:
    """
    Создает таблицу в PostgreSQL с правильными типами
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Формируем CREATE TABLE
    col_defs = [f'"{col}" {pg_type(col_types[col])}' for col in columns]
    create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)})'
    
    cur.execute(create_sql)
    cur.execute(f'TRUNCATE {table_name}')
    
    conn.commit()
    cur.close()
    conn.close()


def insert_data(table_name: str, rows: List[Dict], columns: List[str]) -> int:
    """
    Вставляет данные в таблицу
    Возвращает количество вставленных строк
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Подготавливаем INSERT
    placeholders = ', '.join(['%s'] * len(columns))
    cols = ', '.join([f'"{col}"' for col in columns])
    insert_sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'
    
    count = 0
    for row in rows:
        values = [row.get(col, '') for col in columns]
        try:
            cur.execute(insert_sql, values)
            count += 1
        except Exception as e:
            print(f"      ⚠️ Ошибка вставки строки: {e}")
    
    conn.commit()
    cur.close()
    conn.close()
    
    return count


def flatten_json(obj: Any, parent_key: str = '', sep: str = '_') -> Dict:
    """
    Разворачивает вложенный JSON в плоский словарь
    Пример: {"a": {"b": 1}} → {"a_b": 1}
    """
    items = {}
    
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, (dict, list)):
                items.update(flatten_json(v, new_key, sep))
            else:
                items[new_key] = v
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            new_key = f"{parent_key}{sep}{i}" if parent_key else str(i)
            if isinstance(v, (dict, list)):
                items.update(flatten_json(v, new_key, sep))
            else:
                items[new_key] = v
    else:
        items[parent_key] = obj
    
    return items


def load_csv(file_path: Path) -> Dict:
    """Загрузка CSV файла"""
    try:
        table_name = file_path.stem
        
        with open(file_path, 'r', encoding='utf-8') as f:
            # Определяем разделитель
            first_line = f.readline()
            f.seek(0)
            
            if ';' in first_line:
                delimiter = ';'
            elif '\t' in first_line:
                delimiter = '\t'
            else:
                delimiter = ','
            
            reader = csv.DictReader(f, delimiter=delimiter)
            rows = list(reader)
        
        if not rows:
            return {'status': 'error', 'error': 'пустой файл'}
        
        columns = list(rows[0].keys())
        col_types = detect_column_types(rows, columns)
        
        create_table(table_name, columns, col_types)
        inserted = insert_data(table_name, rows, columns)
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': inserted
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def load_json(file_path: Path) -> Dict:
    """Загрузка JSON файла с разворачиванием вложенности"""
    try:
        table_name = file_path.stem
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Приводим к списку плоских словарей
        rows = []
        
        if isinstance(data, dict):
            # Проверяем формат {"ключ": [...]}
            if len(data) == 1 and isinstance(list(data.values())[0], list):
                items = list(data.values())[0]
                for item in items:
                    if isinstance(item, (dict, list)):
                        rows.append(flatten_json(item))
                    else:
                        rows.append({'value': item})
            else:
                rows.append(flatten_json(data))
        
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, (dict, list)):
                    rows.append(flatten_json(item))
                else:
                    rows.append({'value': item})
        
        if not rows:
            return {'status': 'error', 'error': 'пустой файл'}
        
        # Получаем все возможные колонки
        columns = set()
        for row in rows:
            columns.update(row.keys())
        columns = sorted(list(columns))
        
        # Определяем типы
        col_types = detect_column_types(rows, columns)
        
        # Создаем таблицу и вставляем
        create_table(table_name, columns, col_types)
        inserted = insert_data(table_name, rows, columns)
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': inserted
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def load_sql(file_path: Path) -> Dict:
    """Загрузка SQL файла (выполняем как есть)"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        
        rowcount = cur.rowcount if cur.rowcount else 0
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'rows': rowcount
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def load_excel(file_path: Path) -> Dict:
    """Загрузка Excel файла"""
    try:
        import pandas as pd
    except ImportError:
        return {
            'status': 'skipped',
            'reason': 'pandas не установлен (pip install pandas openpyxl)'
        }
    
    try:
        table_name = file_path.stem
        df = pd.read_excel(file_path)
        
        if df.empty:
            return {'status': 'error', 'error': 'пустой файл'}
        
        # Преобразуем в список словарей
        rows = df.to_dict('records')
        columns = df.columns.tolist()
        
        # Определяем типы
        col_types = detect_column_types(rows, columns)
        
        create_table(table_name, columns, col_types)
        inserted = insert_data(table_name, rows, columns)
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': inserted
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def load_all_from_folder(folder_path: str = 'data') -> List[Dict]:
    """
    Загружает все файлы из папки в PostgreSQL
    """
    print("\n" + "="*70)
    print("🚀 ЗАГРУЗКА ДАННЫХ В POSTGRESQL")
    print("="*70)
    
    data_path = Path(__file__).parent.parent / folder_path
    
    if not data_path.exists():
        print(f"❌ Папка {data_path} не найдена")
        return []
    
    files = list(data_path.glob('*'))
    
    if not files:
        print("⚠️ Папка пуста")
        return []
    
    print(f"\n📁 Найдено файлов: {len(files)}")
    
    results = []
    
    for file_path in files:
        if not file_path.is_file():
            continue
        
        ext = file_path.suffix.lower()
        print(f"\n📄 Файл: {file_path.name}")
        
        if ext == '.csv':
            result = load_csv(file_path)
        elif ext == '.json':
            result = load_json(file_path)
        elif ext == '.sql':
            result = load_sql(file_path)
        elif ext in ('.xlsx', '.xls'):
            result = load_excel(file_path)
        else:
            result = {'status': 'skipped', 'reason': f'формат {ext} не поддерживается'}
        
        results.append({
            'file': file_path.name,
            'result': result
        })
        
        if result.get('status') == 'ok':
            print(f"  ✅ {result['rows']} строк → {result.get('table', '?')}")
        elif result.get('status') == 'skipped':
            print(f"  ⏭️  {result.get('reason', 'пропущено')}")
        else:
            print(f"  ❌ {result.get('error', 'ошибка')}")
    
    # Итог
    ok = sum(1 for r in results if r['result'].get('status') == 'ok')
    skip = sum(1 for r in results if r['result'].get('status') == 'skipped')
    err = sum(1 for r in results if r['result'].get('status') == 'error')
    
    print("\n" + "="*70)
    print(f"✅ Успешно: {ok}")
    print(f"⏭️  Пропущено: {skip}")
    print(f"❌ Ошибок: {err}")
    print("="*70)
    
    return results


if __name__ == '__main__':
    load_all_from_folder()