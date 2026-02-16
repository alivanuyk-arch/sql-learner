import os
import sys
import glob
import json
import csv
from pathlib import Path

# Добавляем путь к проекту
sys.path.append(str(Path(__file__).parent.parent))

import psycopg2
from dotenv import load_dotenv

# Загружаем .env из корня проекта
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Конфиг БД из .env
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', 'postgres')
}

def load_all_to_postgres(data_folder='data'):
    """
    Загружает все поддерживаемые файлы из папки в PostgreSQL
    """
    print("\n" + "="*60)
    print("🚀 ЗАГРУЗЧИК ДАННЫХ В POSTGRESQL")
    print("="*60)
    
    # Проверяем папку data
    data_path = Path(__file__).parent.parent / data_folder
    if not data_path.exists():
        print(f"❌ Папка {data_path} не найдена")
        return
    
    # Получаем все файлы
    files = list(data_path.glob('*'))
    
    if not files:
        print("⚠️ Папка data пуста")
        return
    
    print(f"\n📁 Найдено файлов: {len(files)}")
    
    results = []
    
    for file_path in files:
        if not file_path.is_file():
            continue
            
        ext = file_path.suffix.lower()
        print(f"\n📄 Файл: {file_path.name}")
        
        # Выбираем обработчик по расширению
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
        
        # Выводим результат
        if result.get('status') == 'ok':
            print(f"  ✅ Загружено: {result.get('rows', 0)} строк → {result.get('table', '?')}")
        else:
            print(f"  ⏭️  {result.get('reason', result.get('error', 'ошибка'))}")
    
    # Итог
    print("\n" + "="*60)
    print("📊 ИТОГ ЗАГРУЗКИ:")
    
    ok_count = sum(1 for r in results if r['result'].get('status') == 'ok')
    skip_count = sum(1 for r in results if r['result'].get('status') == 'skipped')
    error_count = sum(1 for r in results if r['result'].get('status') == 'error')
    
    print(f"✅ Успешно: {ok_count}")
    print(f"⏭️  Пропущено: {skip_count}")
    print(f"❌ Ошибок: {error_count}")
    
    return results

def load_csv(file_path):
    """Загрузка CSV файла"""
    try:
        table_name = file_path.stem
        
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Читаем CSV
        with open(file_path, 'r', encoding='utf-8') as f:
            # Пробуем определить разделитель
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
        
        # Получаем колонки
        columns = list(rows[0].keys())
        
        # Создаем таблицу
        col_defs = [f'"{col}" TEXT' for col in columns]
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)})'
        cur.execute(create_sql)
        
        # Очищаем таблицу перед загрузкой
        cur.execute(f'TRUNCATE {table_name}')
        
        # Вставляем данные
        for row in rows:
            placeholders = ', '.join(['%s'] * len(columns))
            cols = ', '.join([f'"{col}"' for col in columns])
            values = [row.get(col, '') for col in columns]
            
            insert_sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'
            cur.execute(insert_sql, values)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': len(rows)
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def load_json(file_path):
    """Загрузка JSON файла"""
    try:
        table_name = file_path.stem
        
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Приводим к списку
        if isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            rows = data
        else:
            return {'status': 'error', 'error': 'неподдерживаемая структура JSON'}
        
        if not rows:
            return {'status': 'error', 'error': 'пустой файл'}
        
        # Собираем все возможные ключи
        all_keys = set()
        for row in rows:
            if isinstance(row, dict):
                all_keys.update(row.keys())
        
        if not all_keys:
            return {'status': 'error', 'error': 'нет данных'}
        
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Создаем таблицу
        col_defs = [f'"{key}" TEXT' for key in all_keys]
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)})'
        cur.execute(create_sql)
        
        # Очищаем таблицу
        cur.execute(f'TRUNCATE {table_name}')
        
        # Вставляем данные
        columns = list(all_keys)
        for row in rows:
            if not isinstance(row, dict):
                continue
            
            values = []
            for col in columns:
                val = row.get(col, '')
                if val is None:
                    val = ''
                values.append(str(val))
            
            placeholders = ', '.join(['%s'] * len(columns))
            cols = ', '.join([f'"{col}"' for col in columns])
            
            insert_sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'
            cur.execute(insert_sql, values)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': len(rows)
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

def load_sql(file_path):
    """Загрузка SQL файла (выполняем как есть)"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql = f.read()
        
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Выполняем SQL
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

def load_excel(file_path):
    """Загрузка Excel файла"""
    try:
        # Проверяем наличие pandas
        try:
            import pandas as pd
        except ImportError:
            return {
                'status': 'skipped',
                'reason': 'pandas не установлен (pip install pandas openpyxl)'
            }
        
        table_name = file_path.stem
        
        # Читаем Excel
        df = pd.read_excel(file_path)
        
        if df.empty:
            return {'status': 'error', 'error': 'пустой файл'}
        
        # Подключаемся к БД
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Создаем таблицу
        columns = df.columns.tolist()
        col_defs = [f'"{col}" TEXT' for col in columns]
        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(col_defs)})'
        cur.execute(create_sql)
        
        # Очищаем таблицу
        cur.execute(f'TRUNCATE {table_name}')
        
        # Вставляем данные
        for _, row in df.iterrows():
            values = [str(val) if pd.notna(val) else '' for val in row]
            placeholders = ', '.join(['%s'] * len(columns))
            cols = ', '.join([f'"{col}"' for col in columns])
            
            insert_sql = f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})'
            cur.execute(insert_sql, values)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'table': table_name,
            'rows': len(df)
        }
        
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

if __name__ == '__main__':
    load_all_to_postgres()