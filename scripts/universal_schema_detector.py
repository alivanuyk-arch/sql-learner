#!/usr/bin/env python3
"""
universal_schema_detector.py - Определяет схему данных
Возвращает структуру с типами: str, int, float, date, bool
"""

import json
import re
from datetime import datetime
from typing import Any, Dict, List, Union

def get_schema(data: Any) -> Any:
    """
    Рекурсивно определяет схему данных
    """
    # Словарь
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            result[key] = get_schema(value)
        return result
    
    # Список
    elif isinstance(data, list):
        if not data:
            return []
        # Берем первый элемент как образец
        return [get_schema(data[0])]
    
    # Примитивы
    else:
        if isinstance(data, bool):
            return "bool"
        elif isinstance(data, int):
            return "int"
        elif isinstance(data, float):
            return "float"
        elif isinstance(data, str):
            # Проверяем на дату
            if re.match(r'^\d{4}-\d{2}-\d{2}$', data):
                return "date"
            if re.match(r'^\d{2}\.\d{2}\.\d{4}$', data):
                return "date"
            return "str"
        elif data is None:
            return "null"
        else:
            return str(type(data).__name__)


def get_schema_from_file(filepath: str) -> Dict:
    """
    Читает файл и возвращает схему
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            if filepath.endswith('.json'):
                data = json.load(f)
            else:
                data = f.read()
        
        return {
            "source": filepath,
            "schema": get_schema(data)
        }
    except Exception as e:
        return {"error": str(e)}


def get_schema_from_postgres(conn_config: Dict, table: str) -> Dict:
    """
    Получает схему таблицы из PostgreSQL
    """
    try:
        import psycopg2
        conn = psycopg2.connect(**conn_config)
        cur = conn.cursor()
        
        # Получаем информацию о колонках
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
        """, (table,))
        
        columns = {}
        for row in cur.fetchall():
            col_name, data_type, is_nullable = row
            
            # Маппим типы PostgreSQL в наши
            if 'int' in data_type:
                col_type = 'int'
            elif 'float' in data_type or 'double' in data_type or 'numeric' in data_type:
                col_type = 'float'
            elif 'date' in data_type or 'timestamp' in data_type:
                col_type = 'date'
            else:
                col_type = 'str'
            
            columns[col_name] = col_type
        
        cur.close()
        conn.close()
        
        return {
            "table": table,
            "columns": columns
        }
    except Exception as e:
        return {"error": str(e)}

