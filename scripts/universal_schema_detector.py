"""
universal_schema_detector.py - Минимальная схема для LLM
Возвращает ТОЛЬКО имена таблиц, полей и их типы
"""

import psycopg2
from typing import Dict, List

def detect_schema(db_config: Dict) -> Dict:
    """
    Возвращает минимальную схему: {таблица: {поле: тип}}
    Типы: int, float, str, date
    """
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    # Все таблицы
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [row[0] for row in cur.fetchall()]
    
    schema = {}
    
    for table in tables:
        # Колонки таблицы
        cur.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, (table,))
        
        columns = {}
        for col in cur.fetchall():
            col_name, data_type = col
            
            # Маппим в 4 типа
            if 'int' in data_type:
                col_type = 'int'
            elif 'float' in data_type or 'numeric' in data_type:
                col_type = 'float'
            elif 'date' in data_type or 'timestamp' in data_type:
                col_type = 'date'
            else:
                col_type = 'str'
            
            columns[col_name] = col_type
        
        schema[table] = columns
    
    cur.close()
    conn.close()
    
    return schema


def get_tables(db_config: Dict) -> List[str]:
    """Просто список таблиц"""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables