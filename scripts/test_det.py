"""
test_schema.py - Правильный тест universal_schema_detector.py
"""

import os
import sys
from pathlib import Path

# Добавляем путь
sys.path.append(str(Path(__file__).parent))

from universal_schema_detector import detect_schema, get_tables
from dotenv import load_dotenv

# Загружаем .env
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', '123'),
    'database': os.getenv('DB_NAME', 'video_analytics')
}

def main():
    print("="*60)
    print("ТЕСТ universal_schema_detector.py")
    print("="*60)
    
    # Получаем схему
    schema = detect_schema(DB_CONFIG)
    
    # Выводим красиво
    for table_name, columns in schema.items():
        print(f"\n📁 {table_name}:")
        for col_name, col_type in columns.items():
            print(f"  - {col_name}: {col_type}")

if __name__ == "__main__":
    main()