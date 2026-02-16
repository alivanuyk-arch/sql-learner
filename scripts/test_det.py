# test_detector.py
import json
from universal_schema_detector import get_schema

def test_simple_dict():
    """Простой словарь"""
    data = {"id": 1, "name": "test", "price": 99.9}
    result = get_schema(data)
    print("✅ Словарь:", result)
    assert result == {"id": "int", "name": "str", "price": "float"}

def test_nested_dict():
    """Вложенный словарь"""
    data = {
        "video": {
            "id": 1,
            "stats": {"views": 15000}
        }
    }
    result = get_schema(data)
    print("✅ Вложенный:", result)
    # Должно быть: {"video": {"id": "int", "stats": {"views": "int"}}}

def test_list():
    """Список объектов"""
    data = [
        {"id": 1, "title": "Video 1"},
        {"id": 2, "title": "Video 2"}
    ]
    result = get_schema(data)
    print("✅ Список:", result)
    # Должно быть: [{"id": "int", "title": "str"}]

def test_json_string():
    """JSON строка"""
    data = '{"users": [{"name": "Ivan", "age": 30}]}'
    result = get_schema(data)
    print("✅ JSON строка:", result)

def test_date():
    """Дата"""
    data = {"date": "2025-11-15", "event": "meeting"}
    result = get_schema(data)
    print("✅ Дата:", result)

def test_complex():
    """Сложная структура"""
    data = {
        "videos": [
            {
                "id": 1,
                "title": "Cat",
                "stats": {
                    "views": 15000,
                    "likes": 450,
                    "daily": [5000, 6000, 4000]
                },
                "created_at": "2025-11-15"
            }
        ],
        "total": 1
    }
    result = get_schema(data)
    print("✅ Сложная:", json.dumps(result, indent=2, ensure_ascii=False))

def test_error_cases():
    """Ошибки"""
    print("\n=== ТЕСТЫ НА ОШИБКИ ===")
    
    # Пустые данные
    try:
        result = get_schema(None)
        print("✅ None:", result)
    except Exception as e:
        print("❌ None упал:", e)
    
    # Пустой список
    try:
        result = get_schema([])
        print("✅ Пустой список:", result)
    except Exception as e:
        print("❌ Пустой список упал:", e)

if __name__ == "__main__":
    print("="*50)
    print("ТЕСТИРОВАНИЕ УНИВЕРСАЛЬНОГО ДЕТЕКТОРА")
    print("="*50)
    
    tests = [
        test_simple_dict,
        test_nested_dict,
        test_list,
        test_json_string,
        test_date,
        test_complex,
        test_error_cases
    ]
    
    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"❌ {test.__name__} УПАЛ: {e}")
        print("-"*40)