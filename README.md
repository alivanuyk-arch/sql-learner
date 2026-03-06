Trainable SQL Query Builder
Telegram-бот, превращающий русские вопросы в SQL. Самообучается на исправлениях.
ИДЕТ РЕФАКТОРИНГ С РАСШИРЕНИЕМ ФУНКЦИОНАЛА!
 ###Что делает
 Русский → SQL – понимает вопросы типа "Сколько видео с 1000+ просмотров?" пока на уровне простых запросов  расширяю делаю бэкэнд для Join функций
 Самообучается – запоминает исправленные SQL как шаблоны
 Автосхема – сам изучает структуру PostgreSQL или JSON данных добавил еще xlsx, doc, txt, архивы.
 Умный кэш – второй раз отвечает мгновенно
 Правки SQL – аналитики могут править сгенерированные запросы

### Быстрый старт
bash
# 1. Клонировать
git clone https://github.com/ваш-репозиторий.git
cd Trainable-SQL-query-builder
# 2. Зависимости
pip install -r requirements.txt
# 3. Настройка
cp .env.example .env
# Отредактируйте .env: токен бота, БД, LLM
# 4. Запустить
python main.py
###Примеры
text
Вы: "Сколько видео в системе?"
Бот: SELECT COUNT(*) FROM videos;
      Результат: 360

Вы: "Топ 5 по просмотрам за ноябрь"
Бот: SELECT title, views_count FROM videos 
     WHERE EXTRACT(MONTH FROM created_at) = 11 
     ORDER BY views_count DESC LIMIT 5;
### Как работает
text
Пользователь → Конструктор → LLM → SQL → База → Ответ
       ↑           ↓           ↓        ↓       ↓
  Исправление ← Кэш ←  Сохранение  ← Выполнение
Компоненты:
Конструктор – умный кэш SQL шаблонов

LLMClient – Ollama/OpenAI для генерации SQL

AutoSchemaDetector – сам изучает БД

PatternLearner – учится на исправлениях

### Конфиг (.env)
env
TELEGRAM_TOKEN=ваш_токен
DATABASE_URL=postgresql://user:pass@localhost/db
LLM_PROVIDER=ollama  # или openai
OLLAMA_MODEL=mistral:7b-instruct
📁 Структура
text
src/
├── bot/           # Telegram бот
├── constructor/   # Кэш и шаблоны SQL
├── llm/           # Работа с LLM
├── database/      # Подключение к БД
└── schema/        # Автоопределение схемы
### Тестирование
bash
python test_llm.py        # Проверить LLM
python test_constructor.py # Проверить кэш
####Для кого
Аналитики – получают SQL без знания синтаксиса
Разработчики – экономят время на рутинных запросах
Команды – накапливают библиотеку типовых SQL
Один раз поправил SQL – система запомнила навсегда.

P.S. Проект не взят из воздуха. Тут был и гуглёж, и помощь ИИ-ассистента, и собственные правки руками. 
