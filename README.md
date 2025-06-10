# MCP сервер для работы с БД Skyeng Platform

Безопасный MCP (Model Context Protocol) сервер для работы с базами данных Skyeng Platform без передачи кредов в удаленное API.

## 🎯 Возможности

- **Безопасность**: только SELECT, WITH, EXPLAIN запросы
- **Простота**: прямая работа с БД по названию
- **Кэширование**: схемы таблиц кэшируются для быстрого доступа
- **Мониторинг**: логирование всех запросов и производительности


## 🚀 Установка

```bash
# Клонируем проект
git clone <repository> mcp-smartroom-db
cd mcp-smartroom-db

# Создаем виртуальное окружение и устанавливаем зависимости
python3 -m venv venv
source venv/bin/activate
pip install -r requirements-mcp.txt
```

## ⚙️ Настройка

### 1. Настройка кредов БД

```bash
# Копируем пример конфигурации
cp .db.yaml.example .db.yaml

# Редактируем файл с кредами
nano .db.yaml
```

Пример содержимого `.db.yaml`:
```yaml
db_name:
  db_host_name.link: port
  user_name: user_password
```

### 2. Тестирование

```bash
# Активируем виртуальное окружение
source venv/bin/activate

# Тестируем подключение к БД
python3 mcp-db-server.py
```

### 3. Интеграция с Cursor

Добавьте в настройки Cursor:

```json
{
  "mcpServers": {
    "skyeng-db-server": {
      "command": "/absolute/path/to/mcp-smartroom-db/mcp-server"
    }
  }
}
```

## 🔒 Безопасность

- Только чтение: разрешены только немодифицирующие запросы
- Локальные креды: пароли хранятся только локально в `.db.yaml`
- Валидация запросов: фильтрация опасных операций
- Логирование: все запросы записываются в `mcp-db-server.log`

## 📁 Структура проекта

```
mcp-smartroom-db/
├── mcp-db-server.py          # Основной сервер
├── mcp-server               # Исполняемый скрипт-обертка
├── .cursor/
│   └── mcp-config.json      # Конфигурация для Cursor
├── venv/                    # Виртуальное окружение
├── requirements-mcp.txt      # Зависимости Python
├── .db.yaml                 # Креды БД (создается вручную)
├── .db.yaml.example         # Пример конфигурации
└── README.md               # Документация
```

## 🔍 Пример использования

После настройки MCP в Cursor можете задавать вопросы:
- "Покажи информацию о БД math"
- "Какие таблицы есть в БД skysmart_english?"
- "Покажи схему таблицы users в БД math"


## 🔧 Диагностика

При возникновении проблем:

1. Проверьте логи в `mcp-db-server.log`
2. Убедитесь что `.db.yaml` содержит правильные креды
3. Тестируйте подключение: `./mcp-server --test`
