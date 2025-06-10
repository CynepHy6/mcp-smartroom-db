# 📖 Краткое руководство по использованию

## 🚀 Использование в других проектах

### ✅ Рекомендуемый способ - Глобальная установка

```bash
# 1. Установка (один раз)
git clone <repository> mcp-skyeng-db
cd mcp-skyeng-db
./install.sh

# 2. Настройка (один раз)
cp ~/.config/mcp-skyeng-db/.db.yaml.example ~/.config/mcp-skyeng-db/.db.yaml
nano ~/.config/mcp-skyeng-db/.db.yaml  # Заполните креды

# 3. Добавление в Cursor (для каждого проекта)
# Добавьте в настройки MCP:
{
  "mcpServers": {
    "skyeng-db-server": {
      "command": "/home/username/.local/bin/mcp-skyeng-db",
      "args": []
    }
  }
}
```

**Преимущества:**
- ✅ Работает из любого проекта
- ✅ Единая конфигурация БД
- ✅ Автообновления через `git pull`
- ✅ Не засоряет проекты

### 📁 Альтернативные способы

#### Git Submodule
```bash
# В корне вашего проекта
git submodule add <repository> tools/mcp-skyeng-db
cd tools/mcp-skyeng-db
./setup.sh
```

#### Прямое копирование (не рекомендуется)
```bash
# В корне вашего проекта
cp -r /path/to/mcp-skyeng-db ./mcp-tools/
cd mcp-tools
./setup.sh
```

## 🔧 Проверка установки

```bash
# Проверить статус установки
./check-install.sh

# Тестировать подключение
mcp-skyeng-db  # для глобальной установки
```

## 🗑️ Удаление

```bash
# Для глобальной установки
./uninstall.sh

# Для локальной установки
rm -rf venv .db.yaml
```

## ⚙️ Настройка конфигурации

### Приоритет поиска .db.yaml:
1. Параметр `--config`
2. Переменная `MCP_DB_CONFIG`
3. Локальный `./.db.yaml`
4. Глобальный `~/.config/mcp-skyeng-db/.db.yaml`

### Примеры использования переменной окружения:
```bash
# Для конкретного проекта
export MCP_DB_CONFIG="/path/to/project/.db.yaml"

# В .bashrc для глобального использования
echo 'export MCP_DB_CONFIG="$HOME/.config/mcp-skyeng-db/.db.yaml"' >> ~/.bashrc
```

## 🎯 Быстрый старт для нового проекта

```bash
# 1. Проверить установку
check-install.sh

# 2. Если не установлен - установить глобально
git clone <repository> && cd mcp-skyeng-db && ./install.sh

# 3. Настроить конфигурацию (если еще не настроена)
cp ~/.config/mcp-skyeng-db/.db.yaml.example ~/.config/mcp-skyeng-db/.db.yaml
nano ~/.config/mcp-skyeng-db/.db.yaml

# 4. Добавить в Cursor настройки MCP
# Скопировать содержимое ~/.config/mcp-skyeng-db/cursor-mcp-config.json

# 5. Готово! Можно использовать в любом проекте
```

## 🆘 Решение проблем

### Проблема: "БД для предмета X не найдена"
```bash
# Проверить список доступных БД
cat ~/.config/mcp-skyeng-db/.db.yaml

# Убедиться что предмет корректно мапится на БД
# Посмотреть логи: ~/.local/share/mcp-skyeng-db/mcp-db-server.log
```

### Проблема: "Файл конфигурации не найден"
```bash
# Проверить пути поиска
MCP_DB_CONFIG="/custom/path/.db.yaml" mcp-skyeng-db

# Или создать символическую ссылку
ln -s /path/to/your/.db.yaml ~/.config/mcp-skyeng-db/.db.yaml
```

### Проблема: "Ошибка подключения к БД"
```bash
# Проверить креды в конфигурации
nano ~/.config/mcp-skyeng-db/.db.yaml

# Тестировать подключение напрямую
psql -h host -p port -U user -d database
``` 