#!/usr/bin/env python3
"""
MCP сервер для безопасной работы с базами данных Skyeng Platform
Поддерживает все предметы платформы с локальным хранением кредов
"""

import json
import logging
import os
import re
import hashlib
import time
from typing import Dict, List, Optional, Any, Union
import asyncio
import yaml

import psycopg2
import psycopg2.extras
from mcp.server import Server
from mcp.types import (
    Resource, Tool, TextContent, CallToolRequest, 
    ListResourcesRequest, ListToolsRequest, ReadResourceRequest
)
import mcp.server.stdio

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('mcp-db-server.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Менеджер для работы с базами данных предметов"""
    
    def __init__(self, config_path: str = None):
        # Приоритет поиска конфигурации:
        # 1. Параметр config_path
        # 2. Переменная окружения MCP_DB_CONFIG
        # 3. Локальный .db.yaml
        # 4. Глобальный ~/.config/mcp-skyeng-db/.db.yaml
        if config_path:
            self.config_path = config_path
        elif os.getenv("MCP_DB_CONFIG"):
            self.config_path = os.getenv("MCP_DB_CONFIG")
        elif os.path.exists(".db.yaml"):
            self.config_path = ".db.yaml"
        elif os.path.exists(os.path.expanduser("~/.config/mcp-skyeng-db/.db.yaml")):
            self.config_path = os.path.expanduser("~/.config/mcp-skyeng-db/.db.yaml")
        else:
            self.config_path = ".db.yaml"  # Fallback
        self.connections: Dict[str, Dict] = {}
        self.schema_cache: Dict[str, Dict] = {}
        self._load_db_config()
    

    
    def _load_db_config(self):
        """Загружает конфигурацию подключений к БД"""
        try:
            with open(self.config_path, "r", encoding="utf-8") as f:
                db_config = yaml.safe_load(f)
                
            # Теперь ключи это названия БД, а не предметы
            for db_name, db_info in db_config.items():
                # Находим хост и порт
                host = None
                port = None
                user = None
                password = None
                
                for key, value in db_info.items():
                    # Если значение числовое - это порт, ключ - хост
                    if isinstance(value, int):
                        host = key
                        port = value
                    # Если значение строковое - это пароль, ключ - пользователь
                    elif isinstance(value, str):
                        user = key
                        password = value
                
                if host and port and user and password:
                    self.connections[db_name] = {
                        "host": host,
                        "port": port,
                        "database": db_name,  # Используем название БД как database
                        "user": user,
                        "password": password
                    }
                    logger.info(f"Загружена конфигурация для БД: {db_name}")
                else:
                    logger.warning(f"Неполная конфигурация для БД {db_name}")
                    
        except FileNotFoundError:
            logger.error(f"Файл конфигурации {self.config_path} не найден")
            raise
        except Exception as e:
            logger.error(f"Ошибка загрузки конфигурации: {e}")
            raise
    

    
    def _validate_query(self, query: str) -> bool:
        """Валидирует SQL запрос - запрещены только модифицирующие операции (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, GRANT, REVOKE, EXEC, EXECUTE)"""
        query_clean = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
        query_clean = re.sub(r'/\*.*?\*/', '', query_clean, flags=re.DOTALL)
        query_clean = query_clean.strip().upper()

        # Разрешаем любые операции получения данных
        allowed_keywords = [
            'SELECT', 'WITH', 'EXPLAIN', 'SHOW', 'DESCRIBE', 'VALUES'
        ]
        if not any(query_clean.startswith(keyword) for keyword in allowed_keywords):
            return False

        # Запрещаем любые модифицирующие операции только как отдельные слова (операторы)
        dangerous_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE'
        ]
        for dangerous in dangerous_keywords:
            # Ищем только целое слово (оператор), не подстроку
            if re.search(rf'\\b{dangerous}\\b', query_clean):
                return False
        return True
    
    def _get_connection(self, db_name: str):
        """Получает подключение к БД"""
        if db_name not in self.connections:
            raise ValueError(f"БД {db_name} не найдена в конфигурации")
        
        conn_config = self.connections[db_name]
        
        try:
            conn = psycopg2.connect(
                host=conn_config["host"],
                port=conn_config["port"],
                database=conn_config["database"],
                user=conn_config["user"],
                password=conn_config["password"],
                connect_timeout=10
            )
            return conn
        except Exception as e:
            logger.error(f"Ошибка подключения к БД {db_name}: {e}")
            raise
        


    def list_databases(self) -> Dict[str, Dict]:
        """Возвращает список всех БД с информацией"""
        databases_info = {}
        
        for db_name, config in self.connections.items():
            try:
                with self._get_connection(db_name) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        # Получаем базовую информацию о БД
                        cur.execute("""
                        SELECT 
                            current_database() as database_name,
                            current_user as current_user,
                            version() as version,
                            pg_database_size(current_database()) as size_bytes
                        """)
                        db_info = dict(cur.fetchone())
                        
                        # Получаем количество таблиц
                        cur.execute("""
                        SELECT COUNT(*) as tables_count 
                        FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        """)
                        tables_info = dict(cur.fetchone())
                        
                        databases_info[db_name] = {
                            **db_info,
                            **tables_info,
                            "connection_config": {
                                "host": config["host"],
                                "database": config["database"],
                                "user": config["user"]
                            },
                            "available": True
                        }
                        
            except Exception as e:
                databases_info[db_name] = {
                    "available": False,
                    "error": str(e),
                    "connection_config": {
                        "host": config["host"],
                        "database": config["database"],
                        "user": config["user"]
                    }
                }
        
        return databases_info
    
    def execute_query_direct(self, query: str, database: str) -> Dict[str, Any]:
        """Выполняет SQL запрос к БД напрямую"""
        if not self._validate_query(query):
            raise ValueError("Запрос содержит недопустимые операции")
        
        if database not in self.connections:
            return {
                "success": False,
                "error": f"БД {database} не найдена в конфигурации",
                "database": database,
                "execution_time": 0
            }
        
        start_time = time.time()
        
        try:
            with self._get_connection(database) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query)
                    
                    if cur.description:
                        results = cur.fetchall()
                        results = [dict(row) for row in results]
                    else:
                        results = []
                    
                    execution_time = time.time() - start_time
                    
                    logger.info(f"Запрос к БД {database} выполнен за {execution_time:.3f}с")
                    
                    return {
                        "success": True,
                        "data": results,
                        "rows_count": len(results),
                        "execution_time": execution_time,
                        "database": database
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка выполнения запроса к БД {database}: {e}")
            return {
                "success": False,
                "error": str(e),
                "database": database,
                "execution_time": time.time() - start_time
            }
    
    def get_table_schema_direct(self, table_name: str, database: str) -> Dict[str, Any]:
        """Получает схему таблицы напрямую по БД"""
        if database not in self.connections:
            return {
                "success": False,
                "error": f"БД {database} не найдена в конфигурации",
                "table_name": table_name,
                "database": database
            }
        
        cache_key = f"{database}:{table_name}"
        
        if cache_key in self.schema_cache:
            return self.schema_cache[cache_key]
        
        schema_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns 
        WHERE table_name = %s
        ORDER BY ordinal_position;
        """
        
        try:
            with self._get_connection(database) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(schema_query, (table_name,))
                    columns = [dict(row) for row in cur.fetchall()]
                    
                    # Получаем информацию о индексах
                    indexes_query = """
                    SELECT indexname, indexdef 
                    FROM pg_indexes 
                    WHERE tablename = %s;
                    """
                    cur.execute(indexes_query, (table_name,))
                    indexes = [dict(row) for row in cur.fetchall()]
                    
                    schema_info = {
                        "success": True,
                        "table_name": table_name,
                        "database": database,
                        "columns": columns,
                        "indexes": indexes,
                        "cached_at": time.time()
                    }
                    
                    self.schema_cache[cache_key] = schema_info
                    return schema_info
                    
        except Exception as e:
            logger.error(f"Ошибка получения схемы таблицы {table_name} в БД {database}: {e}")
            return {
                "success": False,
                "error": str(e),
                "table_name": table_name,
                "database": database
            }
    
    def get_database_info_direct(self, database: str) -> Dict[str, Any]:
        """Получает детальную информацию о БД напрямую"""
        if database not in self.connections:
            return {
                "success": False,
                "error": f"БД {database} не найдена в конфигурации",
                "database": database
            }
        
        config = self.connections[database]
        
        try:
            with self._get_connection(database) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Основная информация
                    cur.execute("""
                    SELECT 
                        current_database() as database_name,
                        current_user as current_user,
                        version() as version,
                        pg_database_size(current_database()) as size_bytes
                    """)
                    db_info = dict(cur.fetchone())
                    
                    # Список таблиц
                    cur.execute("""
                    SELECT 
                        tablename,
                        pg_size_pretty(pg_total_relation_size('public.'||tablename)) as size
                    FROM pg_tables 
                    WHERE schemaname = 'public'
                    ORDER BY pg_total_relation_size('public.'||tablename) DESC
                    """)
                    tables = [dict(row) for row in cur.fetchall()]
                    
                    return {
                        "success": True,
                        "database": database,
                        "info": db_info,
                        "tables": tables,
                        "tables_count": len(tables),
                        "connection_config": {
                            "host": config["host"],
                            "database": config["database"],
                            "user": config["user"]
                        }
                    }
                    
        except Exception as e:
            logger.error(f"Ошибка получения информации о БД {database}: {e}")
            return {
                "success": False,
                "error": str(e),
                "database": database,
                "connection_config": {
                    "host": config["host"],
                    "database": config["database"],
                    "user": config["user"]
                }
            }
    def get_all_tables_schemas_direct(self, database: str) -> Dict[str, Any]:
        """Получает схемы всех таблиц в указанной БД"""
        if database not in self.connections:
            return {
                "success": False,
                "error": f"БД {database} не найдена в конфигурации",
                "database": database
            }

        try:
            with self._get_connection(database) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Получаем все таблицы и их колонки
                    cur.execute("""
                    SELECT 
                        table_name,
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale
                    FROM information_schema.columns 
                    WHERE table_schema = 'public'
                    ORDER BY table_name, ordinal_position;
                    """)
                    columns_data = cur.fetchall()

                    # Получаем индексы для всех таблиц
                    cur.execute("""
                    SELECT 
                        tablename,
                        indexname,
                        indexdef 
                    FROM pg_indexes 
                    WHERE schemaname = 'public';
                    """)
                    indexes_data = cur.fetchall()

                    # Группируем данные по таблицам
                    tables = {}
                    for row in columns_data:
                        table_name = row["table_name"]
                        if table_name not in tables:
                            tables[table_name] = {
                                "columns": [],
                                "indexes": []
                            }
                        tables[table_name]["columns"].append({
                            "column_name": row["column_name"],
                            "data_type": row["data_type"],
                            "is_nullable": row["is_nullable"],
                            "column_default": row["column_default"],
                            "character_maximum_length": row.get("character_maximum_length"),
                            "numeric_precision": row.get("numeric_precision"),
                            "numeric_scale": row.get("numeric_scale")
                        })

                    # Добавляем индексы
                    for row in indexes_data:
                        table_name = row["tablename"]
                        if table_name not in tables:
                            tables[table_name] = {
                                "columns": [],
                                "indexes": []
                            }
                        tables[table_name]["indexes"].append({
                            "indexname": row["indexname"],
                            "indexdef": row["indexdef"]
                        })

                    return {
                        "success": True,
                        "database": database,
                        "tables": tables,
                        "tables_count": len(tables)
                    }

        except Exception as e:
            logger.error(f"Ошибка получения схем всех таблиц для БД {database}: {e}")
            return {
                "success": False,
                "error": str(e),
                "database": database
            }

# Создаем экземпляр менеджера БД
db_manager = DatabaseManager()

# Создаем MCP сервер
server = Server("skyeng-db-server")

@server.list_tools()
async def list_tools() -> list[Tool]:
    """Список доступных инструментов"""
    return [
        Tool(
            name="execute_query",
            description="Выполнить SELECT запрос к БД",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SQL запрос (только SELECT, WITH, EXPLAIN)"
                    },
                    "database": {
                        "type": "string",
                        "description": "Название БД (например: math, skysmart_english)"
                    }
                },
                "required": ["query", "database"]
            }
        ),
        Tool(
            name="get_table_schema",
            description="Получить схему таблицы",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Название таблицы"
                    },
                    "database": {
                        "type": "string",
                        "description": "Название БД"
                    }
                },
                "required": ["table_name", "database"]
            }
        ),
        Tool(
            name="list_databases",
            description="Получить список всех доступных БД",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_database_info",
            description="Получить детальную информацию о БД",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Название БД"
                    }
                },
                "required": ["database"]
            }
        ),
        Tool(
            name="get_all_tables_schemas",
            description="Получить схемы всех таблиц в указанной БД",
            inputSchema={
                "type": "object",
                "properties": {
                    "database": {
                        "type": "string",
                        "description": "Название БД"
                    }
                },
                "required": ["database"]
            }
        )
    ]

@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    """Обработка вызовов инструментов"""
    
    try:
        if name == "execute_query":
            query = arguments.get("query")
            database = arguments.get("database")
            
            if not query or not database:
                return [TextContent(
                    type="text",
                    text="Ошибка: необходимо указать query и database"
                )]
            
            result = db_manager.execute_query_direct(query, database)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        
        elif name == "get_table_schema":
            table_name = arguments.get("table_name")
            database = arguments.get("database")
            
            if not table_name or not database:
                return [TextContent(
                    type="text",
                    text="Ошибка: необходимо указать table_name и database"
                )]
            
            result = db_manager.get_table_schema_direct(table_name, database)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        
        elif name == "list_databases":
            result = db_manager.list_databases()
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        
        elif name == "get_database_info":
            database = arguments.get("database")
            
            if not database:
                return [TextContent(
                    type="text",
                    text="Ошибка: необходимо указать database"
                )]
            
            result = db_manager.get_database_info_direct(database)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        elif name == "get_all_tables_schemas":
            database = arguments.get("database")
            if not database:
                return [TextContent(
                    type="text",
                    text="Ошибка: необходимо указать database"
                )]
            result = db_manager.get_all_tables_schemas_direct(database)
            return [TextContent(
                type="text",
                text=json.dumps(result, ensure_ascii=False, indent=2, default=str)
            )]
        
        else:
            return [TextContent(
                type="text",
                text=f"Неизвестный инструмент: {name}"
            )]
    
    except Exception as e:
        logger.error(f"Ошибка выполнения инструмента {name}: {e}")
        return [TextContent(
            type="text",
            text=f"Ошибка: {str(e)}"
        )]

def show_help():
    """Показать справку"""
    print("MCP сервер для работы с БД Skyeng Platform\n")
    print("Использование:")
    print("  mcp-skyeng-db                 - Запуск MCP сервера")
    print("  mcp-skyeng-db --help           - Показать эту справку")
    print("  mcp-skyeng-db --list-databases - Показать все БД и статус подключения")
    print("  mcp-skyeng-db --test           - Проверить подключения ко всем БД\n")
    print("Доступные инструменты MCP:")
    print("  • execute_query         - Выполнить SELECT/EXPLAIN/WITH запрос к БД")
    print("  • get_table_schema      - Получить схему таблицы (структура, индексы)")
    print("  • list_databases        - Список всех доступных БД и их статус")
    print("  • get_database_info     - Детальная информация о БД (размер, таблицы)")
    print("  • get_all_tables_schemas- Получить схемы всех таблиц в БД\n")
    print("Конфигурация:")
    print("  ~/.config/mcp-skyeng-db/.db.yaml - Настройки подключений к БД")

async def main():
    """Запуск MCP сервера"""
    import sys
    
    # Обработка аргументов командной строки
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        
        if arg in ['--help', '-h']:
            show_help()
            return

        elif arg == '--list-databases':
            try:
                databases = db_manager.list_databases()
                print("Доступные базы данных:")
                for db_name, info in databases.items():
                    status = "✅" if info.get("available", False) else "❌"
                    print(f"  {status} {db_name}")
                    if info.get("available", False):
                        config = info["connection_config"]
                        print(f"      └─ {config['host']} / {config['database']}")
                    else:
                        print(f"      └─ Ошибка: {info.get('error', 'Неизвестная ошибка')}")
                print(f"\nВсего: {len(databases)}")
            except Exception as e:
                print(f"Ошибка: {e}")
            return
        elif arg == '--test':
            try:
                print("🔍 Тестирование подключений...")
                databases = db_manager.list_databases()
                working_count = 0
                for db_name, info in databases.items():
                    status = "✅" if info.get("available", False) else "❌"
                    print(f"  {status} {db_name}")
                    if info.get("available", False):
                        working_count += 1
                        config = info["connection_config"]
                        print(f"      └─ {config['host']} / {config['database']}")
                    else:
                        print(f"      └─ Ошибка: {info.get('error', 'Неизвестная ошибка')}")
                print(f"\n📊 Результат: {working_count}/{len(databases)} подключений работают")
            except Exception as e:
                print(f"Ошибка: {e}")
            return
        else:
            print(f"Неизвестный аргумент: {arg}")
            print("Используйте --help для справки")
            return
    
    # Запуск MCP сервера
    logger.info("Запуск MCP сервера для работы с БД Skyeng Platform")
    
    # Проверяем доступные БД при запуске
    try:
        available_dbs = list(db_manager.connections.keys())
        logger.info(f"Загружено БД: {len(available_dbs)}")
    except Exception as e:
        logger.error(f"Ошибка при проверке доступных БД: {e}")
    
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())