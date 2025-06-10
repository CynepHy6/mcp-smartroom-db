import pytest
import importlib.util
import sys

# Импортируем DatabaseManager из файла с дефисом в имени
spec = importlib.util.spec_from_file_location("mcp_db_server", "./mcp-db-server.py")
mcp_db_server = importlib.util.module_from_spec(spec)
sys.modules["mcp_db_server"] = mcp_db_server
spec.loader.exec_module(mcp_db_server)
DatabaseManager = mcp_db_server.DatabaseManager

@pytest.fixture
def db_manager():
    # Можно передать фиктивный путь, т.к. для теста нужен только _validate_query
    return DatabaseManager(config_path=None)

def test_validate_query_allows_complex_select(db_manager):
    query = '''
    SELECT 
        r.hash,
        r.name,
        r.type,
        r.status,
        r.created_at,
        r.started_at,
        r.closed_at,
        lm.id as lesson_material_id,
        lm.name as lesson_material_name
    FROM room r
    JOIN room_participant rp ON rp.room_id = r.id
    JOIN lesson_material lm ON lm.id = rp.current_material_id
    WHERE r.hash IN ('bufadurelapu', 'zalevemaruzi', 'febefabagafo', 'vuvelevevela')
    ORDER BY r.started_at, lm.id;
    '''
    assert db_manager._validate_query(query) is True

    query = "SELECT \n    r.hash,\n    r.name,\n    r.type,\n    r.status,\n    r.created_at,\n    r.started_at,\n    r.closed_at,\n    lm.id as lesson_material_id,\n    lm.name as lesson_material_name\nFROM room r\nJOIN room_participant rp ON rp.room_id = r.id\nJOIN lesson_material lm ON lm.id = rp.current_material_id\nWHERE r.hash IN ('bufadurelapu', 'zalevemaruzi', 'febefabagafo', 'vuvelevevela')\nORDER BY r.started_at, lm.id;"

    assert db_manager._validate_query(query) is True 