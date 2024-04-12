from storage_manager import StorageManager
from index_manager import IndexManager
from ddl import create_table, drop_table
from dml import insert, delete, select, update

# Initialize the storage and index managers
storage_manager = StorageManager()
index_manager = IndexManager()

def execute_query(parsed_command):
    """
    Execute a parsed SQL command by dispatching it to the appropriate handler.
    """
    try:
        command_type = parsed_command['type']
        handlers = {
            'select': execute_select,
            'insert': execute_insert,
            'delete': execute_delete,
            'create': execute_create_table,
            'drop': execute_drop_table
        }
        if command_type in handlers:
            return handlers[command_type](parsed_command)
        return "Unsupported command type"
    except Exception as e:
        return f"Execution error: {str(e)}"

def execute_select(parsed_command):
    return select(parsed_command)

def execute_insert(parsed_command):
    result = insert(parsed_command)
    if "success" in result.lower():
        index_manager.update_index(parsed_command['table'], parsed_command['values'])
    return result

def execute_delete(parsed_command):
    result = delete(parsed_command)
    if "success" in result.lower():
        index_manager.update_index_on_delete(parsed_command['table'], parsed_command['conditions'])
    return result

def execute_create_table(parsed_command):
    return create_table(parsed_command)

def execute_drop_table(parsed_command):
    return drop_table(parsed_command)

# Example usage for testing
if __name__ == "__main__":
    query = {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': 'id > 10'}
    print(execute_query(query))
