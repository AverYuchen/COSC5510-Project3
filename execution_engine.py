# def execute_query(command):
#     from dml import DMLManager
#     dml_manager = DMLManager()
#     if command['type'] == 'select':
#         return dml_manager.select(command['table'], command.get('conditions'))
#     if command['type'] == 'insert':
#         if 'data' in command:
#             return dml_manager.insert(command['table'], command['data'])
#         else:
#             return "No data to insert"

from dml import DMLManager

def execute_query(command):
    dml_manager = DMLManager()
    if command['type'] == 'insert':
        return dml_manager.insert(command['table'], command['data'])
    elif command['type'] == 'delete':
        return dml_manager.delete(command['table'], command['conditions'])
    elif command['type'] == 'select':
        return dml_manager.select(command['table'], command['columns'], command['conditions'])
    # Add other conditions for different SQL types


if __name__ == "__main__":
    print(execute_query({"type": "insert", "table": "test_table", "data": {"id": 2, "name": "Alice"}}))
    print(execute_query({"type": "delete", "table": "test_table", "conditions": 'name = Alice'}))
    #print(execute_query({"type": "select", "table": "test_table", "columns" : {"id, name"}}))
    print(execute_query({"type": "select", "table": "test_table", "columns" : ['id', 'name'], "conditions":'id=1'}))
    
# from storage_manager import StorageManager
# from index_manager import IndexManager
# from ddl import create_table, drop_table
# from dml import DMLManager  # Import the class

# # Initialize the storage and index managers
# storage_manager = StorageManager()
# index_manager = IndexManager()

# # Create an instance of DMLManager
# dml_manager = DMLManager(storage_manager)

# def parse_conditions(conditions):
#     if not conditions:
#         return lambda row: True  # No conditions means select all

#     # Attempt to parse and handle simple comparison operators
#     operators = {
#         '=': lambda x, y: x == y,
#         '>': lambda x, y: x > y,
#         '<': lambda x, y: x < y,
#         '>=': lambda x, y: x >= y,
#         '<=': lambda x, y: x <= y,
#         '!=': lambda x, y: x != y
#     }
#     for op, func in operators.items():
#         if op in conditions:
#             field, value = conditions.split(op)
#             field = field.strip()
#             value = value.strip().strip("'")  # Remove any single quotes around the value
#             return lambda row, f=field, v=value, func=func: func(row.get(f, type(v)(v)), type(v)(v))

#     raise ValueError(f"Condition parsing error: {conditions}")

# def execute_select(parsed_command):
#     conditions = parsed_command.get('conditions')
#     condition_lambda = parse_conditions(conditions)
#     return dml_manager.select(parsed_command['table'], condition_lambda)


# def execute_query(parsed_command):
#     command_type = parsed_command['type']
#     if command_type == 'select':
#         return execute_select(parsed_command)
#     # Add other commands handling similarly
#     else:
#         return "Unsupported command type"

# def execute_insert(parsed_command):
#     result = dml_manager.insert(parsed_command['table'], parsed_command['values'])
#     # Your indexing operations here
#     return result

# def execute_delete(parsed_command):
#     result = dml_manager.delete(parsed_command['table'], parsed_command.get('conditions'))
#     # Your indexing operations here
#     return result

# def execute_create_table(parsed_command):
#     return create_table(parsed_command['table'], parsed_command['columns'])

# def execute_drop_table(parsed_command):
#     return drop_table(parsed_command['table'])

# # Example usage
# if __name__ == "__main__":
#     # Test the select function with various conditions
#     test_queries = [
#         {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': 'id > 10'},
#         {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': 'name = "Alice"'},
#         {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': 'id >= 10'},
#         {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': 'id != 10'},
#         {'type': 'select', 'table': 'users', 'columns': ['id', 'name'], 'conditions': ''}
#     ]
#     for query in test_queries:
#         print(execute_query(query))
