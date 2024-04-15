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
    from dml import DMLManager
    dml_manager = DMLManager()

    if 'type' in command:  # Check if it's a DML query
        if command['type'] == 'insert':
            return dml_manager.insert(command['table'], command['data'])
        elif command['type'] == 'delete':
            return dml_manager.delete(command['table'], command['conditions'])
        elif command['type'] == 'select':
            # Handling aggregation functions specifically
            if any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
                return handle_aggregations(command, dml_manager)
            else:
                return dml_manager.select(command['table'], command['columns'], command['conditions'])
    else:  # It's an aggregation query
        if command['function'] == 'MAX':
            return aggregate_max(command, dml_manager)
        elif command['function'] == 'MIN':
            return aggregate_min(command, dml_manager)
        elif command['function'] == 'SUM':
            return aggregate_sum(command, dml_manager)
    
    return None

def handle_aggregations(command, dml_manager):
    # Extract the aggregation function and column from the query
    import re
    match = re.match(r"(\w+)\((\w+)\)", command['columns'][0])
    if not match:
        return "Invalid aggregation function syntax"

    func, column = match.groups()
    data = dml_manager.select(command['table'], [column], command['conditions'])
    if func.upper() == 'MAX':
        return max(row[column] for row in data)
    elif func.upper() == 'MIN':
        return min(row[column] for row in data)
    elif func.upper() == 'SUM':
        return sum(row[column] for row in data)

def aggregate_max(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    return max(row[command['column']] for row in data)

def aggregate_min(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    return min(row[command['column']] for row in data)

def aggregate_sum(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    total = 0
    for row in data:
        try:
            # Convert to int before summing; use float if decimals are possible.
            total += int(row[command['column']])
        except ValueError:
            # Log or handle the case where conversion is not possible
            print(f"Warning: Non-numeric data encountered in {command['column']} and will be ignored: {row[command['column']]}")
    return total


# def execute_query(command):
#     dml_manager = DMLManager()

#     if 'type' in command:  # Check if it's a DML query
#         if command['type'] == 'insert':
#             return dml_manager.insert(command['table'], command['data'])
#         elif command['type'] == 'delete':
#             return dml_manager.delete(command['table'], command['conditions'])
#         elif command['type'] == 'select':
#             return dml_manager.select(command['table'], command['columns'], command['conditions'])
#     else:  # It's an aggregation query
#         if command['function'] == 'MAX':
#             return aggregate_max(command, dml_manager)
#         elif command['function'] == 'MIN':
#             return aggregate_min(command, dml_manager)
#         elif command['function'] == 'SUM':
#             return aggregate_sum(command, dml_manager)
    
#     return None

# def aggregate_max(command, dml_manager):
#     """Compute the maximum value of a column in a table."""
#     data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
#     return max(row[command['column']] for row in data)

# def aggregate_min(command, dml_manager):
#     """Compute the minimum value of a column in a table."""
#     data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
#     return min(row[command['column']] for row in data)

# def aggregate_sum(command, dml_manager):
#     """Compute the sum of a column in a table."""
#     data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
#     return sum(int(row[command['column']]) for row in data if row[command['column']].isdigit())


if __name__ == "__main__":
    print(execute_query({"function": "MAX", "table": "state_population", "column": "monthly_state_population"}))
    print(execute_query({"function": "MIN", "table": "county_count", "column": "count_alldrug"}))
    print(execute_query({"function": "SUM", "table": "population", "column": "county_count"}))


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
