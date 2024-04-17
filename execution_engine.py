from dml import DMLManager
from ddl import DDLManager
from storage import StorageManager
import logging

def execute_query(command):
    print(f"Debug execution engine execute query: {command}")
    storage_manager = StorageManager()
    dml_manager = DMLManager(storage_manager)
    ddl_manager = DDLManager()
    

    try:
        if 'type' in command:
            if command['type'] == 'select':
                conditions = command.get('where_clause', "")
                if 'join' in command and command['join'] is not None:
                    return handle_join(command, dml_manager)
                elif any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
                    return handle_aggregations(command, dml_manager, conditions)
                else:
                    return dml_manager.select(command['main_table'], command['columns'], conditions)
            elif command['type'] == 'insert':
                return dml_manager.insert(command['table'], command['data'])
            elif command['type'] == 'delete':
                return dml_manager.delete(command['table'], command['conditions'])
            elif command['type'] == 'create':
                return ddl_manager.create_table(command['table_name'], command['columns'])
            elif command['type'] == 'drop':
                return ddl_manager.drop_table(command['table_name'])
        else:
            logging.error("Unsupported or missing SQL command type")
            return "Unsupported or missing SQL command type"
    except Exception as e:
        logging.error(f"Execution error: {e}")
        return f"Execution error: {e}"


def handle_join(command, dml_manager):
    dml_manager = DMLManager()
    # Assuming command contains something like: 'JOIN state_abbreviation AS b ON a.state_code = b.state_code'
    main_table, main_alias = command['main_table'].split(' AS ')
    join_clause = command['join']
    join_table, join_alias = join_clause.replace('JOIN ', '').split(' ON ')[0].split(' AS ')

    main_data = dml_manager.storage_manager.get_table_data(main_table.strip())
    join_data = dml_manager.storage_manager.get_table_data(join_table.strip())

    joined_data = []
    # Assuming 'ON a.state_code = b.state_code'
    join_condition = join_clause.split(' ON ')[1]
    left_field, right_field = join_condition.split(' = ')
    left_field = left_field.split('.')[1]  # Remove alias 'a.'
    right_field = right_field.split('.')[1]  # Remove alias 'b.'

    for main_item in main_data:
        for join_item in join_data:
            if main_item[left_field] == join_item[right_field]:
                # Adjust how data is merged based on required fields
                merged_item = {**main_item, **join_item}
                joined_data.append(merged_item)

    return joined_data



def evaluate_join_condition(row1, row2, condition):
    left, right = condition.split('=')
    column1 = left.split('.')[-1].strip()
    column2 = right.split('.')[-1].strip()
    return row1.get(column1) == row2.get(column2)


def extract_table_and_alias(table_expression):
    parts = table_expression.split(' AS ')
    if len(parts) > 1:
        # Ensure we remove any possible extra spaces that could affect matching
        return parts[0].strip(), parts[1].strip()
    return parts[0].strip(), None  # No alias, ensure no trailing spaces

def handle_aggregations(command, dml_manager, conditions):
    column = command['columns'][0].split('(')[1].split(')')[0]
    func = command['columns'][0].split('(')[0].strip().upper()
    
    aggregator = getattr(dml_manager, f"{func.lower()}", None)
    if callable(aggregator):
        return aggregator(command['main_table'], column, conditions)
    else:
        logging.error(f"DMLManager does not support {func} aggregation.")
        return f"DMLManager does not support {func} aggregation."

def handle_select(command, dml_manager):
    if any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
        return handle_aggregations(command, dml_manager)
    else:
        return dml_manager.select(command['main_table'], command['columns'], command['conditions'])

def aggregate_max(command, dml_manager):
    data = dml_manager.select(command['main_table'], [command['column']], command.get('conditions'))
    return max(row[command['column']] for row in data)

def aggregate_min(command, dml_manager):
    data = dml_manager.select(command['main_table'], [command['column']], command.get('conditions'))
    return min(row[command['column']] for row in data)

def aggregate_sum(command, dml_manager):
    data = dml_manager.select(command['main_table'], [command['column']], command.get('conditions'))
    total = 0
    for row in data:
        try:
            # Convert to int before summing; use float if decimals are possible.
            total += int(row[command['column']])
        except ValueError:
            # Log or handle the case where conversion is not possible
            print(f"Warning: Non-numeric data encountered in {command['column']} and will be ignored: {row[command['column']]}")
    return total

def simulate_database_fetch(command):
    # This is a placeholder function that should be replaced with actual database interaction
    # Returning a mock result
    return "Mock result: Data fetched successfully"


def evaluate(condition, row1, row2):
    # Basic evaluation of a join condition, e.g., 'table1.id = table2.ref_id'
    
    column1, column2 = condition.split('=')
    column1 = column1.strip()
    column2 = column2.strip()
    return row1[column1] == row2[column2]

# if __name__ == "__main__":
#     # Define a set of test cases to verify each type of SQL command
#     test_queries = [
#         {'type': 'select', 'main_table': 'state_population', 'columns': ['a.state_code', 'b.state'], 'join': 'JOIN state_abbreviation AS b ON a.state_code = b.state_code'}
#     ]

#     # Run tests
#     for query in test_queries:
#         result = execute_query(query)
#         print(f"Test result for {query['type']} on {query.get('main_table', query.get('table'))}: {result}")

if __name__ == "__main__":
    # Define a set of test cases to verify each type of SQL command
    '''
    test_queries = [
        {'type': 'create', 'table_name': 'employees', 'columns': {
            'id': 'INT PRIMARY_KEY',
            'name': 'VARCHAR(100)',
            'department_id': 'INT FOREIGN_KEY(departments,id) INDEX',
            'salary': 'DECIMAL INDEX'
        }},
        {'type': 'drop', 'table_name': 'employees'}
    ]

    # Run tests
    for query in test_queries:
        result = execute_query(query)
        print(f"Test result for {query['type']} on {query.get('table_name')}: {result}")
    '''
    print(execute_query({'type': 'insert', 'table': 'students', 'data': {'id': 'AJk', 'name': 'Ave'}}))
    #print(execute_query({'type': 'create', 'table_name': 'another_test', 'columns': {'id': {'type': 'INT', 'primary_key': False, 'index': False, 'foreign_key': None}, 'name': {'type': 'VARCHAR(50)', 'primary_key': True, 'index': True, 'foreign_key': None}}}))