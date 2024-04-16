

# execution_engine.py

# def execute(parsed_sql):
#     logging.debug(f"Debug Executing parsed SQL: {parsed_sql}")
#     try:
#         if parsed_sql['type'] == 'SELECT':
#             # Simulating a database fetch operation
#             # You should replace this with the actual database interaction logic
#             result = simulate_database_fetch(parsed_sql)
#             logging.debug(f"Execution result: {result}")
#             return result
#         else:
#             # Handle other types of SQL operations
#             pass
#     except Exception as e:
#         logging.error(f"Execution error: {str(e)}")
#         return None

# def execute_query(command):
#     from dml import DMLManager
#     dml_manager = DMLManager()

#     print(f"Debug: Executing parsed SQL: {command}")  # Log the parsed SQL command


#     if 'type' in command:  # Check if it's a DML query
#         if command['type'] == 'insert':
#             return dml_manager.insert(command['table'], command['data'])
#         elif command['type'] == 'delete':
#             return dml_manager.delete(command['table'], command['conditions'])
#         elif command['type'] == 'select':
#             # Handling aggregation functions specifically
#             if any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
#                 return handle_aggregations(command, dml_manager)
#             else:
#                 return dml_manager.select(command['table'], command['columns'], command['conditions'])
#     else:  # It's an aggregation query
#         if command['function'] == 'MAX':
#             return aggregate_max(command, dml_manager)
#         elif command['function'] == 'MIN':
#             return aggregate_min(command, dml_manager)
#         elif command['function'] == 'SUM':
#             return aggregate_sum(command, dml_manager)
    
#     return None

# Setup basic logging configuration
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

from dml import DMLManager
import logging

# def execute_query(command):
#     print(f"Debug execution engine execute query: {command}")  # Log input SQL for debugging
#     dml_manager = DMLManager()

#     try:
#         if 'type' in command:
#             if command['type'] == 'select':
#                 conditions = command.get('where_clause', "")
#                 if 'join' in command and command['join'] is not None:
#                     return handle_join(command, dml_manager)
#                 elif any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
#                     return handle_aggregations(command, dml_manager, conditions)
#                 else:
#                     return dml_manager.select(command['main_table'], command['columns'], conditions)
#             elif command['type'] == 'insert':
#                 return dml_manager.insert(command['table'], command['data'])
#             elif command['type'] == 'delete':
#                 return dml_manager.delete(command['table'], command['conditions'])
#             else:
#                 logging.error("Unsupported SQL command type")
#                 return None
#         else:
#             logging.error("Missing SQL command type")
#             return None
#     except Exception as e:
#         logging.error(f"Execution error: {e}")
#         return None

# def handle_join(self, command):
#     main_table_alias, main_table = command['main_table'].split(' AS ')
#     join_part = command['join']
#     join_table_alias, join_table = join_part.split(' ON ')[0].strip().split(' AS ')

#     main_data = self.storage_manager.get_table_data(main_table)
#     join_data = self.storage_manager.get_table_data(join_table)

#     # Assuming condition is like "a.column_name = b.column_name"
#     left_field, right_field = command['join'].split(' ON ')[1].split('=')
#     left_field = left_field.strip().split('.')[-1]
#     right_field = right_field.strip().split('.')[-1]

#     # Join data based on condition
#     joined_data = []
#     for main_item in main_data:
#         for join_item in join_data:
#             if main_item[left_field] == join_item[right_field]:
#                 # Properly merge data respecting aliases
#                 merged_item = {**{f"{main_table_alias}.{k}": v for k, v in main_item.items()}, 
#                                **{f"{join_table_alias}.{k}": v for k, v in join_item.items()}}
#                 joined_data.append(merged_item)
#     return

def execute_query(command):
    print(f"Debug execution engine execute query: {command}")
    dml_manager = DMLManager()

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
        else:
            logging.error("Unsupported or missing SQL command type")
            return "Unsupported or missing SQL command type"
    except Exception as e:
        logging.error(f"Execution error: {e}")
        return f"Execution error: {e}"
    

def handle_join(command, dml_manager):
    main_table, main_alias = extract_table_and_alias(command['main_table'])
    join_clause = command['join']
    join_table, join_alias = extract_table_and_alias(join_clause.split('ON')[0].strip())

    main_data = dml_manager.storage_manager.get_table_data(main_table)
    join_data = dml_manager.storage_manager.get_table_data(join_table)

    print(f"Main data: {main_data}")
    print(f"Join data: {join_data}")

    joined_data = []
    on_clause = command['join'].split('ON')[1].strip()
    left_field, right_field = [x.strip().split('.')[-1] for x in on_clause.split('=')]

    # Log detailed comparison information
    for main_item in main_data:
        for join_item in join_data:
            main_value = main_item.get(left_field, 'No data')
            join_value = join_item.get(right_field, 'No data')
            print(f"Comparing {main_value} from main table with {join_value} from join table")
            if main_value == join_value:
                print("Match found")
                merged_item = {**{f"{main_alias}.{k}": v for k, v in main_item.items()},
                               **{f"{join_alias}.{k}": v for k, v in join_item.items()}}
                joined_data.append(merged_item)
            else:
                print("No match found")

    print(f"Total joined rows: {len(joined_data)}")
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

if __name__ == "__main__":
    # Define a set of test cases to verify each type of SQL command
    test_queries = [
        {'type': 'select', 'main_table': 'state_abbreviation', 'columns': ['state'], 'join': None, 'where_clause': None},
        {'type': 'select', 'main_table': 'state_abbreviation', 'columns': ['*'], 'where_clause': None},
        {'type': 'select', 'main_table': 'state_abbreviation', 'columns': ['state'], 'where_clause': "state = 'Alaska'"},
        {'type': 'select', 'main_table': 'state_population', 'columns': ['*'], 'where_clause': "state_code = 'AK' AND year = '2018'"},
        {'type': 'insert', 'table': 'test_table', 'data': {'id': 2, 'name': 'Happy'}},
        {'type': 'delete', 'table': 'test_table', 'conditions': 'id = 1'},
        {'type': 'select', 'main_table': 'state_population', 'columns': ['MAX(monthly_state_population)']},
        {'type': 'select', 'main_table': 'state_population', 'columns': ['a.state_code', 'b.state'], 'join': 'JOIN state_abbreviation AS b ON a.state_code = b.state_code'}
    ]

    # Run tests
    for query in test_queries:
        result = execute_query(query)
        print(f"Test result for {query['type']} on {query.get('main_table', query.get('table'))}: {result}")
