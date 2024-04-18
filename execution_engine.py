# EXECUTION_ENGINE

from dml import DMLManager
from ddl import DDLManager
from storage import StorageManager
import logging
import re

# Configure logging to display debug information
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def execute_query(command):
    print(f"Debug execution engine execute query: {command}")
    storage_manager = StorageManager()
    dml_manager = DMLManager(storage_manager)
    ddl_manager = DDLManager()

    try:
        if 'type' in command:
            if command['type'] == 'select':
                conditions = command.get('where_clause', "")
                print(f"EE Debug: Columns parameter being passed: {command['columns']}")
                result = dml_manager.select(command['main_table'], command['columns'], conditions)  # Assign result here
                # result = dml_manager.select(command['main_table'], ['state_code', 'monthly_state_population'], conditions)
                if 'join' in command and command['join'] is not None:
                    return handle_join(command, dml_manager, storage_manager)
                if 'group_by' in command and command['group_by'] is not None:
                    aggregate_info = command['columns'][1]
                    match = re.search(r"(\w+)\((\w+)\)", aggregate_info)
                    if match:
                        aggregation_type = match.group(1).lower()
                        aggregate_column = match.group(2)
                        result = handle_group_by(result, command['group_by'], aggregate_column, aggregation_type)
                    else:
                        print("Error: Failed to parse aggregation details")
                        return None  # Ensure to handle this case properly

                    if result:
                        print("Final aggregated results:", result)
                        return result
                    else:
                        print("Error: No aggregated data found")
                        return "Error: No result found or error occurred"
    
                    # else:
                    #     print("Error: Failed to parse aggregation details")
                    
                elif 'order_by' in command and command['order_by'] is not None:  # Check if result is not None before using it
                    print("Debug: Result before select operation:", result)
                    order_direction = command.get('order_direction', 'ASC')
                    result = handle_order_by(result, command['order_by'], order_direction)
                    print("Debug: Result after select operation:", result) 
                    return result
                elif any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM', 'AVG', 'COUNT']):
                    return handle_aggregations(command, dml_manager, conditions)
                else:
                    return result  # Return result if no order_by or aggregation
            elif command['type'] == 'insert':
                return dml_manager.insert(command['table'], command['data'])
            elif command['type'] == 'delete':
                return dml_manager.delete(command['table'], command['conditions'])
            elif command['type'] == 'create':
                return ddl_manager.create_table(command['table_name'], command['columns'])
            elif command['type'] == 'drop':
                return ddl_manager.drop_table(command['table_name'])
            elif command['type'] == 'update':
                result = dml_manager.update(command['tables'], command['values'],command['where_condition'])
            
        else:
            logging.error("Unsupported or missing SQL command type")
            return "Unsupported or missing SQL command type"
    except Exception as e:
        logging.error(f"Execution error: {e}")
        return f"Execution error: {e}"

def apply_where_clause(data, where_clause):
    import operator
    ops = {
        '=': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '<': operator.lt,
        '>=': operator.ge,
        '<=': operator.le
    }

    def evaluate(item):
        # Simple parser for conditions; assumes well-formed input
        conditions = where_clause.split(' AND ')
        for condition in conditions:
            left, op, right = [x.strip() for x in re.split(r"(=|!=|>|<|>=|<=)", condition)]
            if op in ['=', '!=', '>', '<', '>=', '<=']:
                # Handle quoted string comparison for direct values
                if right.startswith("'") or right.startswith('"'):
                    right = right[1:-1]  # Remove quotes
                elif right.isdigit():
                    right = int(right)
                if not ops[op](item[left], right):
                    return False
        return True

    return [row for row in data if evaluate(row)]

def parse_condition(condition):
    import re
    pattern = r'(\w+\.\w+)\s*(<=|>=|<>|!=|<|>|=)\s*(\w+\.\w+|\'.+?\')'
    match = re.search(pattern, condition)
    return match.group(1), match.group(2), match.group(3)

def evaluate_condition(left_value, right_value, operator):
    # logging.debug(f"Evaluating condition: {left_value} {operator} {right_value}")
    if operator == '=':
        return left_value == right_value
    elif operator == '<':
        return left_value < right_value
    elif operator == '>':
        return left_value > right_value
    elif operator == '!=' or operator == '<>':
        return left_value != right_value
    elif operator == '<=':
        return left_value <= right_value
    elif operator == '>=':
        return left_value >= right_value
    else:
        raise ValueError("Unsupported operator")

def handle_join(command, dml_manager, storage_manager):
    # logging.debug("Starting handle_join function")
    main_table, main_alias = command['main_table'].split(' AS ')
    join_clause = command['join']
    join_table, join_alias = join_clause.replace('JOIN ', '').split(' ON ')[0].split(' AS ')
    
    # logging.debug(f"Main table: {main_table}, Alias: {main_alias}")
    # logging.debug(f"Join table: {join_table}, Alias: {join_alias}")

    columns_to_return = {col.split('.')[1]: col.split('.')[0] for col in command['columns']}
    # logging.debug(f"Columns to return: {columns_to_return}")

    main_data = storage_manager.get_table_data(main_table.strip())
    join_data = storage_manager.get_table_data(join_table.strip())

    # logging.debug(f"Main table data: {main_data}")
    # logging.debug(f"Join table data: {join_data}")

    joined_data = []
    join_condition = join_clause.split(' ON ')[1]
    left_field_with_alias, operator, right_field_with_alias = parse_condition(join_condition)

    # logging.debug(f"Join condition parsed: {left_field_with_alias} {operator} {right_field_with_alias}")

    left_field_alias, left_field = left_field_with_alias.split('.')
    right_field_alias, right_field = right_field_with_alias.split('.')

    for main_item in main_data:
        for join_item in join_data:
            # logging.debug(f"Comparing main item {main_item} with join item {join_item}")
            if evaluate_condition(main_item[left_field], join_item[right_field], operator):
                merged_item = {}
                for col, alias in columns_to_return.items():
                    if alias == main_alias.strip():
                        merged_item[f"{alias}.{col}"] = main_item[col]
                    if alias == join_alias.strip():
                        merged_item[f"{alias}.{col}"] = join_item[col]
                if merged_item not in joined_data:
                    joined_data.append(merged_item)
                    logging.debug(f"Joined item: {merged_item}")


    # logging.debug(f"Final joined data: {joined_data}")
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
    
    aggregator = getattr(dml_manager, func.lower(), None)
    if callable(aggregator):
        print(f"EE Debug: Aggregation function: {func}({column})")
        print(f"EE Debug: Conditions: {conditions}")
        result = aggregator(command['main_table'], column, conditions)
        print(f"EE Debug: Aggregation result: {result}")
        return result
    else:
        logging.error(f"DMLManager does not support {func} aggregation.")
        return f"DMLManager does not support {func} aggregation."

def handle_select(command, dml_manager):
    if any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
        return handle_aggregations(command, dml_manager)
    else:
        return dml_manager.select(command['main_table'], command['columns'], command['conditions'])

def handle_group_by(data, group_by_clause, aggregate_column, aggregation_type):
    grouped_data = {}
    for row in data:
        key = row.get(group_by_clause)
        if key not in grouped_data:
            grouped_data[key] = []
        grouped_data[key].append(row)

    aggregated_data = {}
    for key, rows in grouped_data.items():
        values = [safe_get_value(row, aggregate_column) for row in rows]
        if aggregation_type == 'avg':
            aggregated_value = sum(values) / len(values) if values else 0
        elif aggregation_type == 'sum':
            aggregated_value = sum(values)
        elif aggregation_type == 'count':
            aggregated_value = len(rows)
        
        aggregated_data[key] = aggregated_value

    return aggregated_data

def safe_get_value(row, column_name):
    try:
        # Convert the value to float before returning
        return float(row[column_name])  # Use float in case the data has decimal points
    except KeyError:
        print(f"Error: Column {column_name} not found in row.")
        return 0
    except ValueError:
        print(f"Error: Non-numeric data in {column_name}, cannot aggregate.")
        return 0

def handle_having(grouped_data, having_clause):
    print("EE ebug: Applying HAVING clause:", having_clause)
    filtered_data = {}
    
    # Iterate over grouped data
    for group_key, group_data in grouped_data.items():
        print("EE Debug: Group key:", group_key, "Group data:", group_data)
        
        # Check if group_data is a list
        if isinstance(group_data, list):
            print("EE Debug: Group data is a list.")
            try:
                # Evaluate the HAVING clause
                if eval(having_clause, {'sum': sum, 'len': len}, {'group_data': group_data}):
                    # Add the group data to filtered_data
                    filtered_data[group_key] = group_data
            except Exception as e:
                print("Error occurred during evaluation:", e)
        else:
            print("EE Debug: Group data is not a list.")

    return filtered_data

def handle_order_by(data, order_by_clause, order_direction):
    # Check if data needs to be converted to numeric type before ordering
    if data:
        field, _ = order_by_clause.split()
        convert_to_numeric(data, field)
    # Handling logic for ORDER BY clause
    print(f"Debug: Applying ORDER BY clause: {order_by_clause}")
    print(f"Debug: Order direction: {order_direction}")
    field, order = order_by_clause.split()
    reverse = True if order.upper() == 'DESC' else False
    sorted_data = sorted(data, key=lambda x: int(x[field]) if isinstance(x[field], int) else x[field], reverse=reverse)
    return sorted_data

def convert_to_numeric(data, field):
    for row in data:
        if field in row:
            try:
                row[field] = int(row[field])
            except ValueError:
                pass
    return data

def check_data_type(data):
    # Check the data type of the first value in the dataset
    if data:
        first_value = next(iter(data[0].values()))
        data_type = type(first_value)
        print(f"Data type of the ordered column: {data_type}")
    else:
        print("Data is empty, unable to determine data type.")

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

def aggregate_avg(command, dml_manager):
    data = dml_manager.select(command['main_table'], [command['column']], command.get('conditions'))
    total = sum(item[command['column']] for item in data if item[command['column']] is not None)
    count = sum(1 for item in data if item[command['column']] is not None)
    return total / count if count != 0 else 0

def aggregate_count(command, dml_manager):
    data = dml_manager.select(command['main_table'], ['*'], command.get('conditions'))
    return len(data)

def order_data(data, order_clause):
    import operator
    field, order = order_clause.split()
    reverse = True if order.upper() == 'DESC' else False
    return sorted(data, key=lambda x: x[field], reverse=reverse)

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

