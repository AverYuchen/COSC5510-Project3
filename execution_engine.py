# EXECUTION_ENGINE.py

from dml import DMLManager
from ddl import DDLManager
# from collections import defaultdict
from storage import StorageManager
import logging
import re

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class ExecutionEngine:
    def __init__(self):
        self.storage_manager = StorageManager()
        self.dml_manager = DMLManager(self.storage_manager)
        self.ddl_manager = DDLManager()

    def execute_query(self, command):
        try:
            handler = getattr(self, f"handle_{command['type'].lower()}", self.handle_unsupported)
            return handler(command)
        except Exception as e:
            logging.error(f"Execution error: {e}", exc_info=True)
            return f"Execution error: {e}"


    def handle_select(self, command):
        if 'main_table' not in command or not command['columns']:
            logging.error("Select command is missing 'main_table' or 'columns'")
            return "Invalid command format"
        main_table = command['main_table']
        data = self.dml_manager.select(main_table, ['*'])
        # data = self.dml_manager.select(main_table, command['columns'], command.get('where_clause'))
        
        # Check for the presence of any aggregation functions in the columns specification
        if 'columns' in command and command['columns']:
            if any(func in command['columns'][0].upper() for func in ['MAX', 'MIN', 'SUM', 'AVG', 'COUNT']):
                # If an aggregation function is found, handle the aggregation
                return self.handle_aggregations(command, self.dml_manager, command.get('where_clause'))
        
        if 'join' in command and command['join']:
            select_columns = command['columns']  # This assumes that columns are specified in command.
            for join in command['join']:
                data = self.handle_join(data, join, main_table, select_columns)
            
        if 'where_clause' in command:
            data = self.filter_data_by_condition(data, command['where_clause'])


        if 'group_by' in command and command['group_by']:
            data = self.handle_group_by(data, command['group_by'], command['columns'])
            print("Data after grouping:", data)

        if 'order_by' in command and command['order_by']:
            data = self.handle_order_by(data, command['order_by'])

        if 'having' in command and command['having']:
            data = self.handle_having(data, command['having'])

        #get the needed columns
        final_data = self.filter_select_columns(data, command['columns'])

        return final_data

    
    def filter_data_by_condition(self, data, where_clause):
        if where_clause is None:
            logging.debug("No where_clause provided, returning original data.")
            return data
        condition_function = self.parse_condition_to_function(where_clause)
        filtered_data = [row for row in data if condition_function(row)]
        return filtered_data

    def filter_select_columns(self, data, select_columns):
        if '*' in select_columns:
            return data  # If selecting all columns, return the data as is.
    
        final_data = []
        """
        for row in data:
            filtered_row = {}
            for col in select_columns:
                try:
                    table_alias, column_name = col.split('.')
                    key = f"{table_alias}.{column_name}"
                    if key in row:
                        filtered_row[key] = row[key]
                except ValueError:
                    logging.error(f"Invalid column format: {col}")
            filtered_data.append(filtered_row)
        """
        for row in data:
            filtered_data = {}
            for key, value in row.items():
                if key in select_columns:
                    filtered_data[key] = value
            final_data.append(filtered_data)
        return final_data

    def parse_join_condition(self, condition):
        try:
            left, _, right = condition.partition('=')
            left_table, left_column = left.strip().split('.')
            right_table, right_column = right.strip().split('.')
            return (f"{left_table}.{left_column}", f"{right_table}.{right_column}")
        except ValueError:
            logging.error(f"Invalid join condition format: {condition}")
            raise
        
    def parse_table_alias(self, table_expression):
        parts = table_expression.split(' AS ')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return table_expression.strip(), table_expression.strip()
    
    def handle_join(self, main_data, join, main_table, select_columns):
        logging.debug(f"Starting join operation: main_table={main_table}, join={join}")
        main_table_name, main_alias = self.parse_table_alias(main_table)
        join_table_name, join_alias = self.parse_table_alias(join['join_table'])

        main_data = self.storage_manager.get_table_data(main_table_name)
        join_data = self.storage_manager.get_table_data(join_table_name)
        
        if main_data is None or join_data is None:
            logging.error("Failed to retrieve data for joining: main_data or join_data is None")
            return []

        left_field, right_field = self.parse_join_condition(join['join_condition'])
        join_type = join.get('join_type', 'INNER').upper()

        logging.debug(f"Joining {main_table_name} with {join_table_name} on {left_field} = {right_field}")
        
        method = {
            'LEFT JOIN': self.left_join,
            'RIGHT JOIN': self.right_join,
            'INNER JOIN': self.inner_join,
            'JOIN': self.inner_join  # Treat generic JOIN as INNER JOIN
        }.get(join_type, self.unsupported_join)

        result = method(main_data, join_data, left_field, right_field, join_alias, select_columns)
        logging.debug(f"Join result: {result}")
        return result
    
    def parse_join_condition(self, condition):
        try:
            left, _, right = condition.partition('=')
            left_table, left_column = left.strip().split('.')
            right_table, right_column = right.strip().split('.')
            logging.debug(f"Parsed join condition: {left_table}.{left_column} = {right_table}.{right_column}")
            return (f"{left_table}.{left_column}", f"{right_table}.{right_column}")
        except ValueError as e:
            logging.error(f"Error parsing join condition '{condition}': {e}")
            raise

    def join_data(self, main_data, join_data, left_field, right_field, join_alias, join_type, select_columns):
        joined_data = []
        for main_row in main_data:
            matched = False
            main_value = main_row.get(left_field.split('.')[-1])  # Get the field without alias
            for join_row in join_data:
                join_value = join_row.get(right_field.split('.')[-1])  # Get the field without alias
                if main_value == join_value:
                    matched = True
                    # Build the merged row based on selected columns only
                    merged_row = {}
                    for col in select_columns:
                        table_alias, column_name = col.split('.')
                        if table_alias == join_alias:
                            # Use the join row if column belongs to the join table
                            merged_row[col] = join_row.get(column_name)
                        else:
                            # Use the main row if column belongs to the main table
                            merged_row[col] = main_row.get(column_name)
                    joined_data.append(merged_row)
            if not matched and join_type == 'LEFT JOIN':
                # Handle LEFT JOIN specifics
                null_row = {col: None for col in select_columns}
                joined_data.append({**main_row, **null_row})
        return joined_data

    def left_join(self, main_data, join_data, left_field, right_field, join_alias, select_columns):
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'LEFT JOIN', select_columns)

    def right_join(self, main_data, join_data, left_field, right_field, join_alias, select_columns):
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'RIGHT JOIN', select_columns)

    def inner_join(self, main_data, join_data, left_field, right_field, join_alias, select_columns):
        if not main_data or not join_data:
            logging.error("One of the datasets for joining is empty.")
            return []
        joined_data = self.join_data(main_data, join_data, left_field, right_field, join_alias, 'INNER JOIN', select_columns)
        logging.debug(f"Joined data: {joined_data}")
        return joined_data
    
    def unsupported_join(self, main_data, join_data, left_field, right_field, join_alias):
        logging.error("Unsupported join type")
        return main_data
    
    def handle_aggregations(self, command, data_manager, conditions=None):
        table = command['main_table']
        column = command['columns'][0]  # Assuming the column with aggregation function is always the first one
        match = re.match(r'(\w+)\((\w+)\)', column)
        if match:
            agg_func, agg_column = match.groups()
            data = data_manager.select(table, [agg_column], conditions)

            # Convert data to numeric values
            numeric_data = [self.safe_convert_to_numeric(item[agg_column]) for item in data if item[agg_column] is not None]

            # Perform the aggregation
            if agg_func.upper() == 'MAX':
                result = max(numeric_data)
            elif agg_func.upper() == 'MIN':
                result = min(numeric_data)
            elif agg_func.upper() == 'SUM':
                result = sum(numeric_data)
            elif agg_func.upper() == 'AVG':
                result = sum(numeric_data) / len(numeric_data) if numeric_data else None
            elif agg_func.upper() == 'COUNT':
                result = len(numeric_data)
            else:
                return "Unsupported aggregation function"

            return {column: result}

        return "No valid aggregation found"

    def handle_order_by(self, data, order_by_clause):
        import re
        column, order = re.split(r'\s+', order_by_clause)
        reverse = (order.upper() == 'DESC')
        return sorted(data, key=lambda x: x[column], reverse=reverse)

    def handle_having(self, grouped_data, having_clause):
        # Example simple HAVING clause handler; expand as needed
        result = []
        for item in grouped_data:
            if eval(having_clause, {}, item):
                result.append(item)
        return result
        
    def handle_insert(self, command):
        return self.dml_manager.insert(command['table'], command['data'])

    def handle_delete(self, command):
        return self.dml_manager.delete(command['table'], command['conditions'])

    def handle_update(self, command):
        return self.dml_manager.update(command['tables'], command['values'], command['where_condition'])

    def handle_unsupported(self, command):
        return "Unsupported command type"
        
    @staticmethod
    def safe_convert_to_numeric(value):
        try:
            return float(value)
        except ValueError:
            try:
                return int(value)
            except ValueError:
                logging.error(f"Conversion to numeric failed for value: {value}")
                return None
            
    def handle_group_by(self, data, group_by_column, columns):
        # Setup
        grouped_data = {}
        agg_funcs = {}

        # Parsing columns for aggregate functions and their intended aliases
        for col in columns:
            # This regex extracts the aggregation function, the column it acts on, and an alias if provided
            agg_match = re.match(r'(\w+)\((\w+)\)(?: AS (\w+))?', col)
            if agg_match:
                agg_func, column_name, alias = agg_match.groups()
                # If an alias is specified, it should be used as the key in the output
                agg_funcs[column_name] = (agg_func.upper(), alias or f"{agg_func.upper()}({column_name})")

        # Grouping data
        for row in data:
            # The key for grouping
            key = row[group_by_column]
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(row)

        # Applying aggregation
        result = []
        for key, rows in grouped_data.items():
            aggregated_row = {group_by_column: key}
            for column_name, (agg_func, alias) in agg_funcs.items():
                column_values = [self.safe_convert_to_numeric(row[column_name]) for row in rows if column_name in row and row[column_name] is not None]
                if column_values:
                    if agg_func == 'AVG':
                        aggregated_row[alias] = sum(column_values) / len(column_values)
                    elif agg_func == 'SUM':
                        aggregated_row[alias] = sum(column_values)
                    elif agg_func == 'MAX':
                        aggregated_row[alias] = max(column_values)
                    elif agg_func == 'MIN':
                        aggregated_row[alias] = min(column_values)
                    elif agg_func == 'COUNT':
                        aggregated_row[alias] = len(column_values)
            result.append(aggregated_row)

        return result

    def handle_create(self, command):
        return self.ddl_manager.create_table(command['table_name'], command['columns'])

    def handle_drop_table(self, command):
        return self.ddl_manager.drop_table(command['table_name'])
    
    def handle_create_index(self, command):
        """Handle the creation of an index."""
        try:
            index_name = command['index_name']
            table_name = command['table_name']
            column_name = command['column_name']
            return self.execute_create_index(index_name, table_name, column_name)
        except Exception as e:
            logging.error(f"Error creating index: {e}")
            return f"Error creating index: {e}"

    def execute_create_index(self, index_name, table_name, column_name):
        """Executes index creation in the storage manager."""
        if not self.storage_manager.table_exists(table_name):
            return "Error: Table does not exist."
        if not self.storage_manager.column_exists(table_name, column_name):
            return "Error: Column does not exist."
        return self.storage_manager.create_index(table_name, column_name, index_name)
    
    def handle_drop_index(self, command):
        """Handle the dropping of an index."""
        try:
            return self.ddl_manager.drop_index(command['table_name'], command['index_name'])
        except Exception as e:
            logging.error(f"Error dropping index: {e}")
            return f"Error dropping index: {e}"
    
    def parse_condition_to_function(self, where_clause):
        def eval_condition(row, conditions):
            if 'AND' in conditions:
                left, right = conditions.split('AND', 1)
                return eval_condition(row, left.strip()) and eval_condition(row, right.strip())
            if 'OR' in conditions:
                left, right = conditions.split('OR', 1)
                return eval_condition(row, left.strip()) or eval_condition(row, right.strip())
            
            pattern = r"(\w+)\s*(=|!=|<>|<|>|<=|>=|LIKE|IN|BETWEEN)\s*(.*)"
            match = re.match(pattern, conditions.strip(), re.IGNORECASE)
            if not match:
                raise ValueError("Invalid WHERE clause format")
            column, operator, value = match.groups()
            return self.apply_operator(row, column.strip(), operator.strip().upper(), value.strip().strip("'"))

        return lambda row: eval_condition(row, where_clause)
    
    def apply_operator(self, row, column, operator, value):
        logging.debug(f"Applying operator: {operator} on column: {column} with value: {value}")
        
        if operator == "IN":
            values = eval(value)
            result = self.safe_convert_to_numeric_where(row.get(column)) in values
            logging.debug(f"IN operator result: {result}")
            return result
        elif operator == "LIKE":
            regex = re.compile("^" + value.replace('%', '.*') + "$")
            result = regex.match(row.get(column)) is not None
            logging.debug(f"LIKE operator result: {result}")
            return result
        elif "BETWEEN" in operator:
            # Ensure proper handling of BETWEEN
            value = value[len("BETWEEN"):].strip()  # Remove the 'BETWEEN' keyword and strip any leading/trailing whitespace
        else:
            value = self.safe_convert_to_numeric_where(value)
            if operator == "=": operator = "=="
            print(row)
            result = self.compare_values(row.get(column), value, operator)
            logging.debug(f"Comparison operator {operator} result: {result}")
            return result
    
    def compare_values(self, row_value, value, operator):
        print(row_value)
        row_value = self.safe_convert_to_numeric_where(row_value)
        if operator == "==":
            return row_value == value
        elif operator == "!=":
            return row_value != value
        elif operator == "<":
            return row_value < value
        elif operator == ">":
            return row_value > value
        elif operator == "<=":
            return row_value <= value
        elif operator == ">=":
            return row_value >= value

    def safe_convert_to_numeric_where(self, value):
        try:
            return float(value) if '.' in value or 'e' in value.lower() else int(value)
        except ValueError:
            return value  # Return as string if conversion isn't possible
        
# Example usage
if __name__ == "__main__":
    engine = ExecutionEngine()
    
    # command_1 = {'type': 'select', 'main_table': 'state_population', 'columns': ['*'], 'join': [], 'where_clause': "state_code = 'AK' AND year = '2018'", 'group_by': None, 'order_by': None, 'having': None}
    # {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'INNER JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    command_2 = {'type': 'select', 'main_table': 'state_population', 'columns': ['state_code', 'AVG(monthly_state_population) AS average_population'], 'join': [], 'where_clause': None, 'group_by': 'state_code', 'order_by': None, 'having': None}
    # {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'LEFT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}  
    # command_3 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t2.A', 't1.B', 't2.B'], 'join': [{'join_type': 'RIGHT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    # command_4 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't2.A', 't2.B'], 'join': [{'join_type': 'INNER JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': 't1.A > 7', 'group_by': None, 'order_by': None, 'having': None}
    # command_5 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't2.A', 't2.B'], 'join': [{'join_type': 'LEFT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': 't1.A != 7', 'group_by': None, 'order_by': None, 'having': None}
    # command_6 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't2.A', 't2.B'], 'join': [{'join_type': 'INNER JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': 't1.A IN (2,3,4)', 'group_by': None, 'order_by': None, 'having': None}
    

    # print(command_1)
    # print(engine.execute_query(command_1))
    print(command_2)
    print(engine.execute_query(command_2))
    # print(command_3)
    # print(engine.execute_query(command_3))
    # print(command_4)
    # print(engine.execute_query(command_4))
    # print(command_5)
    # print(engine.execute_query(command_5))
    # print(command_6)
    # print(engine.execute_query(command_6))


    