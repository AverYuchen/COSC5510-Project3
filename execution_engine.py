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

    # def execute_query(self, command):
    #     try:
    #         handler = getattr(self, f"handle_{command['type']}", self.handle_unsupported)
    #         return handler(command)
    #     except Exception as e:
    #         logging.error(f"Execution error: {e}")
    #         return f"Execution error: {e}"

    def execute_query(self, command):
        try:
            # Mapping command types to handler functions
            handler = getattr(self, f"handle_{command['type'].lower()}", self.handle_unsupported)
            return handler(command)
        except Exception as e:
            logging.error(f"Execution error: {e}")
            return f"Execution error: {e}"

    def handle_select(self, command):
        main_table = command['main_table']
        data = self.dml_manager.select(main_table, command['columns'], command.get('where_clause'))
        
        # Check for the presence of any aggregation functions in the columns specification
        if 'columns' in command and command['columns']:
            if any(func in command['columns'][0].upper() for func in ['MAX', 'MIN', 'SUM', 'AVG', 'COUNT']):
                # If an aggregation function is found, handle the aggregation
                return self.handle_aggregations(command, self.dml_manager, command.get('where_clause'))
        
        if 'join' in command and command['join']:
            select_columns = command['columns']  # This assumes that columns are specified in command.
            for join in command['join']:
                data = self.handle_join(data, join, main_table, select_columns)


        if 'group_by' in command and command['group_by']:
            data = self.handle_group_by(data, command['group_by'], command['columns'])
            print("Data after grouping:", data)

        if 'order_by' in command and command['order_by']:
            data = self.handle_order_by(data, command['order_by'])

        if 'having' in command and command['having']:
            data = self.handle_having(data, command['having'])

        return data
    
    def filter_select_columns(self, data, select_columns):
        filtered_data = []
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
        return filtered_data

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
        main_table_name, main_alias = self.parse_table_alias(main_table)
        join_table_name, join_alias = self.parse_table_alias(join['join_table'])
        main_data = self.storage_manager.get_table_data(main_table_name)
        join_data = self.storage_manager.get_table_data(join_table_name)
        left_field, right_field = self.parse_join_condition(join['join_condition'])

        join_type = join.get('join_type', 'INNER').upper()
        method = {
            'LEFT JOIN': self.left_join,
            'RIGHT JOIN': self.right_join,
            'INNER JOIN': self.inner_join,
            'JOIN': self.inner_join  # Treat generic JOIN as INNER JOIN
        }.get(join_type, self.unsupported_join)

        return method(main_data, join_data, left_field, right_field, join_alias, select_columns)
        
    def parse_join_condition(self, condition):
        left, _, right = condition.partition('=')
        return left.strip(), right.strip()
    
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
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'INNER JOIN', select_columns)
    
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
            agg_match = re.match(r'(\w+)\((\w+)\)(?: AS (\w+))?', col)
            if agg_match:
                agg_func, column_name, alias = agg_match.groups()
                agg_funcs[column_name] = (agg_func.upper(), alias or f"{agg_func.upper()}({column_name})")

        # Grouping data
        for row in data:
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
    
    # def handle_create_index(self, command):
    #     """Handle the creation of an index."""
    #     try:
    #         # Assuming DDLManager can handle index creation
    #         return self.ddl_manager.create_index(command['table_name'], command['column_name'], command['index_name'])
    #     except Exception as e:
    #         logging.error(f"Error creating index: {e}")
    #         return f"Error creating index: {e}"
    
    def handle_create(self, command):
        return self.ddl_manager.create_table(command['table_name'], command['columns'])

    def handle_drop(self, command):
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

# Example usage
if __name__ == "__main__":
    engine = ExecutionEngine()
    
    # command_1 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'INNER JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    # command_2 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'LEFT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}  
    # command_3 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t2.A', 't1.B', 't2.B'], 'join': [{'join_type': 'RIGHT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    # command_4 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t2.A', 't1.B', 't2.B'], 'join': [{'join_type': 'JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    # command_5 = {'type': 'select', 'main_table': 'state_population', 'columns': ['state_code', 'monthly_state_population'], 'join': [], 'where_clause': None, 'group_by': None, 'order_by': 'monthly_state_population ASC', 'having': None}
    # command_6 = {'type': 'select', 'main_table': 'state_population', 'columns': ['state_code', 'monthly_state_population'], 'join': [], 'where_clause': None, 'group_by': None, 'order_by': 'monthly_state_population DESC', 'having': None}
    
    command_5 = {'type': 'DROP_INDEX', 'index_name': 'index_id', 'table_name': 'TestTable1'}
    command_6 = {'type': 'CREATE_INDEX', 'index_name': 'index_id', 'table_name': 'TestTable1', 'column_name': 'A'}


    # print(command_1)
    # print(engine.execute_query(command_1))
    # print(command_2)
    # print(engine.execute_query(command_2))
    # print(command_3)
    # print(engine.execute_query(command_3))
    # print(command_4)
    # print(engine.execute_query(command_4))
    
    
    print(command_5)
    print(engine.execute_query(command_5))
    print(command_6)
    print(engine.execute_query(command_6))


    
