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
            handler = getattr(self, f"handle_{command['type']}", self.handle_unsupported)
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
            for join in command['join']:
                data = self.handle_join(data, join, main_table)

        if 'group_by' in command and command['group_by']:
            data = self.handle_group_by(data, command['group_by'], command['columns'])
            print("Data after grouping:", data)

        if 'order_by' in command and command['order_by']:
            data = self.handle_order_by(data, command['order_by'])

        if 'having' in command and command['having']:
            data = self.handle_having(data, command['having'])

        return data

    
    def parse_table_alias(self, table_expression):
        parts = table_expression.split(' AS ')
        if len(parts) == 2:
            return parts[0].strip(), parts[1].strip()
        return table_expression.strip(), table_expression.strip()

    def handle_join(self, main_data, join, main_table):
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

        return method(main_data, join_data, left_field, right_field, join_alias)    
        
    def parse_join_condition(self, condition):
        left, _, right = condition.partition('=')
        return left.strip(), right.strip()

    def join_data(self, main_data, join_data, left_field, right_field, join_alias, join_type):
        joined_data = []
        for main_row in main_data:
            matched = False
            main_value = main_row.get(left_field.split('.')[-1])  # Get the field without alias
            for join_row in join_data:
                join_value = join_row.get(right_field.split('.')[-1])  # Get the field without alias
                if main_value == join_value:
                    matched = True
                    merged_row = {**main_row, **{f"{join_alias}.{k}": v for k, v in join_row.items()}}
                    joined_data.append(merged_row)
            if not matched and join_type in ['LEFT JOIN', 'RIGHT JOIN']:
                null_row = {f"{join_alias}.{k}": None for k in join_data[0].keys()}
                if join_type == 'LEFT JOIN':
                    joined_data.append({**main_row, **null_row})
                else:
                    joined_data.append({**null_row, **join_row})
        return joined_data

    def left_join(self, main_data, join_data, left_field, right_field, join_alias):
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'LEFT JOIN')

    def right_join(self, main_data, join_data, left_field, right_field, join_alias):
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'RIGHT JOIN')

    def inner_join(self, main_data, join_data, left_field, right_field, join_alias):
        return self.join_data(main_data, join_data, left_field, right_field, join_alias, 'INNER JOIN')

    def unsupported_join(self, main_data, join_data, left_field, right_field, join_alias):
        logging.error("Unsupported join type")
        return main_data
    
    # def handle_aggregations(self, data, agg_func, column_name):
    #     numeric_data = [float(row[column_name]) for row in data if column_name in row and row[column_name] is not None]

    #     if agg_func == 'MAX':
    #         return max(numeric_data)
    #     elif agg_func == 'MIN':
    #         return min(numeric_data)
    #     elif agg_func == 'SUM':
    #         return sum(numeric_data)
    #     elif agg_func == 'AVG':
    #         return sum(numeric_data) / len(numeric_data) if numeric_data else None
    #     elif agg_func == 'COUNT':
    #         return len(data)
    #     else:
    #         return None
    
    # def handle_aggregations(self, command, data_manager, conditions=None):
    #     table = command['main_table']
    #     column = command['columns'][0]  # Assuming the column with aggregation function is always the first one
    #     match = re.match(r'(\w+)\((\w+)\)', column)
    #     if match:
    #         agg_func, agg_column = match.groups()
    #         data = data_manager.select(table, [agg_column], conditions)

    #         # Perform the aggregation
    #         if agg_func.upper() == 'MAX':
    #             result = max(item[agg_column] for item in data if item[agg_column] is not None)
    #         elif agg_func.upper() == 'MIN':
    #             result = min(item[agg_column] for item in data if item[agg_column] is not None)
    #         elif agg_func.upper() == 'SUM':
    #             result = sum(item[agg_column] for item in data if item[agg_column] is not None)
    #         elif agg_func.upper() == 'AVG':
    #             values = [item[agg_column] for item in data if item[agg_column] is not None]
    #             result = sum(values) / len(values) if values else None
    #         elif agg_func.upper() == 'COUNT':
    #             result = len([item for item in data if item[agg_column] is not None])
    #         else:
    #             return "Unsupported aggregation function"

    #         return {column: result}

    #     return "No valid aggregation found"
    
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

    def handle_create(self, command):
        return self.ddl_manager.create_table(command['table_name'], command['columns'])

    def handle_drop(self, command):
        return self.ddl_manager.drop_table(command['table_name'])

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



    # def handle_group_by(self, data, group_by_column, columns):
    #     # Initialize an empty dictionary to emulate defaultdict(list)
    #     grouped_data = {}
    #     agg_funcs = {}
    #     for col in columns:
    #         match = re.match(r'(\w+)\((\w+)\)', col)
    #         if match:
    #             agg_func, column_name = match.groups()
    #             agg_funcs[column_name] = agg_func.upper()

    #     # Group the data by the specified column
    #     for row in data:
    #         key = row[group_by_column]
    #         if key not in grouped_data:
    #             grouped_data[key] = []
    #         grouped_data[key].append(row)

    #     # Process grouped data
    #     result = []
    #     for key, rows in grouped_data.items():
    #         aggregated_row = {group_by_column: key}
    #         for column_name, agg_func in agg_funcs.items():
    #             # Ensure values are converted properly
    #             column_values = [ExecutionEngine.safe_convert_to_numeric(row[column_name]) 
    #                              for row in rows if column_name in row and row[column_name] is not None]

    #             if column_values:
    #                 if agg_func == 'AVG':
    #                     aggregated_row[column_name] = sum(column_values) / len(column_values)
    #                 elif agg_func == 'SUM':
    #                     aggregated_row[column_name] = sum(column_values)
    #                 elif agg_func == 'MAX':
    #                     aggregated_row[column_name] = max(column_values)
    #                 elif agg_func == 'MIN':
    #                     aggregated_row[column_name] = min(column_values)
    #                 elif agg_func == 'COUNT':
    #                     aggregated_row[column_name] = len(column_values)
    #         result.append(aggregated_row)
    #     return result
                            


# Example usage
if __name__ == "__main__":
    engine = ExecutionEngine()
    
    command_1 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'INNER JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    command_2 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t1.A', 't1.B', 't2.B'], 'join': [{'join_type': 'LEFT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}  
    command_3 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t2.A', 't1.B', 't2.B'], 'join': [{'join_type': 'RIGHT JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    command_4 = {'type': 'select', 'main_table': 'TestTable1 AS t1', 'columns': ['t2.A', 't1.B', 't2.B'], 'join': [{'join_type': 'JOIN', 'join_table': 'TestTable2 AS t2', 'join_condition': 't1.A = t2.A'}], 'where_clause': None, 'group_by': None, 'order_by': None, 'having': None}
    # command_5 = {'type': 'select', 'main_table': 'state_population', 'columns': ['state_code', 'monthly_state_population'], 'join': [], 'where_clause': None, 'group_by': None, 'order_by': 'monthly_state_population ASC', 'having': None}
    # command_6 = {'type': 'select', 'main_table': 'state_population', 'columns': ['state_code', 'monthly_state_population'], 'join': [], 'where_clause': None, 'group_by': None, 'order_by': 'monthly_state_population DESC', 'having': None}
    
    print(command_1)
    print(engine.execute_query(command_1))
    print(command_2)
    print(engine.execute_query(command_2))
    print(command_3)
    print(engine.execute_query(command_3))
    print(command_4)
    print(engine.execute_query(command_4))
    # print(command_5)
    # print(engine.execute_query(command_5))
    # print(command_6)
    # print(engine.execute_query(command_6))


    
