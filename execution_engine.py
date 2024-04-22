# EXECUTION_ENGINE.py

from dml import DMLManager
from ddl import DDLManager
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
        
        if 'join' in command and command['join']:
            for join in command['join']:
                data = self.handle_join(data, join, main_table)

        # Aggregation handling remains unchanged
        for col in command['columns']:
            if any(agg in col.upper() for agg in ['MAX', 'MIN', 'SUM', 'COUNT', 'AVG']):
                agg_match = re.match(r'(\w+)\((\w+)\)', col)
                if agg_match:
                    agg_func, column_name = agg_match.groups()
                    data = [{col: self.handle_aggregations(data, agg_func.upper(), column_name)}]
                    break

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
    
    def handle_aggregations(self, data, agg_func, column_name):
        numeric_data = [float(row[column_name]) for row in data if column_name in row and row[column_name] is not None]

        if agg_func == 'MAX':
            return max(numeric_data)
        elif agg_func == 'MIN':
            return min(numeric_data)
        elif agg_func == 'SUM':
            return sum(numeric_data)
        elif agg_func == 'AVG':
            return sum(numeric_data) / len(numeric_data) if numeric_data else None
        elif agg_func == 'COUNT':
            return len(data)
        else:
            return None
    
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


    
