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
        self.ddl_manager = DDLManager()
        self.dml_manager = DMLManager()

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
        # Log initial action of checking for index
        logging.debug(f"Checking for index on table {main_table}")
        if self.has_index(main_table, command):
            logging.debug("Index found, selecting with index.")
            return self.select_with_index(command)
        else:
            logging.debug("No index found, selecting without index.")
            return self.select_no_index(command)
    
    def select_no_index(self, command):
        if 'main_table' not in command or not command['columns']:
            logging.error("Select command is missing 'main_table' or 'columns'")
            return "Invalid command format"
        
        main_table = command['main_table']
        data = self.dml_manager.select(main_table, ['*'])  # Fetch all rows from the table

        # Apply WHERE clause filtering at the very beginning
        if 'where_clause' in command:
            data = self.filter_data_by_condition(data, command['where_clause'])

        # Process JOINs if specified
        if 'join' in command:
            for join in command['join']:
                data = self.handle_join(data, join, main_table, command['columns'])

        # Check for the presence of aggregation functions
        aggregation_needed = any(
            func in col.upper() for col in command['columns'] for func in ['MAX', 'MIN', 'SUM', 'AVG', 'COUNT']
        )

        # Handle GROUP BY with or without aggregation
        if 'group_by' in command and command['group_by'] is not None:
            data = self.handle_group_by(data, command['group_by'], command['columns'])
            # Apply HAVING clause if present
            if 'having' in command and command['having']:
                data = self.handle_having(data, command['having'])
        elif aggregation_needed:
            # Process non-grouped aggregations
            data = self.handle_aggregations(command, data)

        # If ORDER BY is specified, sort the data accordingly
        if 'order_by' in command and command['order_by']:
            data = self.handle_order_by(data, command['order_by'])

        # Only return the columns specified in the SELECT clause
        final_data = self.filter_select_columns(data, command['columns'])
        return final_data
    
    def handle_aggregations(self, command, data):
        # Assume data is a list of dictionaries, each representing a row
        results = {}
        for column in command['columns']:
            # Extract the function and column name (e.g., "MAX(monthly_state_population)")
            match = re.match(r"(\w+)\((\w+)\)", column)
            if match:
                func, col_name = match.groups()
                try:
                    # Attempt to convert data to numeric types before aggregation
                    values = [self.safe_convert_to_numeric(row[col_name]) for row in data if col_name in row and row[col_name] is not None]
                except ValueError as e:
                    logging.error(f"Error converting data to numeric type for column {col_name}: {e}")
                    continue  # Skip this column if conversion fails

                # Perform the aggregation operation
                if func.upper() == 'MAX':
                    results[column] = max(values) if values else None
                elif func.upper() == 'MIN':
                    results[column] = min(values) if values else None
                elif func.upper() == 'SUM':
                    results[column] = sum(values) if values else None
                elif func.upper() == 'AVG':
                    results[column] = sum(values) / len(values) if values else None
                elif func.upper() == 'COUNT':
                    results[column] = len(values)

        return [results]  # Return a list containing a single dictionary

    @staticmethod
    def safe_convert_to_numeric(value):
        try:
            # Convert to float if possible, otherwise to int
            return float(value)
        except ValueError:
            try:
                return int(value)
            except ValueError:
                logging.error(f"Conversion to numeric failed for value: {value}")
                return None
    
    def select_with_index(self, command):
        main_table = command['main_table']
        columns = command.get('columns', [])
        where_clause = command.get('where_clause', '')

        # Initialize data collection
        data = []

        if where_clause:
            # Regex to find the value for the indexed column in WHERE clause
            pattern = rf"{columns[0]}\s*=\s*['\"]?([^'\"]+)['\"]?"  # Assuming column[0] is the indexed column
            match = re.search(pattern, where_clause)
            if match and self.storage_manager.column_has_index(main_table, columns[0]):
                value = match.group(1)
                # Fetch only the requested column using the index
                indexed_data = self.dml_manager.select_with_index(main_table, columns[0], value)
                # Format data to include only requested columns
                data.extend([{col: row.get(col) for col in columns} for row in indexed_data])
            else:
                # No match found in WHERE clause for indexed column, fall back to full table scan but limited to requested columns
                data = self.dml_manager.select(main_table, columns)
        else:
            # If no WHERE clause, perform a regular select on specified columns
            logging.debug("No WHERE clause or no index match, performing full table scan.")
            data = self.dml_manager.select(main_table, columns)

        return data

    def finalize_selection(self, data, command):
        # A more simplified processing suitable for non-indexed selects
        # This method can be expanded or contracted based on specific needs
        if 'where_clause' in command:
            data = self.filter_data_by_condition(data, command['where_clause'])

        return data  # Return data directly without further processing

    def has_index(self, table, command):
        # Retrieve column information if available from the command or assume all columns
        requested_columns = command.get('columns', [])
        if not requested_columns:
            # If no specific columns are requested, consider all columns of the table
            requested_columns = self.storage_manager.get_table_columns(table)

        # Check for each column if an index exists
        for column in requested_columns:
            if self.storage_manager.column_has_index(table, column):
                logging.debug(f"Index found on column: {column}")
                return True
        logging.debug("No index found on any requested columns.")
        return False

    
    def filter_data_by_condition(self, data, where_clause):
        if where_clause is None:
            logging.debug("No where_clause provided, returning original data.")
            return data
        condition_function = self.parse_condition_to_function(where_clause)
        filtered_data = [row for row in data if condition_function(row)]
        return filtered_data
    
    def filter_select_columns(self, data, select_columns):
        
        if '*' in select_columns:
            return data
        
        final_data = []
        for row in data:
            filtered_row = {}
            for column in select_columns:
                if ' AS ' in column:
                    # Split to get the function and alias
                    func_part, alias = column.split(' AS ')
                    alias = alias.strip()
                    # Use the alias as the key to fetch from row
                    filtered_row[alias] = row.get(alias)
                else:
                    # This will handle both aggregated columns without alias and regular columns
                    func_match = re.match(r'(\w+)\((\w+)\)', column)
                    if func_match:
                        # It's an aggregated column without alias
                        func_name, col_name = func_match.groups()
                        agg_key = func_name.upper() + '(' + col_name + ')'
                        filtered_row[column] = row.get(agg_key)
                    else:
                        # Regular column without any function
                        filtered_row[column] = row.get(column)

            final_data.append(filtered_row)
        return final_data


    def parse_join_condition(self, condition):
        left, _, right = condition.partition('=')
        left_table, left_column = left.strip().split('.')
        right_table, right_column = right.strip().split('.')
        return (left_column.strip(), right_column.strip())  # Return only column names, ignore aliases for comparison
      
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

        # Choose the join method based on the data size
        join_method = self.decide_join_method(main_data, join_data, join_type)

        # Execute the appropriate join method based on type
        if join_type in ["INNER JOIN", "JOIN"]:  # Treat 'JOIN' as 'INNER JOIN'
            return join_method(main_data, join_data, left_field, right_field, main_alias, join_alias, select_columns, 'inner')
        elif join_type == "LEFT JOIN":
            return join_method(main_data, join_data, left_field, right_field, main_alias, join_alias, select_columns, 'left')
        elif join_type == "RIGHT JOIN":
            return join_method(main_data, join_data, left_field, right_field, main_alias, join_alias, select_columns, 'right')
        else:
            logging.error(f"Unsupported join type: {join_type}")
            return []

    def decide_join_method(self, main_data, join_data, join_type):
        # If either dataset is large, use merge join, otherwise use nested loop
        if len(main_data) > 1000 or len(join_data) > 1000:
            logging.debug("Using merge join due to large dataset size")
            return self.merge_join
        else:
            logging.debug("Using nested loop join for smaller dataset size")
            return self.nested_loop_join
        
    def merge_join(self, main_data, join_data, left_field, right_field, main_alias, join_alias, select_columns, join_type):
        # Extract just the column name from field references
        _, left_column = left_field.split('.')
        _, right_column = right_field.split('.')

        # Sort data based on these extracted column names
        main_data.sort(key=lambda x: x.get(left_column))
        join_data.sort(key=lambda x: x.get(right_column))

        i, j = 0, 0
        result = []
        while i < len(main_data) and j < len(join_data):
            while i < len(main_data) and j < len(join_data) and main_data[i][left_column] < join_data[j][right_column]:
                if join_type == 'left':
                    # For LEFT JOIN, add main data with None for join columns if no match
                    merged_row = self.merge_rows(main_data[i], {}, main_alias, join_alias, select_columns)
                    result.append(merged_row)
                i += 1
            while i < len(main_data) and j < len(join_data) and main_data[i][left_column] > join_data[j][right_column]:
                if join_type == 'right':
                    # For RIGHT JOIN, add join data with None for main columns if no match
                    merged_row = self.merge_rows({}, join_data[j], main_alias, join_alias, select_columns)
                    result.append(merged_row)
                j += 1
            while i < len(main_data) and j < len(join_data) and main_data[i][left_column] == join_data[j][right_column]:
                # Add matched rows
                merged_row = self.merge_rows(main_data[i], join_data[j], main_alias, join_alias, select_columns)
                result.append(merged_row)
                i += 1
                j += 1

        # Handle remaining rows after main loop for outer joins
        if join_type == 'left':
            while i < len(main_data):
                merged_row = self.merge_rows(main_data[i], {}, main_alias, join_alias, select_columns)
                result.append(merged_row)
                i += 1
        elif join_type == 'right':
            while j < len(join_data):
                merged_row = self.merge_rows({}, join_data[j], main_alias, join_alias, select_columns)
                result.append(merged_row)
                j += 1

        return result

    def nested_loop_join(self, main_data, join_data, left_field, right_field, main_alias, join_alias, select_columns, join_type):
        # Split fields to remove the table alias if present
        _, left_column = left_field.split('.')
        _, right_column = right_field.split('.')
        result = []

        # INNER JOIN and JOIN (defaulting to INNER JOIN)
        if join_type in ['inner', 'JOIN']:  # Treat 'JOIN' as 'INNER JOIN'
            for main_row in main_data:
                for join_row in join_data:
                    if main_row.get(left_column) == join_row.get(right_column):
                        merged_row = self.merge_rows(main_row, join_row, main_alias, join_alias, select_columns)
                        result.append(merged_row)

        # LEFT JOIN
        elif join_type == 'left':
            for main_row in main_data:
                matched = False
                for join_row in join_data:
                    if main_row.get(left_column) == join_row.get(right_column):
                        merged_row = self.merge_rows(main_row, join_row, main_alias, join_alias, select_columns)
                        result.append(merged_row)
                        matched = True
                if not matched:
                    # Include the main row with None for join table columns
                    merged_row = self.merge_rows(main_row, {}, main_alias, join_alias, select_columns)
                    result.append(merged_row)

        # RIGHT JOIN
        elif join_type == 'right':
            for join_row in join_data:
                matched = False
                for main_row in main_data:
                    if main_row.get(left_column) == join_row.get(right_column):
                        merged_row = self.merge_rows(main_row, join_row, main_alias, join_alias, select_columns)
                        result.append(merged_row)
                        matched = True
                if not matched:
                    # Include the join row with None for main table columns
                    merged_row = self.merge_rows({}, join_row, main_alias, join_alias, select_columns)
                    result.append(merged_row)

        return result
   
    def merge_rows(self, main_row, join_row, main_alias, join_alias, select_columns):
        """
        Merge rows from two tables based on provided columns and aliases.

        Parameters:
        - main_row: Dictionary representing a row from the main table.
        - join_row: Dictionary representing a row from the join table.
        - main_alias: Alias used for the main table.
        - join_alias: Alias used for the join table.
        - select_columns: List of columns selected in the query, in the format 'alias.column'.

        Returns:
        - A dictionary representing the merged row.
        """
        merged_row = {}
        # Debugging: Log the incoming rows
        # logging.debug(f"Merging rows with main_row from '{main_alias}': {main_row} and join_row from '{join_alias}': {join_row}")
        
        # Iterate through the selected columns and merge data from both rows
        for col in select_columns:
            table_alias, column_name = col.split('.')
            # Determine which table's data to use based on the alias and add to the merged row
            if table_alias == main_alias:
                if column_name in main_row:
                    merged_row[col] = main_row[column_name]
                    # logging.debug(f"Added {column_name} from main table '{main_alias}': {main_row[column_name]}")
                else:
                    merged_row[col] = None
                    # logging.debug(f"Column {column_name} not found in main table '{main_alias}', setting as None")
            elif table_alias == join_alias:
                if column_name in join_row:
                    merged_row[col] = join_row[column_name]
                    # logging.debug(f"Added {column_name} from join table '{join_alias}': {join_row[column_name]}")
                else:
                    merged_row[col] = None
                    # logging.debug(f"Column {column_name} not found in join table '{join_alias}', setting as None")

        return merged_row

    
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

    def join_data(self, main_data, join_data, left_field, right_field, main_alias, join_alias, join_type, select_columns):
        joined_data = []
        for main_row in main_data:
            matched = False
            main_value = main_row.get(left_field.split('.')[-1])  # Ignore alias here
            for join_row in join_data:
                join_value = join_row.get(right_field.split('.')[-1])  # Ignore alias here
                if main_value == join_value:
                    matched = True
                    merged_row = {}
                    for col in select_columns:
                        table_alias, column_name = col.split('.')
                        if table_alias == join_alias:
                            merged_row[col] = join_row.get(column_name)
                        else:
                            merged_row[col] = main_row.get(column_name)
                    joined_data.append(merged_row)
            if not matched and (join_type == 'LEFT JOIN' or join_type == 'RIGHT JOIN'):
                # Ensure LEFT JOIN or RIGHT JOIN specifics are handled
                null_row = {col: None for col in select_columns if col.split('.')[0] == join_alias}
                joined_data.append({**main_row, **null_row})
        return joined_data          
    

    def parse_columns_for_aggregation(self, columns):
        # This regex now correctly captures potential spaces around the AS keyword
        agg_funcs = {}
        for col in columns:
            match = re.match(r'(\w+)\((\w+)\)\s*(AS\s*(\w+))?', col.strip(), re.IGNORECASE)
            if match:
                agg_func, column_name, _, alias = match.groups()
                alias = alias or f"{agg_func.upper()}({column_name})"
                agg_funcs[column_name] = (agg_func.upper(), alias)
        return agg_funcs

    def handle_order_by(self, data, order_by_clause):
        import re
        column, order = re.split(r'\s+', order_by_clause)
        reverse = (order.upper() == 'DESC')
        return sorted(data, key=lambda x: x[column], reverse=reverse)

    def handle_having(self, grouped_data, having_clause):
        # Example simple HAVING clause handler; expand as needed
        data = self.filter_data_by_condition(grouped_data, having_clause)
        return data
        
    def handle_insert(self, command):
        return self.dml_manager.insert(command['table'], command['data'])

    def handle_delete(self, command):
        return self.dml_manager.delete(command['table'], command['conditions'])

    def handle_update(self, command):
        return self.dml_manager.update(command['tables'], command['values'], command['where_condition'])

    def handle_unsupported(self, command):
        return "Unsupported command type"

        
    
    def handle_group_by(self, data, group_by_column, columns):
        grouped_data = {}
        agg_funcs = self.parse_columns_for_aggregation(columns)

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

    
    def finalize_query_results(self, data, columns):
        # Ensure results use correct aliases or column names
        final_results = []
        for row in data:
            final_row = {}
            for col in columns:
                # Attempt to extract an alias or use column directly
                alias = self.extract_alias(col)
                # Check if the alias exists in the aggregated data
                if alias in row:
                    final_row[alias] = row[alias]
                else:
                    # Use direct column name if no alias is found
                    final_row[col.split(' AS ')[0]] = row.get(col.split(' AS ')[0], None)
            final_results.append(final_row)
        return final_results

    def extract_alias(self, column):
        # Extracts the alias from a column specification, if present
        parts = column.split(' AS ')
        if len(parts) == 2:
            return parts[1].strip()
        return column.split('(')[0].strip()

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
            if 'BETWEEN' in conditions and 'AND' in conditions:
                pattern = r"(\w+)\s*(BETWEEN)\s*(.*)\s*(AND)\s*(.*)"
                match = re.match(pattern, conditions.strip(), re.IGNORECASE)
                if not match:
                    raise ValueError("Invalid BETWEEN clause format")
                column, between_operator, value_1, and_operator, value_2 = match.groups()
                print([column, value_1, value_2])
                return self.apply_operator(row, column.strip(), ">=", value_1.strip().strip("'")) and self.apply_operator(row, column.strip(), "<=", value_2.strip().strip("'"))
            
            if 'AND' in conditions and 'BETWEEN' not in conditions:
                left, right = conditions.split('AND', 1)
                return eval_condition(row, left.strip()) and eval_condition(row, right.strip())
            if 'OR' in conditions:
                left, right = conditions.split('OR', 1)
                return eval_condition(row, left.strip()) or eval_condition(row, right.strip())
            pattern = r"\(?(.*)\)?\s*(=|!|<>|<|>|<=|>=|LIKE|IN|BETWEEN)\s*(.*)"
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
            """
        elif "BETWEEN" in operator:
            # Ensure proper handling of BETWEEN
            value = value[len("BETWEEN"):].strip()  # Remove the 'BETWEEN' keyword and strip any leading/trailing whitespace
            """
        else:
            value = self.safe_convert_to_numeric_where(value)
            if operator == "=": operator = "=="
            # elif operator == "!=": operator = "!="  # No change needed, just making it explicit
            result = self.compare_values(row.get(column), value, operator)
            logging.debug(f"Comparison operator {operator} result: {result}")
            return result

    
    def compare_values(self, row_value, value, operator):
        # First, ensure that values are properly converted or reported if None
        row_value = self.safe_convert_to_numeric_where(row_value)
        value = self.safe_convert_to_numeric_where(value)

        # Add logging to check values just before comparison
        logging.debug(f"Pre-comparison: Row Value: {row_value} ({type(row_value)}), Value: {value} ({type(value)}), Operator: {operator}")
        
        if operator == "!":
            return row_value != value
        elif operator == "==":
            return row_value == value
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
            if value is None:
                logging.debug("Received None value; original value is None.")
                return None
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                logging.error(f"Conversion failed for value: {value}, returning None.")
                return None

# Example usage
if __name__ == "__main__":
    engine = ExecutionEngine()