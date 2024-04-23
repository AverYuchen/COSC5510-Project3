# DML.py

import logging
import re
from storage import StorageManager
from ddl import DDLManager
import os
import csv

# Configure logging
# logging.basicConfig(filename='dbms_debug.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class DMLManager:
    def __init__(self, storage_manager):
        self.storage_manager = storage_manager  # Use the passed instance
        self.ddl_manager = DDLManager() 
        logging.debug("DMLManager initialized with provided storage manager.")

    def insert(self, table_name, data):
        if table_name not in self.storage_manager.schemas:  # Correct method to check table existence
            logging.error(f"Insert operation failed: Table {table_name} does not exist.")
            return "Error: Table does not exist."

        if not self.validate_data(table_name, data, command='insert'):
            logging.error("Insert operation failed: Data validation failed.")
            return "Error: PK exist - Data validation failed."

        try:
            self.storage_manager.insert_data(table_name, data)
            logging.info(f"Data successfully inserted into {table_name}.")
            return "Data inserted successfully."
        except Exception as e:
            logging.error(f"Insert operation failed: {e}")
            return "Error: Failed to insert data."

    def validate_data(self, table_name, data, command):
        schema = self.storage_manager.get_schema(table_name)
        if not schema:
            logging.error(f"No schema available for table {table_name}")
            return False

        for field, value in data.items():
            if field not in schema['columns']:
                logging.error(f"Data validation error: Field '{field}' is not in the schema.")
                return False
            
            #verify if the inserted data matched the datatype definition
            expected_type = schema['columns'][field]['type']
            print(expected_type)
            if expected_type == 'int' and not isinstance(value, int):
                try:
                    value = int(value)  # Convert to int if necessary
                    data[field] = value
                except ValueError:
                    logging.error(f"Type conversion error for field '{field}': expected int, got {value}")
                    return False
            elif expected_type == 'varchar' and not isinstance(value, str):
                logging.error(f"Type validation error for field '{field}': expected string, got {type(value).__name__}")
                return False
        #verify if the inserted data matched the primary key condition
        if not self.check_primary_key_constraint(table_name, data, schema, command):
            print(self.check_primary_key_constraint(table_name, data, schema, command))
            logging.error(f"Inserted data is not satisfied primary key rule for table {table_name}")
            return False

        return True

    def delete(self, table_name, conditions): 
        try:
            result = self.storage_manager.delete_data(table_name, conditions)
            return result
        except Exception as e:
            logging.error(f"Delete operation failed: {str(e)}")
       
    
    def parse_conditions_delete(self, conditions):
        import re
        # Simplify parsing logic to handle basic conditions like "id = 9"
        pattern = r"^\s*(\w+)\s*=\s*(\d+)\s*$"
        match = re.match(pattern, conditions)
        if match:
            field, value = match.groups()
            # Ensure the field exists in the schema and value type is correct
            schema = self.storage_manager.get_schema('test_table')
            if field in schema['columns']:
                if schema['columns'][field]['type'] == 'int':
                    # Convert value to int for comparison, assuming all ids are integers
                    return lambda row: int(row.get(field, None)) == int(value)
                else:
                    return lambda row: str(row.get(field, None)) == value
            else:
                logging.error(f"Field {field} not found in schema.")
                return None
        else:
            logging.error("Failed to parse delete conditions")
            return None

    def update(self, table_name, value, conditions):
        #check validation:
        if self.validate_data(table_name, value, command='update'):
            logging.debug(f"Attempting to update {table_name} with conditions {conditions} to {value} ")
            try:
                data = self.storage_manager.get_table_data(table_name)
                print(data)
                condition_function = self.parse_conditions(conditions)
                retrieved_data = [d for d in data if condition_function(d)]
                print(retrieved_data)
                rows_updated = self.storage_manager.update_table_data(table_name, value, retrieved_data, conditions)
                if rows_updated > 0:
                    logging.info(f"Updated {rows_updated} rows in {table_name}.")
                    return f"Updated {rows_updated} rows."
                else:
                    logging.warning(f"No rows updated in {table_name}.")
                    return "No rows matched the conditions."
            except Exception as e:
                logging.error(f"Update operation failed: {e}")
                return "Error: Failed to update data."
        else:
            return "new data does not match the datatype or conflict with primary key uniqueness"
    
    def check_primary_key_constraint(self, table_name, data, schema, command):
        """
        Check if the data violates primary key constraints.
        """
        print(schema)
        primary_keys = schema.get('primary_key')
        existing_data = self.storage_manager.get_table_data(table_name)
        if primary_keys:
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys] 
            for primary_key in primary_keys:
                if primary_key not in data and command == 'insert':
                    return False
                if primary_key not in data and command == 'update':
                    return True
                for row in existing_data:
                    if schema['columns'][primary_key]['type'] == "int":
                        row[primary_key] = int(row[primary_key])
                    if row[primary_key] == data[primary_key]:
                        return False

        return True

    def create_table(self, table_name, columns):
        """
        Facilitates the creation of a table and its schema definitions.
        
        Args:
            table_name (str): The name of the table to create.
            columns (dict): A dictionary describing the columns and constraints.
            
        Returns:
            str: A message indicating the success or failure of the operation.
        """
        return self.ddl_manager.create_table(table_name, columns)

    def normalize_value(self, value):
        # Normalize quotes and remove whitespace
        if isinstance(value, str):
            return value.strip().replace("‘", "'").replace("’", "'").replace("’", "'").replace("’", "'")
        return value

    def select(self, table_name, columns, conditions=None):
        # Retrieve data from the storage manager
        data = self.storage_manager.get_table_data(table_name)
        # print(f"Debug: Data retrieved from {table_name}: {data}")

        # Apply conditions if specified
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]

        # Handle column selection
        if columns == ['*']:
            # If columns contain '*', return all data without filtering columns
            return data
        else:
            # Process column names for SQL function extraction or direct use
            processed_columns = []
            for col in columns:
                # Check if the column is wrapped in a function and extract the column name
                match = re.search(r"\w+\((\w+)\)", col)
                if match:
                    # Extract the column name from the function
                    processed_columns.append(match.group(1))
                else:
                    processed_columns.append(col)

            # Filter data to only include specified columns
            filtered_data = [{k: v for k, v in item.items() if k in processed_columns} for item in data]
            return filtered_data

    def max(self, table, column, conditions=None):
        # Find the maximum value in the specified column
        data = self.storage_manager.get_table_data(table)
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        column_values = [d.get(column, float('-inf')) for d in data]
        return max(column_values)

    def min(self, table, column, conditions=None):
        # Find the minimum value in the specified column
        data = self.storage_manager.get_table_data(table)
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        column_values = [d.get(column, float('inf')) for d in data]
        return min(column_values)

    def sum(self, table, column, conditions=None):
        data = self.storage_manager.get_table_data(table)
        print(f"Debug: Retrieved data for sum: {data}")  # Debug: Print retrieved data
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        print(f"Debug: Filtered data for sum: {data}")  # Debug: Print filtered data
        values_for_sum = [int(d.get(column, 0)) for d in data if d.get(column).isdigit()]
        print(f"Debug: Values for sum: {values_for_sum}")  # Debug: Print values used for sum
        return sum(values_for_sum)

    def avg(self, main_table, column, conditions=None):
        # Calculate the average of the specified column
        data = self.storage_manager.get_table_data(main_table)
        if not data:
            print("Debug: Table not found or no data available.")
            return "Table not found or no data available."

        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        
        column_values = [d.get(column, None) for d in data]
        print(f"Debug: All values in the column: {column_values}")

        # Filter out non-numeric values
        numeric_values = [value for value in column_values if str(value).isdigit()]
        print(f"Debug: Numeric values in the column: {numeric_values}")

        if not numeric_values:
            return "No numerical values found in the specified column."

        # Convert numeric values to integers and calculate the average
        numeric_values = [int(value) for value in numeric_values]
        average = sum(numeric_values) / len(numeric_values)
        return average

    def count(self, main_table, column=None, conditions=None):
        # Count the number of rows in the specified table or the number of non-null values in the specified column
        data = self.storage_manager.get_table_data(main_table)
        if not data:
            print("Debug: Table not found or no data available.")
            return "Table not found or no data available."

        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        
        if column:
            # Count non-null values in the specified column
            column_values = [d.get(column, None) for d in data]
            print(f"Debug: All values in the column: {column_values}")

            # Filter out null values
            non_null_values = [value for value in column_values if value is not None]
            print(f"Debug: Non-null values in the column: {non_null_values}")

            count = len(non_null_values)
        else:
            # Count the number of rows in the table
            count = len(data)

        return count

    def parse_conditions(self, conditions):
        print(f"Parsing conditions: {conditions}")
        operators = {
            '=': '==',
            '!=': '!=',
            '>': '>',
            '<': '<',
            '>=': '>=',
            '<=': '<='
        }

        conditions = re.split(r"\b(AND|OR)\b", conditions, flags=re.IGNORECASE)
        print(f"Split conditions: {conditions}")
        parsed_conditions = []

        for part in conditions:
            part = part.strip()
            if part.upper() in ['AND', 'OR']:
                parsed_conditions.append(part.lower())  # Use Python's lower case and/or
            else:
                match = re.search(r"(.*?)(=|!=|>|<|>=|<=)(.*)", part.strip(), re.IGNORECASE)
                if match:
                    column, op, value = match.groups()
                    column, value = column.strip(), value.strip()

                    if column in ['id']:  # Add more columns if needed
                        value = value.strip("'")
                        condition_str = f"(d['{column}'] {operators[op]} '{value}')"
                    elif value.isdigit():
                        condition_str = f"(d['{column}'] {operators[op]} '{value}')"
                    else:
                        value = value.strip("'")
                        condition_str = f"(d['{column}'] {operators[op]} '{value}')"

                    parsed_conditions.append(condition_str)
                else:
                    logging.error(f"Could not parse condition: {part}")
                    return None  # Or raise an Exception

        condition_str = " ".join(parsed_conditions)
        print(f"Final condition string: {condition_str}")
        
        # Return a callable lambda function
        return lambda d: eval(condition_str, {}, {"d": d})
    
    def aggregate(self, agg_type, table, column, conditions=None):
        # This should connect to your database and execute the appropriate aggregation query.
        # This is a simplified example:
        query = f"SELECT {agg_type.upper()}({column}) FROM {table}"
        if conditions:
            query += f" WHERE {conditions}"
        
        # Simulating database connection and query execution
        cursor = self.db_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None

    def group_by(self, main_table, group_column, aggregate_func, aggregate_column, conditions=None):
        data = self.storage_manager.get_table_data(main_table)
        if not data:
            return "Table not found or no data available."
        
        # Apply conditions if any
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [row for row in data if condition_function(row)]

        # Grouping data
        grouped_data = {}
        for row in data:
            key = row.get(group_column)
            if key not in grouped_data:
                grouped_data[key] = []
            grouped_data[key].append(row)
        
        # Aggregating data
        aggregated_data = {}
        for key, rows in grouped_data.items():
            if aggregate_func == 'sum':
                aggregated_data[key] = sum(self.safe_convert(row.get(aggregate_column, 0)) for row in rows)
            elif aggregate_func == 'avg':
                values = [self.safe_convert(row.get(aggregate_column, 0)) for row in rows]
                aggregated_data[key] = sum(values) / len(values) if values else 0
            elif aggregate_func == 'count':
                aggregated_data[key] = len(rows)

        return aggregated_data

    def safe_convert(self, value):
        try:
            return float(value)
        except ValueError:
            return 0
    
    def having(self, aggregated_data, condition):
        print("Debug dml: Applying HAVING condition:", condition)
        
        # Parse the condition
        condition_function = self.parse_conditions(condition)
        
        # Filter aggregated data based on the condition
        filtered_data = {key: value for key, value in aggregated_data.items() if condition_function(value)}
        
        print("Debug: Filtered data after HAVING condition:", filtered_data)
        
        return filtered_data

    def order_by(self, data, order_column, ascending=True):
            # Convert string numeric data to integers before sorting
            numeric_data = [
                {order_column: int(row.get(order_column))}
                for row in data
                if row.get(order_column).isdigit()
            ]
            sorted_data = sorted(numeric_data, key=lambda x: x[order_column], reverse=not ascending)
            return sorted_data
        
# if __name__ == "__main__":
#     from storage import StorageManager

#     storage_manager = StorageManager()
#     dml_manager = DMLManager(storage_manager)

#     queries = [
#         "SELECT state FROM state_abbreviation WHERE state = 'Alaska'",
#         "SELECT * FROM state_population WHERE state_code = 'AK' AND year = '2018'",
#         "SELECT state FROM state_abbreviation WHERE state = 'California' OR state = 'Texas'",
#         "INSERT INTO test_table (id, name) VALUES (1, 'Hachii')",
#         "DELETE FROM test_table WHERE id = 0",
#         "SELECT MAX(monthly_state_population) FROM state_population",
#         "SELECT a.state_code, b.state FROM state_population AS a JOIN state_abbreviation AS b ON a.state_code = b.state_code",
#         "SELECT a.state_code, b.state FROM state_population AS a INNER JOIN state_abbreviation AS b ON a.state_code = b.state_code"
#     ]

#     # Mock responses for testing purposes
#     for query in queries:
#         if query.startswith("SELECT"):
#             print("\nQuerying:", query)
#             result = dml_manager.select(query)  # Simulate a select method
#             print("Results:", result)
#         elif query.startswith("INSERT"):
#             print("\nInserting:", query)
#             result = dml_manager.insert(query)  # Simulate an insert method
#             print("Insertion Status:", result)
#         elif query.startswith("DELETE"):
#             print("\nDeleting:", query)
#             result = dml_manager.delete(query)  # Simulate a delete method
#             print("Deletion Status:", result)