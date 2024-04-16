import logging
from storage import StorageManager
import re

class DMLManager:
    def __init__(self):
        self.storage_manager = StorageManager()

    def insert(self, table_name, row):
        data = self.storage_manager.get_table_data(table_name)
        data.append(row)
        self.storage_manager.update_table_data(table_name, data)
        return "Insert successful."
    
    def delete(self, table_name, condition):
        print(f"Attempting to delete from {table_name} where {condition}")
        data = self.storage_manager.get_table_data(table_name)
        
        if not data:
            return "Table not found or empty."

        # Normalize data to ensure consistent quote and type handling
        for row in data:
            for key in row:
                row[key] = self.normalize_value(row[key])

        initial_count = len(data)
        if condition:
            condition_function = self.parse_conditions(condition)
            data = [row for row in data if not condition_function(row)]
        else:
            return "No condition provided."

        updated_count = len(data)
        deleted_count = initial_count - updated_count

        if deleted_count > 0:
            self.storage_manager.update_table_data(table_name, data)
            
        print(f"Deleted count: {deleted_count}")

        return f"Deleted {deleted_count} rows."

    def normalize_value(self, value):
        # Normalize quotes and remove whitespace
        if isinstance(value, str):
            return value.strip().replace("‘", "'").replace("’", "'").replace("’", "'").replace("’", "'")
        return value
    
    
    def select(self, table_name, columns, conditions):
        data = self.storage_manager.get_table_data(table_name)
        filtered_data = []

        if conditions:
            condition_function = self.parse_conditions(conditions)
            filtered_data = [d for d in data if condition_function(d)]
        else:
            filtered_data = data  # No conditions specified, select all data

        if columns is not None:
            if "*" in columns:
                return filtered_data
            else:
                selected_cols = [{k: v for k, v in single_entry.items() if k in columns} for single_entry in filtered_data]
        else:
            return None

        return selected_cols
    

    def max(self, table, column, conditions=None):
        data = self.storage_manager.get_table_data(table)
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        return max(d.get(column, float('-inf')) for d in data)

    def min(self, table, column, conditions=None):
        data = self.storage_manager.get_table_data(table)
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        return min(d.get(column, float('inf')) for d in data)
    def sum(self, table, column, conditions=None):
        data = self.storage_manager.get_table_data(table)
        if not data:
            return "Table not found or no data available."
        if conditions:
            condition_function = self.parse_conditions(conditions)
            data = [d for d in data if condition_function(d)]
        return sum(d.get(column, 0) for d in data if isinstance(d.get(column), (int, float)))


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
                        condition_str = f"(d['{column}'] {operators[op]} {value})"
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

    def execute_query(self, query):
        # Execute the query using your database connection
        # This is a placeholder function, replace it with your actual database execution code
        print(f"Executing query: {query}")
        # Simulate a database response
        return "Query result"
    
if __name__ == "__main__":
    
    dml_manager = DMLManager()
    # Assuming parse_conditions is part of the current module
    test_condition = dml_manager.parse_conditions("id = '1'")
    print(test_condition({'id': '1', 'name': 'Test'}))  # Should return True

