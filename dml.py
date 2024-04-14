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


    def parse_conditions(self, conditions):
        """ Build a function to evaluate conditions safely without using eval directly on user input. """
        # Map SQL-like operators to Python operators
        operators = {'=': '==', '!=': '!=', '>': '>', '<': '<', '>=': '>=', '<=': '<='}

        # Handle logical operators (AND, OR) and ensure proper use of Python logical operators
        logical_operators = re.split(r"\b(AND|OR)\b", conditions, flags=re.IGNORECASE)
        parsed_conditions = []

        for part in logical_operators:
            part = part.strip()
            if part.upper() in ['AND', 'OR']:
                parsed_conditions.append(part.lower())  # Use Python's lower case and/or
            else:
                # Split on operators and capture the operator used
                column, op, value = re.split(r"(\s*=\s*|\s*!=\s*|\s*>\s*|\s*<\s*|\s*>=\s*|\s*<=\s*)", part, maxsplit=1)
                column = column.strip()
                value = value.strip().strip("'")  # Assume values are always single-quoted in conditions
                op = op.strip()
                # Replace SQL-like operators with Python operators
                python_op = operators[op]
                # Construct the condition string for eval
                parsed_conditions.append(f"(d['{column}'] {python_op} '{value}')")

        condition_str = " ".join(parsed_conditions)

        # Define a function to evaluate the condition for each row (dictionary) in the data
        def condition_eval(d):
            return eval(condition_str, {}, {"d": d})

        return condition_eval

    def delete(self, table_name, condition):
        """
        Delete rows from a table based on a provided condition.
        """
        data = self.storage_manager.get_table_data(table_name)
        original_count = len(data)
        condition_function = self.parse_conditions(condition)
        data = [d for d in data if not condition_function(d)]
        self.storage_manager.update_table_data(table_name, data)
        deleted_count = original_count - len(data)
        logging.info(f"Deleted {deleted_count} rows from {table_name}")
        return f"Deleted {deleted_count} rows."

if __name__ == "__main__":
    dml = DMLManager()
    all_data = dml.select("state_abbreviation", "*", None)
    print("Results after select all:", all_data)
    # dml.insert("test_table", {"id": 1, "name": "Test"})
    # results = dml.select("test_table", "*", None)
    # print("Results after insert:", results)
    # # Example of a more complex query
    # complex_query = dml.select("test_table", "*", "name = 'Test' OR name = 'Alice'")
    # print("Results after complex query:", complex_query)

    # print(dml.select("state_abbreviation", "*", "state = 'California' OR state = 'Texas'"))

# import logging

# # Assuming StorageManager is defined in another module and imported correctly here
# from query_input_manager import StorageManager

# class DuplicatePrimaryKeyError(Exception):
#     """Exception raised for duplicate primary key entries in a table."""
#     pass

# class DMLManager:
#     def __init__(self, storage_manager):
#         """
#         Initialize the DML Manager with a reference to the StorageManager.
#         """
#         self.storage_manager = storage_manager

#     def insert(self, table_name, row):
#         """
#         Insert a new row into the specified table, checking for primary key constraints.
#         """
#         data = self.storage_manager.get_table_data(table_name)
#         primary_key = self.get_primary_key(table_name)
#         if primary_key and any(row[primary_key] == existing[primary_key] for existing in data):
#             logging.error(f"Duplicate primary key value in '{table_name}'")
#             raise DuplicatePrimaryKeyError(f"Duplicate primary key value in '{table_name}'")
#         data.append(row)
#         self.storage_manager.update_table_data(table_name, data)
#         logging.info(f"Inserted 1 row into {table_name}")
#         return "Insert successful."

#     def delete(self, table_name, condition):
#         """
#         Delete rows from a table based on a provided condition.
#         """
#         data = self.storage_manager.get_table_data(table_name)
#         original_count = len(data)
#         data = [row for row in data if not condition(row)]
#         self.storage_manager.update_table_data(table_name, data)
#         deleted_count = original_count - len(data)
#         logging.info(f"Deleted {deleted_count} rows from {table_name}")
#         return f"Deleted {deleted_count} rows."

#     def select(self, table_name, condition=lambda row: True):
#         """
#         Select rows from a table that meet a specified condition.
#         """
#         if table_name not in self.storage_manager.data:
#             return "Table does not exist."
#         return [row for row in self.storage_manager.get_table_data(table_name) if condition(row)]

#     def update(self, table_name, updates, condition):
#         """
#         Update rows in a table based on a condition and set new values from updates.
#         """
#         data = self.storage_manager.get_table_data(table_name)
#         updated_count = 0
#         for row in data:
#             if condition(row):
#                 row.update(updates)
#                 updated_count += 1
#         self.storage_manager.update_table_data(table_name, data)
#         logging.info(f"Updated {updated_count} rows in {table_name}")
#         return f"Updated {updated_count} rows."

#     def get_primary_key(self, table_name):
#         """
#         Retrieve the primary key column for a given table from its schema.
#         """
#         schema = self.storage_manager.data.get(table_name, {})
#         return schema.get('primary_key') if isinstance(schema, dict) else None

# # Example usage
# if __name__ == "__main__":
#     storage = StorageManager()
#     dml = DMLManager(storage)
#     print(dml.insert('users', {'id': 1, 'name': 'Alice'}))  # Test the insert function
#     print(dml.select('users', lambda x: x['id'] == 1))  # Test the select function
