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

    
    
    # def delete(self, table_name, condition):
    #     """
    #     Delete rows from a table based on a provided condition.
    #     """
    #     data = self.storage_manager.get_table_data(table_name)
    #     original_count = len(data)
    #     condition_function = self.parse_conditions(condition)
    #     data = [d for d in data if not condition_function(d)]
    #     self.storage_manager.update_table_data(table_name, data)
    #     deleted_count = original_count - len(data)
    #     logging.info(f"Deleted {deleted_count} rows from {table_name}")
    #     return f"Deleted {deleted_count} rows."
    
    
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


    # def parse_conditions(self, conditions):
    #     print(f"Parsing conditions: {conditions}")
    #     # Map SQL-like operators to Python operators with regex patterns for matching
    #     operators = {
    #         '=': '==',
    #         '!=': '!=',
    #         '>': '>',
    #         '<': '<',
    #         '>=': '>=',
    #         '<=': '<='
    #     }

    #     # Prepare to handle logical operators (AND, OR) and ensure proper Python logical operators
    #     conditions = re.split(r"\b(AND|OR)\b", conditions, flags=re.IGNORECASE)
    #     print(f"Parsing conditions: {conditions}")
    #     parsed_conditions = []

    #     for part in conditions:
    #         part = part.strip()
    #         if part.upper() in ['AND', 'OR']:
    #             parsed_conditions.append(part.lower())  # Use Python's lower case and/or
    #         else:
    #             # Split on operators and capture the operator used
    #             match = re.search(r"(.*?)(=|!=|>|<|>=|<=)(.*)", part.strip(), re.IGNORECASE)
    #             if match:
    #                 column, op, value = match.groups()
    #                 column, value = column.strip(), value.strip()
    #                 # Check if the value is a digit or a properly quoted string
    #                 if value.isdigit():  # simple check to decide if it's a number
    #                     condition_str = f"(d['{column}'] {operators[op]} {value})"
    #                 else:  # Assume the value is a string and needs to be quoted
    #                     # Strip any existing single quotes from the value to avoid syntax errors
    #                     value = value.strip("'")
    #                     condition_str = f"(d['{column}'] {operators[op]} '{value}')"
    #                 parsed_conditions.append(condition_str)
    #             else:
    #                 logging.error(f"Could not parse condition: {part}")
    #                 return None  # Or raise an Exception

    #     condition_str = " ".join(parsed_conditions)

    #     # Define a function to evaluate the condition for each row (dictionary) in the data
    #     def condition_lambda(d):
    #         return eval(condition_str, {"d": d})

    #     return condition_lambda

if __name__ == "__main__":
    
    dml_manager = DMLManager()
    # Assuming parse_conditions is part of the current module
    test_condition = dml_manager.parse_conditions("id = '1'")
    print(test_condition({'id': '1', 'name': 'Test'}))  # Should return True


# class TestDMLManager:
#     @staticmethod
#     def setup_sample_data():
#         """ Set up sample data for testing. """
#         return [
#             {'id': '1', 'name': 'Test'},
#             {'id': '1', 'name': 'Test'},
#             {'id': '1', 'name': 'Test'},
#             {'id': '6', 'name': 'Oliver'},
#             {'id': '1', 'name': "Hachii"},
#             {'id': '1', 'name': "‘Happy’"},
#             {'id': '1', 'name': "’Back’"},
#         ]

#     @staticmethod
#     def test_delete():
#         """ Test the delete function of DMLManager. """
#         dml_manager = DMLManager()  # Assuming DMLManager is already imported and set up properly

#         # Setup initial data
#         test_table_data = TestDMLManager.setup_sample_data()
#         dml_manager.storage_manager.update_table_data('test_table', test_table_data)

#         # Test deletion
#         delete_count = dml_manager.delete('test_table', "id = '1'")
#         print(delete_count)  # Output the result to see the deletion count

#         # Fetch remaining data to verify
#         remaining_data = dml_manager.storage_manager.get_table_data('test_table')
#         print(f"Remaining data: {remaining_data}")

# if __name__ == "__main__":
#     TestDMLManager.test_delete()
