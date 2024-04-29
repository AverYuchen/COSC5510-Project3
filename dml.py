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
    def __init__(self):
        self.storage_manager = StorageManager()  # Use the passed instance
        self.ddl_manager = DDLManager() 
        #logging.debug("DMLManager initialized with provided storage manager.")


    
    def insert(self, table_name, data):
        # Load the latest data to make sure the insertion checks against all existing records
        self.storage_manager.load_latest_data()
        self.storage_manager.load_latest_schema()
        schema = self.storage_manager.get_schema(table_name)

        if table_name not in self.storage_manager.schemas:
            #logging.error(f"Insert operation failed: Table {table_name} does not exist.")
            return "Error: Table does not exist."

        if not self.validate_data_PK(table_name, data, command='insert'):
        # if not self.validate_data(table_name, data, command='insert'):

            return "Error: Data validation failed due to primary key duplication or type mismatch."

        try:
            self.storage_manager.insert_data(table_name, data)
            return "Data inserted successfully."
        except Exception as e:
            #logging.error(f"Insert operation failed: {e}")
            return "Error: Failed to insert data."

        
    def check_primary_key_constraint(self, table_name, data, schema, command):
        primary_keys = schema.get('primary_key')
        existing_data = self.storage_manager.get_table_data(table_name)
        #logging.debug(f"Checking PK with existing data: {existing_data}")
        if primary_keys:
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys]
            for primary_key in primary_keys:
                
                if primary_key not in data and command == 'insert':
                    return False
                if primary_key not in data and command == 'update':
                    return True
                # if primary_key not in data:
                #     logging.error("Primary key field missing in data provided.")
                #     return False
                existing_keys = [row[primary_key] for row in existing_data if primary_key in row]
                if data[primary_key] in existing_keys:
                    #logging.error(f"Duplicate primary key error for value {data[primary_key]}")
                    return False
        return True
    
    
    def validate_data_PK(self, table_name, data, command):
        self.storage_manager.load_schema(table_name)  # Ensure schema is up-to-date
        schema = self.storage_manager.get_schema(table_name)
        if not schema:
            #logging.error(f"No schema available for table {table_name}")
            return False
        if not self.check_primary_key_constraint(table_name, data, schema, command):
            return False
        if any(not self.validate_type(data[key], schema['columns'][key]['type']) for key in data if key in schema['columns']):
            #logging.error("Data type validation failed.")
            return False
        return True


    def validate_type(self, value, expected_type):
        if expected_type == 'int':
            if not isinstance(value, int):
                try:
                    int(value)  # Try converting to int
                    return True
                except ValueError:
                    #logging.debug(f"Failed to convert {value} to int.")
                    return False
            return True
        elif expected_type == 'varchar':
            return isinstance(value, str)
        # Add more type checks as needed
        return False

    

    def delete(self, table_name, conditions):
        # Load latest data and schema
        self.storage_manager.load_latest_data()
        self.storage_manager.load_latest_schema()

        # Parse conditions to filter applicable rows
        condition_function = self.parse_conditions(conditions)
        data_to_delete = [d for d in self.storage_manager.get_table_data_w_datatype(table_name) if condition_function(d)]

        # Check for foreign key references before deleting
        if not self.can_delete(table_name, data_to_delete):
            return "Error: Data cannot be deleted due to foreign key constraints."

        # Perform deletion if safe
        try:
            delete_count = 0
            for row in data_to_delete:
                self.storage_manager.delete_data(table_name, lambda r: r == row)
                delete_count += 1

            if delete_count > 0:
                return f"Deleted {delete_count} rows from {table_name}."
            else:
                return "No rows matched the conditions or needed deletion."
        except Exception as e:
            #logging.error(f"Delete operation failed: {str(e)}")
            return f"Error: Failed to delete data due to {str(e)}"

    def can_delete(self, table_name, data_to_delete):
        """Check if data can be deleted based on foreign key constraints."""
        schema = self.storage_manager.get_schema(table_name)
        foreign_keys = schema.get('foreign_keys', {})
        if isinstance(foreign_keys, list):  # Convert list to dict if needed
            foreign_keys = {fk['column']: fk for fk in foreign_keys}

        for ref_table, ref_schema in self.storage_manager.schemas.items():
            ref_foreign_keys = ref_schema.get('foreign_keys', {})
            if isinstance(ref_foreign_keys, list):
                ref_foreign_keys = {fk['column']: fk for fk in ref_foreign_keys}

            for fk_column, fk_details in ref_foreign_keys.items():
                if fk_details['references']['table'] == table_name:
                    # Check if any data in the referencing table matches the foreign key values
                    ref_data = self.storage_manager.get_table_data(ref_table)
                    for ref_row in ref_data:
                        if ref_row[fk_column] in [row[fk_details['references']['column']] for row in data_to_delete]:
                            return False  # Data is referenced, cannot delete
        return True  # No references, safe to delete


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
                #logging.error(f"Field {field} not found in schema.")
                return None
        else:
            #logging.error("Failed to parse delete conditions")
            return None
        
    def column_in_foreign_keys(self, table_name, column_name):
        """Check if the column is used as a foreign key in any other table."""
        self.storage_manager.load_latest_schema()  # Ensure all schemas are loaded
        for other_table, other_schema in self.storage_manager.schemas.items():
            # Check the foreign_keys in each table to see if they reference the column in question
            foreign_keys = other_schema.get('foreign_keys', {})
            if isinstance(foreign_keys, dict):  # Assuming foreign_keys is a dictionary where keys are column names
                for fk_column, fk_detail in foreign_keys.items():
                    if fk_detail['references']['table'] == table_name and fk_detail['references']['column'] == column_name:
                        # logging.debug(f"Column {column_name} in table {table_name} is used as a foreign key in table {other_table}")
                        return True
        # logging.debug(f"Column {column_name} in table {table_name} is not used as a foreign key in any other tables")
        return False
    
    def update(self, table_name, new_values, conditions):
        # logging.debug(f"Starting update operation for table {table_name} with conditions: {conditions} and new values: {new_values}")
        self.storage_manager.load_latest_schema()
        schema = self.storage_manager.get_schema(table_name)

        if not schema:
            # logging.error("Table schema not found.")
            return "Error: Table schema not found."

        primary_key = schema.get('primary_key')
        if primary_key and any(key in new_values for key in (primary_key if isinstance(primary_key, list) else [primary_key])):
            if self.column_in_foreign_keys(table_name, primary_key):
                # logging.error(f"Attempt to update primary key {primary_key} that is used as a foreign key.")
                return "Error: Cannot update primary key as it is used as a foreign key in another table."

        if not self.validate_data(table_name, new_values, 'update'):
            # logging.error("Data validation failed.")
            return "Error: Data validation failed."

        condition_function = self.parse_conditions(conditions)
        if not condition_function:
            # logging.error("Invalid conditions provided.")
            return "Error: Invalid conditions."

        retrieved_data = self.storage_manager.get_table_data_w_datatype(table_name)
        filtered_data = [d for d in retrieved_data if condition_function(d)]
        rows_updated = self.storage_manager.update_table_data_2(table_name, new_values, filtered_data, condition_function)

        if rows_updated > 0:
            # logging.debug(f"Updated {rows_updated} rows in {table_name}.")
            return f"Updated {rows_updated} rows in {table_name}."
        else:
            # logging.debug("No rows matched the conditions or needed updating.")
            return "No rows matched the conditions or needed updating."

        
    def validate_data(self, table_name, data, command):
        schema = self.storage_manager.get_schema(table_name)
        if not schema:
            #logging.error(f"No schema available for table {table_name}")
            return False

        for field, value in data.items():
            if field not in schema['columns']:
                #logging.error(f"Data validation error: Field '{field}' is not in the schema.")
                return False
            
            #verify if the inserted data matched the datatype definition
            expected_type = schema['columns'][field]['type']
            print(expected_type)
            if expected_type == 'int' and not isinstance(value, int):
                try:
                    value = int(value)  # Convert to int if necessary
                    data[field] = value
                except ValueError:
                    #logging.error(f"Type conversion error for field '{field}': expected int, got {value}")
                    return False
            elif expected_type == 'varchar' and not isinstance(value, str):
                #logging.error(f"Type validation error for field '{field}': expected string, got {type(value).__name__}")
                return False
            
        #verify if the inserted data matched the primary key condition
        if not self.check_primary_key_constraint(table_name, data, schema, command):
            print(self.check_primary_key_constraint(table_name, data, schema, command))
            #logging.error(f"Inserted data is not satisfied primary key rule for table {table_name}")
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
        self.storage_manager.load_latest_data()
        self.storage_manager.load_latest_schema()
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
        
    def select_with_index(self, table_name, column, value):
        """
        Perform an indexed select on the specified table and column.

        Args:
            table_name (str): The name of the table to query.
            column (str): The column to query.
            value (str): The value to match in the indexed column.

        Returns:
            list[dict]: A list of dictionaries representing the rows that match the query.
        """
        # Check if the BTree index exists for the specified column
        index_key = (table_name, column)
        if index_key in self.storage_manager.indexes:
            # Retrieve using BTree index
            try:
                # This assumes that the BTree index is keyed by the column value
                indexed_rows = self.storage_manager.indexes[index_key][value]
                return indexed_rows
            except KeyError:
                # If the value is not found in the index, return an empty list
                return []
        else:
            # Fallback to full table scan if no index exists
            return [row for row in self.storage_manager.get_table_data(table_name) if row[column] == value]

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

                    """if column in ['id']:  # Add more columns if needed
                        value = value.strip("'")
                        condition_str = f"(d['{column}'] {operators[op]} '{value}')"
                    el"""
                    if value.isdigit():
                        condition_str = f"(d['{column}'] {operators[op]} {value})"
                    else:
                        value = value.strip("'")
                        condition_str = f"(d['{column}'] {operators[op]} '{value}')"

                    parsed_conditions.append(condition_str)
                else:
                    #logging.error(f"Could not parse condition: {part}")
                    return None  # Or raise an Exception

        condition_str = " ".join(parsed_conditions)
        print(f"Final condition string: {condition_str}")
        
        # Return a callable lambda function
        return lambda d: eval(condition_str, {}, {"d": d})

    def safe_convert(self, value):
        try:
            return float(value)
        except ValueError:
            return 0


if __name__ == "__main__":

    def test_insert_duplicate_primary_key():
        dml_manager = DMLManager()
        table_name = "test_table"
        data = {'id': 21, 'name': 'Ellen'}

        # First insert
        result1 = dml_manager.insert(table_name, data)
        print(result1)  # Expected: "Data inserted successfully."

        # Attempt to insert duplicate primary key
        result2 = dml_manager.insert(table_name, data)
        print(result2)  # Expected: "Error: Duplicate primary key error for value 21"

    test_insert_duplicate_primary_key()

