# STORAGE.py

import csv
import json
import os
import logging
from BTrees.OOBTree import BTree
import unittest

# conda install blist

class StorageManager:
    def __init__(self, data_directory="data"):
            # Ensure the data_directory is correctly set
            self.data_directory = os.path.abspath(data_directory)
            self.schema_directory = os.path.join(self.data_directory, 'schemas')
            if not os.path.exists(self.data_directory):
                os.makedirs(self.data_directory)
                if not os.path.exists(self.schema_directory):
                    os.makedirs(self.schema_directory)
            self.schemas = {}
            self.data = {}
            self.indexes = {}  # Dictionary to hold BTree indexes for each table
            self.define_schemas()
            self.load_schemas()
            self.load_all_data()
            # self.initialize_indexes()
            
    def define_schemas(self):
        # Manually defining schemas for each table
        self.schemas['state_abbreviation'] = {
            'columns': {
                'state': {'type': 'varchar'},
                'state_code': {'type': 'varchar'}
            },
            'primary_key': 'state',
            'foreign_keys': [],
            'indexes': []
        }


        self.schemas['state_population'] = {
            'columns': {
                'state_code': {'type': 'varchar'},
                'month': {'type': 'int'},
                'year': {'type': 'year'},
                'monthly_state_population': {'type': 'int'}
            },
            'primary_key': ['state_code', 'month', 'year'],  # Assuming composite primary key
            'foreign_keys': [],
            'indexes': []
        }

        self.schemas['test_table'] = {
            'columns': {
                'id': {'type': 'int'},
                'name': {'type': 'varchar'}
            },
            'primary_key': 'id',
            'foreign_keys': [],
            'indexes': []
        }
        
        self.schemas['Reli11000'] = {
            "columns": {
                "A": {"type": "int"},
                "B": {"type": "int"}
            },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
            }
        
        self.schemas['Relii1000'] = {
            "columns": {
                "A": {"type": "int"},
                "B": {"type": "int"}
            },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
            }
        
        self.schemas['TestTable1'] = {
            "columns": { "A": { "type": "int" }, "B": { "type": "varchar" } },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
        }
        
        self.schemas['TestTable2'] = {
            "columns": { "A": { "type": "int" }, "B": { "type": "varchar" } },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
        }
        

        file_1 = os.path.join(self.schema_directory, 'state_abbreviation.json')
        if os.path.exists(file_1) == False:
            with open(file_1, "w") as json_file:
                json.dump(self.schemas['state_abbreviation'], json_file)
        file_2 = os.path.join(self.schema_directory, 'state_population.json')
        if os.path.exists(file_2) == False:
            with open(file_2, "w") as json_file:
                json.dump(self.schemas['state_population'], json_file)
        file_3 = os.path.join(self.schema_directory, 'test_table.json')
        if os.path.exists(file_3) == False:
            with open(file_3, "w") as json_file:
                json.dump(self.schemas['test_table'], json_file)
        
        
        file_4 = os.path.join(self.schema_directory, 'Reli11000.json')
        if os.path.exists(file_4) == False:
            with open(file_4, "w") as json_file:
                json.dump(self.schemas['Reli11000'], json_file)
                
        file_5 = os.path.join(self.schema_directory, 'Relii1000.json')
        if os.path.exists(file_5) == False:
            with open(file_5, "w") as json_file:
                json.dump(self.schemas['Relii1000'], json_file)
                
        file_6 = os.path.join(self.schema_directory, 'TestTable1.json')
        if os.path.exists(file_5) == False:
            with open(file_6, "w") as json_file:
                json.dump(self.schemas['TestTable1'], json_file)
                
        file_7 = os.path.join(self.schema_directory, 'TestTable2.json')
        if os.path.exists(file_5) == False:
            with open(file_7, "w") as json_file:
                json.dump(self.schemas['TestTable2'], json_file)
                
                
        #print("Schemas defined for all tables.")

    def load_all_data(self):
        # Load data files if present
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                table_name = filename[:-4]  # Strip the '.csv' part
                if table_name in self.schemas:  # Only load data for defined schemas
                    file_path = os.path.join(self.data_directory, filename)
                    self.data[table_name] = self.read_csv(file_path)

    def read_csv(self, file_path):
        try:
            with open(file_path, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                return [row for row in reader]
        except Exception as e:
            logging.error(f"Failed to read {file_path}: {e}")
            return []  # Return an empty list on error

    def load_schemas(self):
        # Load only schema files
        for filename in os.listdir(self.schema_directory):
            if filename.endswith(".json"):
                table_name = filename[:-5]  # Remove the .json extension
                self.schemas[table_name] = self.load_schema(os.path.join(self.schema_directory, filename))

    def load_schema(self, schema_path):
        schema = {}
        try:
            with open(schema_path, newline='') as file:
                schema = json.load(file)
        except FileNotFoundError:
            logging.error(f"Schema file {schema_path} not found.")
        except Exception as e:
            logging.error(f"Error reading schema file {schema_path}: {e}")
        return schema

    def get_schema(self, table_name):
        schema = self.schemas.get(table_name)
        if schema:
            logging.debug(f"Schema found for table {table_name}: {schema}")
        else:
            logging.debug(f"No schema found for table {table_name}.")
        return schema

    def create_schema(self, table_name, schema):
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        with open(schema_file, "w") as json_file:
            json.dump(schema, json_file)
        if table_name not in self.schemas:
            self.schemas[table_name] = schema
            return "Schema for {0} created successfully.".format(table_name)
        else:
            return "Error: Schema for {0} already exists.".format(table_name)
    
    def drop_schema(self, table_name):
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        if os.path.exists(schema_file):
            os.remove(schema_file)
            return "Schema file {0} is dropped successfully".format(table_name)
        else:
            return "Error: Drop schema task fail, the schema file not existed."

    def delete_data(self, table_name, conditions):
        condition_func = self.parse_conditions_safe(conditions)
        if condition_func is None:
            logging.error("Deletion failed: Invalid condition syntax")
            return "Error: Invalid condition syntax"

        try:
            initial_data = self.data.get(table_name, [])
            new_data = [row for row in initial_data if not condition_func(row)]
            rows_deleted = len(initial_data) - len(new_data)

            logging.debug(f"Initial data: {initial_data}")
            logging.debug(f"New data after deletion: {new_data}")

            if rows_deleted > 0:
                self.data[table_name] = new_data
                result = self.write_csv(table_name)  # Write changes back to the CSV file
                if result is not None:
                    return result
                return f"Deleted {rows_deleted} rows."
            else:
                return "No rows matched the condition."
        except Exception as e:
            logging.error(f"Deletion failed: {e}")
            return f"Error: Failed to delete data due to {e}"

    def write_csv(self, table_name):
        filename = os.path.join(self.data_directory, f"{table_name}.csv")
        try:
            with open(filename, mode='w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=self.schemas[table_name]['columns'].keys())
                writer.writeheader()
                writer.writerows(self.data[table_name])
            logging.info(f"Data for {table_name} successfully written to CSV.")
        except Exception as e:
            logging.error(f"Failed to write to {filename}: {e}")
            return f"Error: Failed to write data due to {e}"

    def parse_conditions_safe(self, conditions):
        import re
        # This regex now captures both digits and non-digits for values
        match = re.match(r"^\s*(\w+)\s*=\s*(['\"]?)(\w+)\2\s*$", conditions)
        if match:
            field, _, value = match.groups()
            # Try to convert to integer, fallback to string
            try:
                value = int(value)
            except ValueError:
                pass  # Keep value as string if conversion fails

            def condition_func(row):
                # Convert both to string for comparison to handle different data types gracefully
                row_value = row.get(field)
                if isinstance(row_value, int):
                    row_value = str(row_value)
                return row_value == str(value)

            return condition_func
        else:
            logging.error("Invalid condition syntax or unhandled condition format: " + conditions)
            return None
 
    def insert_data(self, table_name, data):
        # Check if schema exists for the table
        if table_name in self.schemas:
            if table_name not in self.data:
                self.data[table_name] = []
            self.data[table_name].append(data)
            self.write_csv(table_name)  # Ensure data is written to file after insertion
            return "Data inserted successfully."
        else:
            return "Error: Table does not exist."
        
    def get_table_data(self, table_name):
        table_data = self.data.get(table_name, [])
        # print(f"Table Data for {table_name}: {table_data}")  # Debugging statement
        return table_data
    
    def update_table_data(self, table_name, value, parsed_condition, conditions):
        try:
            initial_data = self.get_table_data(table_name)
            retrieved_data = [row for row in initial_data if parsed_condition(row)]
            print(retrieved_data)
            updated_data = []
            changed_rows = 0
            for row in retrieved_data:
                for col, content in value.items():
                    row[col] = content
                updated_data.append(row)
                changed_rows += 1
            
            self.delete_data(table_name, conditions)
            self.insert_data(table_name, updated_data[0])

            return changed_rows
        except Exception as e:
            logging.error(f"update failed: {e}")
            return 0

    # def create_index(self, table_name, column_name, index_name):
    #     index_key = (table_name, column_name)
    #     if index_key not in self.indexes:
    #         self.indexes[index_key] = BTree()
    #     for row in self.data[table_name]:
    #         key = row[column_name]
    #         self.indexes[index_key].insert(key, row)
    #     print(f"Index {index_name} created on {table_name}({column_name})")
    

    def create_index(self, table_name, column_name, index_name):
        # Construct the index key based on table, column names and index name for unique identification
        index_key = (table_name, column_name, index_name)
        
        # Check if an index with the same name already exists
        if index_key in self.indexes:
            return "Error: Index {} already exists on {}({}).".format(index_name, table_name, column_name)
        
        # Create a new BTree for the index if it does not exist
        self.indexes[index_key] = BTree()

        # Iterate through each row in the table data and populate the BTree
        for row in self.data.get(table_name, []):
            # Extract the key from the specified column
            key = row[column_name]

            # If the key already exists in the index, append the row to the existing list
            if key in self.indexes[index_key]:
                self.indexes[index_key][key].append(row)
            else:
                # Otherwise, create a new entry in the BTree with this key and initialize with a list
                self.indexes[index_key][key] = [row]

        # Print a confirmation message indicating successful index creation
        print(f"Index {index_name} created on {table_name}({column_name})")
        return "Index {} created on {}({}).".format(index_name, table_name, column_name)

    def drop_index(self, table_name, index_name):
        """Drop an index from a table."""
        schema = self.get_schema(table_name)
        if not schema:
            return "Error: Table does not exist."

        # Remove the index from schema
        initial_length = len(schema['indexes'])
        schema['indexes'] = [index for index in schema['indexes'] if index['name'] != index_name]

        if len(schema['indexes']) == initial_length:
            return f"Error: Index '{index_name}' does not exist on table '{table_name}'."

        # Update the schema file
        self.update_schema(table_name, schema)
        return f"Index '{index_name}' dropped from '{table_name}'."

    def update_schema(self, table_name, schema):
        """Update the schema file after a change."""
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        try:
            with open(schema_file, 'w') as file:
                json.dump(schema, file, indent=4)
            self.schemas[table_name] = schema
            return "Schema updated successfully."
        except Exception as e:
            logging.error(f"Failed to update schema for {table_name}: {e}")
            return f"Error updating schema: {e}"

    def table_exists(self, table_name):
        """Check if the specified table exists in the database."""
        exists = table_name in self.schemas
        logging.debug(f"Checking if table exists ('{table_name}'): {exists}")
        return exists

    def column_exists(self, table_name, column_name):
        """Check if the specified column exists in the specified table."""
        if self.table_exists(table_name):
            exists = column_name in self.schemas[table_name]['columns']
            logging.debug(f"Checking if column exists ('{column_name}' in '{table_name}'): {exists}")
            return exists
        return False

    def index_exists(self, table_name, index_name):
        """Check if the specified index exists on the specified table."""
        if self.table_exists(table_name):
            # Assuming each index is stored as a dictionary in a list of indexes
            exists = any(index['name'] == index_name for index in self.schemas[table_name].get('indexes', []))
            logging.debug(f"Checking if index exists ('{index_name}' on '{table_name}'): {exists}")
            return exists
        return False

    def update_index_metadata(self, table_name, index_name, column_name=None, action="create"):
        """Update metadata for an index based on the action (create or drop)."""
        if action == "create":
            if 'indexes' not in self.schemas[table_name]:
                self.schemas[table_name]['indexes'] = []
            self.schemas[table_name]['indexes'].append({'name': index_name, 'column': column_name})
            logging.debug(f"Index '{index_name}' created on '{table_name}({column_name})'")
        elif action == "drop":
            if self.index_exists(table_name, index_name):
                self.schemas[table_name]['indexes'] = [idx for idx in self.schemas[table_name]['indexes'] if idx['name'] != index_name]
                logging.debug(f"Index '{index_name}' dropped from '{table_name}'")
                
    def print_index_info(self, table_name):
        """Prints information about indexes on a specified table."""
        if self.table_exists(table_name):
            indexes = self.schemas[table_name].get('indexes', [])
            if indexes:
                print(f"Indexes on table '{table_name}':")
                for index in indexes:
                    print(f"  Index Name: {index['name']}, Column: {index['column']}")
            else:
                print(f"No indexes found on table '{table_name}'.")
        else:
            print(f"Table '{table_name}' does not exist.")
    
    # def initialize_indexes(self):
    #     for table, schema in self.schemas.items():
    #         if 'indexes' in schema:
    #             for index in schema['indexes']:
    #                 self.indexes[(table, index)] = BTree()
                    
class TestStorageManager(unittest.TestCase):
    def setUp(self):
        # Initialize the StorageManager with a specific data set
        self.storage_manager = StorageManager(data_directory="test_data")
        self.storage_manager.data = {
            'TestTable1': [
                {'A': '1', 'B': 'Alpha'},
                {'A': '2', 'B': 'Beta'},
                {'A': '3', 'B': 'Gamma'}
            ]
        }
        # Define schema for simplicity in the test environment
        self.storage_manager.schemas['TestTable1'] = {
            "columns": { "A": { "type": "int" }, "B": { "type": "varchar" } },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
        }

    def test_create_index(self):
        # Test the creation of an index
        self.storage_manager.create_index('TestTable1', 'A', 'index_A')
        index_key = ('TestTable1', 'A')
        
        # Verify that the index exists
        self.assertIn(index_key, self.storage_manager.indexes)
        
        # Verify the content of the index
        btree = self.storage_manager.indexes[index_key]
        expected_keys = ['1', '2', '3']
        for key in expected_keys:
            self.assertIn(key, btree)

        # Verify that the values are correct (can be extended based on data integrity needs)
        for row in self.storage_manager.data['TestTable1']:
            self.assertEqual(btree[row['A']], row)

if __name__ == '__main__':
    unittest.main()