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
        
        self.schemas['Reli110000'] = {
            "columns": {
                "A": {"type": "int"},
                "B": {"type": "int"}
            },
            "primary_key": ["A"],
            "foreign_keys": [],
            "indexes": []
            }
        
        self.schemas['Relii10000'] = {
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
                
        file_8 = os.path.join(self.schema_directory, 'Reli110000.json')
        if os.path.exists(file_8) == False:
            with open(file_8, "w") as json_file:
                json.dump(self.schemas['Reli110000'], json_file)
                
        file_9 = os.path.join(self.schema_directory, 'Relii10000.json')
        if os.path.exists(file_9) == False:
            with open(file_9, "w") as json_file:
                json.dump(self.schemas['Relii10000'], json_file)
                
        file_6 = os.path.join(self.schema_directory, 'TestTable1.json')
        if os.path.exists(file_6) == False:
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
                    self.load_indexes_for_table(table_name) 
                    
    def load_indexes_for_table(self, table_name):
        schema = self.schemas.get(table_name, {})
        for index in schema.get('indexes', []):
            self.indexes[(table_name, index['column'], index['name'])] = BTree()

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
    
    def load_latest_schema(self):
        self.schemas = {}
        self.define_schemas()
        self.load_schemas()
    
    def load_latest_data(self):
        self.data = {}
        self.load_all_data()

    def get_schema(self, table_name):
        schema = self.schemas.get(table_name)
        if schema:
            logging.debug(f"Schema found for table {table_name}: {schema}")
        else:
            logging.debug(f"No schema found for table {table_name}.")
        return schema

    def create_schema(self, table_name, schema):
        if table_name not in self.schemas:
            self.schemas[table_name] = schema
            schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
            with open(schema_file, "w") as json_file:
                json.dump(schema, json_file)
            return "Schema for {0} created successfully.".format(table_name)
        else:
            return "Error: Schema for {0} already exists.".format(table_name)
    
    def drop_schema(self, table_name):
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        if os.path.exists(schema_file):
            os.remove(schema_file)
            self.load_latest_schema()
            self.load_latest_data()
            return "Schema file {0} is dropped successfully".format(table_name)
        else:
            return "Error: Drop schema task fail, the schema file not existed."

    def delete_data(self, table_name, condition_func):
        if condition_func is None:
            logging.error("Deletion failed: Invalid condition syntax")
            return "Error: Invalid condition syntax"

        try:
            initial_data = self.get_table_data_w_datatype(table_name)
            print(initial_data)
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
    """
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
    """ 
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
    
    def get_table_data_w_datatype(self, table_name):
        table_data = self.data.get(table_name, [])
        table_schema = self.get_schema(table_name)
        int_col = []
        for col in table_schema['columns']:
            if(table_schema['columns'][col]['type']) == 'int':
                int_col.append(col)
        for data in table_data:
            data.update((k, int(v)) for k, v in data.items() if k in int_col)

        # print(f"Table Data for {table_name}: {table_data}")  # Debugging statement
        return table_data
 
    def update_table_data(self, table_name, value, retrieved_data, condition_func):
        try:
            updated_data = []
            changed_rows = 0
            for row in retrieved_data:
                for col, content in value.items():
                    row[col] = content
                updated_data.append(row)
                changed_rows += 1
            print(updated_data)
            self.delete_data(table_name, condition_func)
            for row in updated_data:
                self.insert_data(table_name, row)
            return changed_rows
        except Exception as e:
            logging.error(f"update failed: {e}")
            return 0
        
    def create_index(self, table_name, column_name, index_name):
        # Ensure the table exists
        self.load_latest_data()
        if not self.table_exists(table_name):
            return f"Error: Table '{table_name}' does not exist."

        # Load the schema to make sure it is up to date
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        try:
            with open(schema_file, 'r') as file:
                self.schemas[table_name] = json.load(file)
        except FileNotFoundError:
            return f"Error: Schema file for '{table_name}' not found."
        except Exception as e:
            return f"Error reading schema file for '{table_name}': {e}"

        # Check if an index with the same name already exists for the table and column
        existing_indexes = self.schemas[table_name].get('indexes', [])
        if any(idx['name'] == index_name and idx['column'] == column_name for idx in existing_indexes):
            return f"Index {index_name} already exists on {table_name}({column_name})."

        # Create the new index in the schema
        new_index = {'name': index_name, 'column': column_name}
        self.schemas[table_name]['indexes'].append(new_index)

        # Save the updated schema back to the JSON file
        try:
            with open(schema_file, 'w') as file:
                json.dump(self.schemas[table_name], file, indent=4)
        except Exception as e:
            return f"Error saving updated schema for '{table_name}': {e}"

        # Add the index to the runtime dictionary if it does not exist
        index_key = (table_name, column_name, index_name)
        if index_key not in self.indexes:
            self.indexes[index_key] = BTree()

            # Populate the BTree with existing data
            for row in self.data.get(table_name, []):
                key = row[column_name]
                if key in self.indexes[index_key]:
                    self.indexes[index_key][key].append(row)
                else:
                    self.indexes[index_key][key] = [row]

        self.save_schema(table_name)
        self.load_latest_schema()
        return f"Index {index_name} created on {table_name}({column_name})."
    def save_schema(self, table_name):
        schema_file_path = os.path.join(self.schema_directory, f"{table_name}.json")
        try:
            with open(schema_file_path, 'w') as file:
                json.dump(self.schemas[table_name], file, indent=4)
            logging.info(f"Schema for {table_name} saved successfully.")
        except Exception as e:
            logging.error(f"Failed to save schema for {table_name}: {e}")
            return f"Error saving schema: {e}"

        
    def drop_index(self, table_name, index_name):
        # Verify index existence
        self.load_latest_schema()
        if not self.index_exists(table_name, index_name, check_file=True):
            return f"Error: Index '{index_name}' does not exist on table '{table_name}'."

        # Update index metadata in the schema file
        error = self.update_index_metadata(table_name, index_name, action='drop')
        if error:
            return error  # Return any errors that occurred during the update

        # Remove the index from the runtime dictionary
        index_keys_to_remove = [(table, col, name) for (table, col, name) in self.indexes if table == table_name and name == index_name]
        for key in index_keys_to_remove:
            del self.indexes[key]

        # self.save_schema(table_name)
        self.load_latest_schema()
        return f"Index '{index_name}' dropped from '{table_name}'."


    def index_exists(self, table_name, index_name, check_file=False):
        # Check in-memory first
        in_memory_check = any(key[2] == index_name and key[0] == table_name for key in self.indexes.keys())
        if in_memory_check:
            return True

        # Optionally check in the file if specified
        if check_file:
            try:
                with open(os.path.join(self.schema_directory, f"{table_name}.json"), 'r') as file:
                    schema = json.load(file)
                    return any(idx['name'] == index_name for idx in schema.get('indexes', []))
            except Exception as e:
                logging.error(f"Failed to read schema file for {table_name}: {e}")
                return False

        return False

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


    def update_index_metadata(self, table_name, index_name, action='drop'):
        # Load the schema to ensure it's up-to-date
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        try:
            with open(schema_file, 'r') as file:
                schema = json.load(file)
        except FileNotFoundError:
            return f"Error: Schema file for '{table_name}' not found."
        except Exception as e:
            return f"Error reading schema file for '{table_name}': {e}"

        if action == 'drop':
            # Remove the index from the schema
            schema['indexes'] = [index for index in schema.get('indexes', []) if index['name'] != index_name]

            # Save the updated schema back to the JSON file
            try:
                with open(schema_file, 'w') as file:
                    json.dump(schema, file, indent=4)
            except Exception as e:
                return f"Error saving updated schema for '{table_name}': {e}"

        return None  # None indicates success in this context
    
    def column_has_index(self, table, column):
        """
        Check if the specified column of a table has an index.

        Args:
            table (str): The name of the table.
            column (str): The name of the column.

        Returns:
            bool: True if an index exists, False otherwise.
        """
        schema = self.get_schema_index(table)
        if schema:
            indexes = schema.get('indexes', [])
            return any(index['column'] == column for index in indexes)
        return False
    
    def get_schema_index(self, table_name):
        """
        Fetch schema for the given table from JSON file.

        Args:a
            table_name (str): Table name to fetch the schema for.

        Returns:
            dict: Schema dictionary if found, else None.
        """
        schema_file = os.path.join(self.schema_directory, f"{table_name}.json")
        try:
            with open(schema_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            logging.error(f"Schema file {schema_file} not found for table {table_name}.")
        except Exception as e:
            logging.error(f"Error reading schema file {schema_file}: {e}")
        return None
    
    def show_tables(self):
        """
        Returns a list of all table names in the database.
        
        Returns:
            list: A list of table names.
        """
        # This will return a list of all table names for which schemas are defined.
        return [filename[:-4] for filename in os.listdir(self.data_directory) if filename.endswith('.csv')]

