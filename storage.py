import csv
import os
import logging
import unittest

class StorageManager:
    def __init__(self, data_directory="data"):
            # Ensure the data_directory is correctly set
            self.data_directory = os.path.abspath(data_directory)
            if not os.path.exists(self.data_directory):
                os.makedirs(self.data_directory)
            self.schemas = {}
            self.data = {}
            self.define_schemas()
            self.load_all_data()

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
        print("Schemas defined for all tables.")

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
            if filename.endswith(".csv"):
                table_name = filename[:-4]  # Remove the .csv extension
                self.schemas[table_name] = self.load_schema(os.path.join(self.schema_directory, filename))

    def load_schema(self, schema_path):
        schema = {'columns': {}, 'primary_key': None, 'foreign_keys': [], 'indexes': []}
        try:
            with open(schema_path, newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    if len(row) != 2:
                        continue  # Expect exactly two entries per row, skip otherwise
                    col_name, col_type = row
                    schema['columns'][col_name] = {'type': col_type}
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
        if table_name not in self.schemas:
            self.schemas[table_name] = schema
            return "Schema for {0} created successfully.".format(table_name)
        else:
            return "Error: Schema for {0} already exists.".format(table_name)
        
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
    
    def update_table_data(self, table_name, data):
        self.data[table_name] = data
        self.write_csv(table_name + '.csv', data)

class TestStorageManager(unittest.TestCase):
    def setUp(self):
        """Prepare a test environment."""
        self.storage = StorageManager()
        # Assuming test_table schema already defined in your StorageManager setup
        # Populate the table with test data
        self.storage.data['test_table'] = [
            {'id': '1', 'name': 'Alice'},
            {'id': '2', 'name': 'Bob'},
            {'id': '3', 'name': 'Charlie'},
            {'id': '9', 'name': 'Joieho'}
        ]
        self.storage.write_csv('test_table')  # Write initial test data to CSV (optional)

    def test_delete_specific_row(self):
        """Test deleting a specific row identified by ID."""
        # Check initial state
        initial_count = len(self.storage.data['test_table'])
        self.assertEqual(initial_count, 4)

        # Perform deletion of id = 9
        result = self.storage.delete_data('test_table', 'id = 2')
        
        # Check the results of the deletion
        self.assertEqual(result, "Deleted 1 rows.")
        self.assertEqual(len(self.storage.data['test_table']), initial_count - 1)
        
        # Verify that the specific item is removed
        remaining_ids = [row['id'] for row in self.storage.data['test_table']]
        self.assertNotIn('2', remaining_ids)

        # Ensure the CSV file is updated correctly
        with open(os.path.join(self.storage.data_directory, 'test_table.csv'), 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data_in_file = list(reader)
            self.assertEqual(len(data_in_file), initial_count - 1)
            ids_in_file = [row['id'] for row in data_in_file]
            self.assertNotIn('2', ids_in_file)

    def tearDown(self):
        """Clean up after tests."""
        # Optionally remove any files or other clean-up actions

if __name__ == '__main__':
    unittest.main()
