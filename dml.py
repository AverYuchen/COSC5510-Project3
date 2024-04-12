import logging

from storage_manager import StorageManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s') 

def insert(self, table_name, row):
        """
        Insert a new row into the specified table, checking for primary key constraints.
        """
        data = self.storage_manager.tables.get(table_name, [])
        primary_key = self.get_primary_key(table_name)
        if primary_key and any(row[primary_key] == existing[primary_key] for existing in data):
            logging.error(f"Duplicate primary key value in '{table_name}'")
            raise DuplicatePrimaryKeyError(f"Duplicate primary key value in '{table_name}'")
        data.append(row)
        self.storage_manager.update_table(table_name, data)
        logging.info(f"Inserted 1 row into {table_name}")
        return "Insert successful."

def delete(self, table_name, condition):
        """
        Delete rows from a table based on a provided condition.
        """
        data = self.storage_manager.tables.get(table_name, [])
        original_count = len(data)
        data = [row for row in data if not condition(row)]
        self.storage_manager.update_table(table_name, data)
        deleted_count = original_count - len(data)
        logging.info(f"Deleted {deleted_count} rows from {table_name}")
        return f"Deleted {deleted_count} rows."

def select(self, table_name, condition=lambda row: True):
        """
        Select rows from a table that meet a specified condition.
        """
        data = self.storage_manager.tables.get(table_name, [])
        return [row for row in data if condition(row)]

def update(self, table_name, updates, condition):
        """
        Update rows in a table based on a condition and set new values from updates.
        """
        data = self.storage_manager.tables.get(table_name, [])
        updated_count = 0
        for row in data:
            if condition(row):
                row.update(updates)
                updated_count += 1
        self.storage_manager.update_table(table_name, data)
        logging.info(f"Updated {updated_count} rows in {table_name}")
        return f"Updated {updated_count} rows."
    
class DuplicatePrimaryKeyError(Exception):
    pass

class DMLManager:
    def __init__(self, storage_manager):
        """
        Initialize the DML Manager with a reference to the StorageManager for data operations.
        """
        self.storage_manager = storage_manager

    def get_primary_key(self, table_name):
        """
        Retrieve the primary key column for a given table from its schema.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            str: The name of the primary key column, or None if not found.
        """
        # Attempt to retrieve the schema from the tables dictionary
        schema = self.storage_manager.tables.get(table_name, {})

        # Check if schema is a dictionary and has a 'primary_key' key
        if isinstance(schema, dict) and 'primary_key' in schema:
            return schema['primary_key']
        
        # If schema is not a dictionary or does not contain 'primary_key'
        elif isinstance(schema, list) and schema:  # Check if it's a non-empty list
            # Assuming schema information might be in the first element of the list
            # This is a guess; you need to adjust this based on your actual schema structure
            if isinstance(schema[0], dict):
                return schema[0].get('primary_key')
        
        return None

# Example usage
if __name__ == "__main__":
    storage = StorageManager()
    dml = DMLManager(storage)
    try:
        print(dml.insert('users', {'id': 1, 'name': 'Alice'}))
    except DuplicatePrimaryKeyError as e:
        print(e)