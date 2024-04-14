from storage import StorageManager

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
        if conditions is None: 
            filtered_data = data
        else:
            conditions = conditions.replace('=','==')
            conditions_key_value = conditions.split(" ", maxsplit = 1)
            conditions = "{}{}{}{}{}".format('d','["',conditions_key_value[0],'"]',conditions_key_value[1])
            #print(conditions)
            filtered_data = [d for d in data if eval(conditions)]
        if columns is not None:
            if "*" in columns:
                return filtered_data
            else:
                selected_cols = [{k: v for k, v in single_entry.items() if k in columns} for single_entry in filtered_data]
        else:
            return None
        return selected_cols

if __name__ == "__main__":
    dml = DMLManager()
    dml.insert("test_table", {"id": 1, "name": "Test"})
    results = dml.select("test_table")
    print("Results after insert:", results)

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
