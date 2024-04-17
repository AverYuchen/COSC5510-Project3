import os
import csv
import json
from storage import StorageManager

class DDLManager:
    def __init__(self):
        self.ddlstorage = StorageManager()
        
    def create_table(self, table_name, columns):
        tables = self.ddlstorage.schemas
        if table_name in tables:
            return "Error: Table already exists."
        
        schema = {'columns': {}, 'primary_key': [], 'foreign_keys': [], 'indexes': []}
        headers = []
        datapath = os.path.join(self.ddlstorage.data_directory, f"{table_name}.csv")
        try:
            with open(datapath, mode='w', newline='') as file:
                writer = csv.writer(file)
                for col_name, col_def in columns.items():
                    print(col_name, col_def)
                    if not self.validate_column_definition(col_def):
                        return f"Error: Invalid column definition for {col_name}."
                    headers.append(col_name)
                    #pump up schema with new table's data definition
                    for key, value in col_def.items():
                        if key == 'type':
                            if '(' in value:
                                value = value.split('(')[0]
                            value = value.lower()
                            schema['columns'][col_name] = {'type': value}
                        elif key == 'primary_key' and value:
                                schema['primary_key'].append(col_name)
                        elif key == 'foreign_keys' and value is not None:
                                schema['foreign_keys'].append(col_name)
                        else:
                            if value:
                                schema['indexes'].append(col_name)
                self.ddlstorage.create_schema(table_name, schema)
                writer.writerow(headers)
            
        except IOError as e:
            return f"Error: Failed to create new table due to {str(e)}"
        
        return f"Table '{table_name}' created successfully."

    def validate_column_definition(self, col_def):
        # Simple validation for column type and constraints
        valid_types = {'INT', 'VARCHAR', 'YEAR'}
        datatype = col_def["type"]
        if '(' in datatype:
            datatype = datatype.split('(')[0]
        if datatype not in valid_types:
            print(datatype)
            return False
        return True

    def drop_table(self, table_name):
        if table_name not in self.ddlstorage.schemas:
            return "Error: Table does not exist."
        os.remove(os.path.join(self.ddlstorage.data_directory, f"{table_name}.csv"))
        self.ddlstorage.drop_schema(table_name)
        return f"Table '{table_name}' dropped successfully."

# Example usage for testing
if __name__ == "__main__":
    ddl = DDLManager()
    #print(ddl.drop_table('another_test'))
