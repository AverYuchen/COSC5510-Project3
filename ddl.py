import os
import csv

class DDLManager:
    def __init__(self, schema_directory="data"):
        """
        Initialize the DDL Manager with a directory to store schema definitions.

        Parameters:
            schema_directory (str): The directory where table schemas are stored as csv files.
        """
        self.schema_directory = schema_directory
        if not os.path.exists(schema_directory):
            os.makedirs(schema_directory)
        self.tables = self.load_all_schemas()    
    


    def load_all_schemas(self):
        """
        Load all schema definitions from the schema directory into memory.

        Returns:
            dict: A dictionary of table schemas.
        """
        schemas = {}
        for filename in os.listdir(self.schema_directory):
            if filename.endswith(".csv"):
                table_name = filename[:-4]  # Remove the .csv extension
                with open(os.path.join(self.schema_directory, filename), 'r', encoding='utf-8-sig') as file:
                    reader =  csv.reader(file)
                    headers = next(reader)
                    schemas[table_name] = headers
        return schemas
    '''
    def create_table(self, table_name, columns):
        """
        Create a new table schema and save it to a JSON file.

        Parameters:
            table_name (str): The name of the table.
            columns (dict): A dictionary of column definitions.

        Returns:
            str: A message indicating the success or failure of the operation.
        """
        if table_name in self.tables:
            return "Error: Table already exists."

        self.tables[table_name] = {'columns': columns}
        self.save_schema(table_name)
        return f"Table '{table_name}' created successfully."
    '''
    def create_table(self, table_name, columns):
        """
        Create a new table schema and save it to a csv file.

        Parameters:
            table_name (str): The name of the table.
            columns (dict): A dictionary of column definitions.

        Returns:
            str: A message indicating the success or failure of the operation.
        """
        if table_name in self.tables:
            return "Error: Table already exists."
        headers = list(columns.keys())
        datatypes = list(columns.values())
        self.save_schema(table_name, headers)
        self.save_datatypes(table_name, datatypes)
        return f"Table '{table_name}' created successfully."
    
    def save_datatypes (self, table_name, datatypes):
        with open('datatypes.txt', 'a') as f:
            f.write(f"{table_name}:{datatypes}\n")

    def drop_table(self, table_name):
        """
        Drop a table schema and delete its csv file.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            str: A message indicating the success or failure of the operation.
        """
        if table_name not in self.tables:
            return "Error: Table does not exist."

        del self.tables[table_name]
        os.remove(os.path.join(self.schema_directory, f"{table_name}.csv"))
        with open('datatypes.txt', 'r') as file:
            lines = file.readlines()
            remaining_lines = [line for line in lines if not line.startswith(table_name)]
         # Write the remaining lines back to the file
        with open('datatypes.txt', 'w') as file:
            file.writelines(remaining_lines)
        return f"Table '{table_name}' dropped successfully."

    def save_schema(self, table_name, headers):
        """
        Save the schema of a table to a csv file.

        Parameters:
            table_name (str): The name of the table and its headers.
        """
        with open(os.path.join(self.schema_directory, f"{table_name}.csv"), 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers)

    

# Functions to be used by other modules
def create_table(table_name, columns):
    return DDLManager().create_table(table_name, columns)

def drop_table(table_name):
    return DDLManager().drop_table(table_name)

# Example usage for testing
if __name__ == "__main__":
    ddl = DDLManager()
    '''test over "create" and "drop" within the class'''
    #print(ddl.tables)
    print(ddl.create_table('users', {'id': 'INT', 'name' : 'VARCHAR(50)'}))
    #print(ddl.drop_table('test_table2'))
    