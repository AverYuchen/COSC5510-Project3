import os
import json

class DDLManager:
    def __init__(self, schema_directory="schemas"):
        """
        Initialize the DDL Manager with a directory to store schema definitions.

        Parameters:
            schema_directory (str): The directory where table schemas are stored as JSON files.
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
            if filename.endswith(".json"):
                table_name = filename[:-5]  # Remove the .json extension
                with open(os.path.join(self.schema_directory, filename), 'r') as file:
                    schemas[table_name] = json.load(file)
        return schemas

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

    def drop_table(self, table_name):
        """
        Drop a table schema and delete its JSON file.

        Parameters:
            table_name (str): The name of the table.

        Returns:
            str: A message indicating the success or failure of the operation.
        """
        if table_name not in self.tables:
            return "Error: Table does not exist."

        del self.tables[table_name]
        os.remove(os.path.join(self.schema_directory, f"{table_name}.json"))
        return f"Table '{table_name}' dropped successfully."

    def save_schema(self, table_name):
        """
        Save the schema of a table to a JSON file.

        Parameters:
            table_name (str): The name of the table.
        """
        with open(os.path.join(self.schema_directory, f"{table_name}.json"), 'w') as file:
            json.dump(self.tables[table_name], file, indent=4)

# Functions to be used by other modules
def create_table(table_name, columns):
    return DDLManager().create_table(table_name, columns)

def drop_table(table_name):
    return DDLManager().drop_table(table_name)

# Example usage for testing
if __name__ == "__main__":
    ddl = DDLManager()
    print(ddl.create_table('state_population_2', {
        'state_id': 'INT PRIMARY KEY',
        'state_name': 'VARCHAR(100)',
        'population': 'INT',
        'year': 'YEAR'
    }))
    print(ddl.drop_table('state_population_2'))
