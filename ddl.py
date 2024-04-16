import os
import csv

class DDLManager:
    def __init__(self, schema_directory="data"):
        self.schema_directory = schema_directory
        if not os.path.exists(schema_directory):
            os.makedirs(schema_directory)
        self.tables = self.load_all_schemas()

    def load_all_schemas(self):
        schemas = {}
        for filename in os.listdir(self.schema_directory):
            if filename.endswith(".csv"):
                table_name = filename[:-4]  # Remove the .csv extension
                with open(os.path.join(self.schema_directory, filename), newline='') as file:
                    reader = csv.reader(file)
                    schema = {
                        'columns': {},
                        'primary_key': None,
                        'foreign_keys': [],
                        'indexes': []
                    }
                    for row in reader:
                        if len(row) < 2:  # Ensure there are at least two elements to unpack
                            continue
                        col_name, col_type, *constraints = row
                        schema['columns'][col_name] = {'type': col_type}
                        for constraint in constraints:
                            if constraint == 'PRIMARY_KEY':
                                schema['primary_key'] = col_name
                            elif 'FOREIGN_KEY' in constraint:
                                ref_table, ref_col = constraint.split('(')[1].strip(')').split(',')
                                schema['foreign_keys'].append({'column': col_name, 'ref_table': ref_table, 'ref_column': ref_col})
                            elif constraint == 'INDEX':
                                schema['indexes'].append(col_name)
                    schemas[table_name] = schema
        return schemas

    # def load_all_schemas(self):
    #     schemas = {}
    #     for filename in os.listdir(self.schema_directory):
    #         if filename.endswith(".csv"):
    #             table_name = filename[:-4]  # Remove the .csv extension
    #             with open(os.path.join(self.schema_directory, filename), newline='') as file:
    #                 reader = csv.reader(file)
    #                 schema = {
    #                     'columns': {},
    #                     'primary_key': None,
    #                     'foreign_keys': [],
    #                     'indexes': []
    #                 }
    #                 for row in reader:
    #                     col_name, col_type, *constraints = row
    #                     schema['columns'][col_name] = {'type': col_type}
    #                     for constraint in constraints:
    #                         if constraint == 'PRIMARY_KEY':
    #                             schema['primary_key'] = col_name
    #                         elif 'FOREIGN_KEY' in constraint:
    #                             ref_table, ref_col = constraint.split('(')[1].strip(')').split(',')
    #                             schema['foreign_keys'].append({'column': col_name, 'ref_table': ref_table, 'ref_column': ref_col})
    #                         elif constraint == 'INDEX':
    #                             schema['indexes'].append(col_name)
    #                 schemas[table_name] = schema
    #     return schemas

    def create_table(self, table_name, columns):
        if table_name in self.tables:
            return "Error: Table already exists."

        schema_path = os.path.join(self.schema_directory, f"{table_name}.csv")
        try:
            with open(schema_path, mode='w', newline='') as file:
                writer = csv.writer(file)
                for col_name, col_def in columns.items():
                    if not self.validate_column_definition(col_def):
                        return f"Error: Invalid column definition for {col_name}."
                    row = [col_name, col_def.split()[0], *col_def.split()[1:]]
                    writer.writerow(row)
        except IOError as e:
            return f"Error: Failed to write schema file due to {str(e)}"

        self.tables[table_name] = self.load_schema(table_name)
        return f"Table '{table_name}' created successfully."
    
    

    def validate_column_definition(self, col_def):
        # Simple validation for column type and constraints
        valid_types = {'INT', 'VARCHAR', 'YEAR'}
        parts = col_def.split()
        if parts[0] not in valid_types:
            return False
        return True


    def drop_table(self, table_name):
        if table_name not in self.tables:
            return "Error: Table does not exist."

        del self.tables[table_name]
        os.remove(os.path.join(self.schema_directory, f"{table_name}.csv"))
        return f"Table '{table_name}' dropped successfully."

    def load_schema(self, table_name):
        schema_path = os.path.join(self.schema_directory, f"{table_name}.csv")
        with open(schema_path, newline='') as file:
            reader = csv.reader(file)
            schema = {
                'columns': {},
                'primary_key': None,
                'foreign_keys': [],
                'indexes': []
            }
            for row in reader:
                col_name, col_type, *constraints = row
                schema['columns'][col_name] = {'type': col_type}
                for constraint in constraints:
                    if constraint == 'PRIMARY_KEY':
                        schema['primary_key'] = col_name
                    elif 'FOREIGN_KEY' in constraint:
                        ref_table, ref_col = constraint.split('(')[1].strip(')').split(',')
                        schema['foreign_keys'].append({'column': col_name, 'ref_table': ref_table, 'ref_column': ref_col})
                    elif constraint == 'INDEX':
                        schema['indexes'].append(col_name)
        return schema

# Example usage for testing
if __name__ == "__main__":
    ddl = DDLManager()
    print(ddl.create_table('state_population_2', {
        'state_id': 'INT PRIMARY_KEY',
        'state_name': 'VARCHAR(100)',
        'population': 'INT',
        'year': 'YEAR FOREIGN_KEY(years,id) INDEX'
    }))
    print(ddl.drop_table('state_population_2'))
