import json

def load_schema(file_path):
    with open(file_path, 'r') as file:
        schema = json.load(file)
    return schema

def create_indices(schema):
    indices = {}
    for table_name, table_info in schema['tables'].items():
        if 'indices' in table_info:
            for index in table_info['indices']:
                column = index['column']
                indices[(table_name, column)] = BTree(3)  # Example: using t=3 for B-Trees
    return indices

# Load schema and create indices
schema = load_schema('schema.json')
indices = create_indices(schema)
