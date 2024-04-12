class BPlusTree:
    def __init__(self):
        self.nodes = {}

    def insert(self, key, value):
        self.nodes[key] = value

    def find(self, key):
        return self.nodes.get(key, None)

    def delete(self, key):
        if key in self.nodes:
            del self.nodes[key]

class IndexManager:
    def __init__(self):
        self.indices = {}

    def create_index(self, table_name, column_name):
        """
        Create a new index for a table on a specific column.

        Parameters:
            table_name (str): The name of the table.
            column_name (str): The column to index.
        """
        if table_name not in self.indices:
            self.indices[table_name] = {}
        if column_name not in self.indices[table_name]:
            self.indices[table_name][column_name] = BPlusTree()
        else:
            print("Index already exists for", table_name, "on column", column_name)

    def drop_index(self, table_name, column_name):
        """
        Drop an index for a table on a specific column.

        Parameters:
            table_name (str): The name of the table.
            column_name (str): The column index to drop.
        """
        if table_name in self.indices and column_name in self.indices[table_name]:
            del self.indices[table_name][column_name]
            if not self.indices[table_name]:
                del self.indices[table_name]
            print("Index dropped for", table_name, "on column", column_name)
        else:
            print("No index found for", table_name, "on column", column_name)

    def update_index(self, table_name, column_name, key, value):
        """
        Update an index by inserting or modifying an entry.

        Parameters:
            table_name (str): The name of the table.
            column_name (str): The column index to update.
            key (any): The key to insert or update in the index.
            value (any): The value associated with the key.
        """
        if table_name in self.indices and column_name in self.indices[table_name]:
            self.indices[table_name][column_name].insert(key, value)
        else:
            print("No index found for", table_name, "on column", column_name, "to update.")

    def search_index(self, table_name, column_name, key):
        """
        Search for a value in an index based on a key.

        Parameters:
            table_name (str): The name of the table.
            column_name (str): The column index to search.
            key (any): The key to search in the index.

        Returns:
            any: The value associated with the key, or None if not found.
        """
        if table_name in self.indices and column_name in self.indices[table_name]:
            return self.indices[table_name][column_name].find(key)
        else:
            print("No index found for", table_name, "on column", column_name)
            return None

# Example usage
if __name__ == "__main__":
    index_manager = IndexManager()
    index_manager.create_index('users', 'id')
    index_manager.update_index('users', 'id', 1, {'name': 'Alice', 'age': 30})
    print(index_manager.search_index('users', 'id', 1))
    index_manager.drop_index('users', 'id')
