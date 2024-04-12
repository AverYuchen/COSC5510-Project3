class MemoryExecution:
    def __init__(self, storage_manager):
        """
        Initialize the memory execution engine with a reference to the storage manager for data access.

        Parameters:
            storage_manager (StorageManager): The storage manager to fetch data from.
        """
        self.storage_manager = storage_manager

    def select(self, table_name, conditions=None, projection=None, order_by=None, group_by=None, having=None):
        """
        Selects data from a table and performs operations like filtering, projection, and sorting.

        Parameters:
            table_name (str): The name of the table to perform the query on.
            conditions (function): A function that determines which rows to include.
            projection (list): A list of column names to include in the results.
            order_by (str): The column name to sort the results by.
            group_by (str): The column name to group the results by.
            having (function): A function to filter groups.

        Returns:
            list: A list of dictionaries representing the selected rows.
        """
        data = self.storage_manager.tables.get(table_name, [])
        
        # Filtering based on conditions
        if conditions:
            data = [row for row in data if conditions(row)]

        # Projection
        if projection:
            data = [{col: row[col] for col in projection if col in row} for row in data]

        # Sorting
        if order_by:
            data.sort(key=lambda x: x.get(order_by))

        # Grouping and aggregation
        if group_by:
            from itertools import groupby
            data.sort(key=lambda x: x[group_by])  # Necessary for groupby to work
            grouped_data = groupby(data, key=lambda x: x[group_by])
            if having:
                data = [group for key, group in grouped_data if having(list(group))]
            else:
                data = [list(group) for _, group in grouped_data]

        return data

    def insert(self, table_name, row):
        """
        Inserts a row into the specified table.

        Parameters:
            table_name (str): The table to insert the data into.
            row (dict): The data to insert as a dictionary of column values.
        """
        self.storage_manager.tables[table_name].append(row)
        self.storage_manager.update_table(table_name, self.storage_manager.tables[table_name])

    def delete(self, table_name, condition):
        """
        Deletes rows from the specified table based on a condition.

        Parameters:
            table_name (str): The table to delete from.
            condition (function): A function to determine which rows to delete.
        """
        original_data = self.storage_manager.tables[table_name]
        new_data = [row for row in original_data if not condition(row)]
        self.storage_manager.update_table(table_name, new_data)

# Example usage
if __name__ == "__main__":
    from storage_manager import StorageManager
    storage_manager = StorageManager()
    mem_exec = MemoryExecution(storage_manager)
    print(mem_exec.select('state_population', conditions=lambda x: x['state_code'] == 'AK', projection=['state_code','year']))

