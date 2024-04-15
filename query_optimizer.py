from index_manager import IndexManager

class QueryOptimizer:
    def __init__(self, index_manager):
        self.index_manager = index_manager

    def optimize_query(self, query):
        # Example of optimizing a SELECT query
        if query['type'] == 'select':
            return self.optimize_select(query)
    
    def optimize_select(self, query):
        table = query['table']
        for column in query.get('columns', []):
            if self.index_manager.is_indexed(table, column):
                print(f"Using index for {column} in {table}")
            else:
                print(f"No index for {column} in {table}")
        return query

# Example usage
index_manager = IndexManager()
optimizer = QueryOptimizer(index_manager)
query = {'type': 'select', 'table': 'users', 'columns': ['id', 'name']}
optimized_query = optimizer.optimize_query(query)
