import re

def parse_sql(sql):
    """Parses an SQL string into a command dictionary."""
    sql = sql.strip().rstrip(';')  # Remove leading/trailing spaces and trailing semicolon
    command_type = sql.split()[0].lower()

    if command_type == 'select':
        return parse_select(sql)
    elif command_type == 'create':
        return parse_create_table(sql)
    elif command_type == 'insert':
        return parse_insert(sql)
    elif command_type == 'delete':
        return parse_delete(sql)
    else:
        return {'error': "Unsupported SQL command: " + sql}

def parse_select(sql):
    """Parses a SELECT SQL statement."""
    pattern = r'SELECT\s+(.*?|\*)\s+FROM\s+(\w+)(\s+WHERE\s+((?:.(?!order by))*))?(\s+(.*))?'

    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid SELECT syntax'}

    columns, table, __, where_clause, __, additional = match.groups()
    return {
        'type': 'select',
        'table': table,
        'columns': [column.strip() for column in columns.split(',')],
        'conditions': where_clause if where_clause else None,
        'additional': parse_additional_clauses(additional.strip()) if additional else None
    }

def parse_create_table(sql):
    """
    Parses a CREATE TABLE SQL statement.

    Args:
        sql (str): The CREATE TABLE statement.

    Returns:
        dict: A dictionary containing the type of SQL command, the table name, and the columns with their specifications.
    """
    pattern = r'CREATE TABLE (\w+)\s*\((.*)\)\s*;'
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid CREATE TABLE syntax'}

    table_name, columns_part = match.groups()
    columns = {}
    # Splitting each column entry correctly handling cases where commas may be inside type definitions (e.g., VARCHAR(255))
    column_definitions = re.split(r'\s*,\s*(?![^()]*\))', columns_part)
    
    for column_definition in column_definitions:
        column_parts = column_definition.strip().split()
        column_name = column_parts[0]
        column_type = ' '.join(column_parts[1:])
        columns[column_name] = column_type

    return {
        'type': 'create',
        'table': table_name,
        'columns': columns
    }

def parse_insert(sql):
    """Parses an INSERT INTO SQL statement."""
    pattern = r"INSERT INTO (\w+) \((.*)\) VALUES \((.*)\);"
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid INSERT syntax'}

    table_name, columns, values = match.groups()
    columns = [col.strip() for col in columns.split(',')]
    values = [value.strip().strip("'") for value in values.split(',')]
    data = dict(zip(columns, values))

    return {
        'type': 'insert',
        'table': table_name,
        'data': data
    }


# def parse_insert(sql):
#     """Parses an INSERT INTO SQL statement."""
#     pattern = r'INSERT INTO (\w+) \((.*)\) VALUES \((.*)\)'
#     match = re.match(pattern, sql, re.IGNORECASE)
#     if not match:
#         return {'error': 'Invalid INSERT syntax'}

#     table_name, columns, values = match.groups()
#     columns = [col.strip() for col in columns.split(',')]
#     values = [value.strip().strip("'") for value in values.split(',')]

#     return {
#         'type': 'insert',
#         'table': table_name,
#         'columns': columns,
#         'values': values
#     }

def parse_delete(sql):
    """Parses a DELETE FROM SQL statement."""
    pattern = r'DELETE FROM (\w+)( WHERE (.*))?'
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid DELETE syntax'}

    table, _, conditions = match.groups()
    return {
        'type': 'delete',
        'table': table,
        'conditions': conditions.strip() if conditions else None
    }

def parse_additional_clauses(clause):
    """Parses additional clauses in SQL statements like ORDER BY, GROUP BY, and HAVING."""
    additional = {}
    if 'order by' in clause.lower():
        additional['order_by'] = re.search(r'ORDER BY (.+)', clause, re.IGNORECASE).group(1)
    if 'group by' in clause.lower():
        additional['group_by'] = re.search(r'GROUP BY (.+)', clause, re.IGNORECASE).group(1)
    if 'having' in clause.lower():
        additional['having'] = re.search(r'HAVING (.+)', clause, re.IGNORECASE).group(1)

    return additional

# Example usage for testing
if __name__ == "__main__":
    print(parse_sql("SELECT * FROM users;"))
    print(parse_sql("SELECT id FROM users;"))
    print(parse_sql("SELECT * FROM state_abbreviation where state_code = 'NY';"))
    print(parse_sql("SELECT id, name FROM users WHERE id = 1 ORDER BY name;"))
    print(parse_sql("CREATE TABLE users (id INT, name VARCHAR(100));"))
    print(parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice');"))
    print(parse_sql("DELETE FROM users WHERE id = 1;"))
    sql_command = "CREATE TABLE users (id INT, name VARCHAR(100), registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
    print(parse_create_table(sql_command))
# import re

# def parse_sql(sql):
#     """
#     Parses an SQL string into a command dictionary.
#     """
#     sql = sql.strip()
#     command_type = sql.split()[0].lower()
#     if command_type == 'select':
#         return parse_select(sql)
#     elif command_type == 'create':
#         return parse_create_table(sql)
#     elif command_type == 'insert':
#         return parse_insert(sql)
#     elif command_type == 'delete':
#         return parse_delete(sql)
#     else:
#         raise ValueError("Unsupported SQL command: " + sql)

# def parse_select(sql):
#     """
#     Parses a SELECT SQL statement.
#     """
#     pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(\s+WHERE\s+(.*?))?(.*)"
#     match = re.match(pattern, sql, re.IGNORECASE)
#     if not match:
#         return {'error': 'Invalid SELECT syntax'}

#     columns, table, _, where_clause, additional = match.groups()
#     return {
#         'type': 'select',
#         'table': table,
#         'columns': [column.strip() for column in columns.split(',')],
#         'conditions': where_clause.strip() if where_clause else None,
#         'additional': parse_additional_clauses(additional.strip()) if additional else None
#     }

# def parse_create_table(sql):
#     """
#     Parses a CREATE TABLE SQL statement.
#     """
#     pattern = r"CREATE TABLE (\w+) \((.*)\);"
#     match = re.match(pattern, sql, re.IGNORECASE)
#     if not match:
#         return {'error': 'Invalid CREATE TABLE syntax'}

#     table_name, columns = match.groups()
#     column_defs = {col.split()[0]: ' '.join(col.split()[1:]) for col in columns.split(',')}
#     return {
#         'type': 'create',
#         'table': table_name,
#         'columns': column_defs
#     }

# def parse_insert(sql):
#     """
#     Parses an INSERT INTO SQL statement.
#     """
#     pattern = r"INSERT INTO (\w+) \((.*)\) VALUES \((.*)\);"
#     match = re.match(pattern, sql, re.IGNORECASE)
#     if not match:
#         return {'error': 'Invalid INSERT syntax'}

#     table_name, columns, values = match.groups()
#     return {
#         'type': 'insert',
#         'table': table_name,
#         'columns': [col.strip() for col in columns.split(',')],
#         'values': [value.strip().strip("'") for value in values.split(',')]
#     }

# def parse_delete(sql):
#     """
#     Parses a DELETE FROM SQL statement.
#     """
#     pattern = r"DELETE FROM (\w+)( WHERE (.*))?"
#     match = re.match(pattern, sql, re.IGNORECASE)
#     if not match:
#         return {'error': 'Invalid DELETE syntax'}

#     table, _, conditions = match.groups()
#     return {
#         'type': 'delete',
#         'table': table,
#         'conditions': conditions.strip() if conditions else None
#     }

# def parse_additional_clauses(clause):
#     """
#     Parses additional clauses in SQL statements like ORDER BY, GROUP BY, and HAVING.
#     """
#     additional = {}
#     if 'order by' in clause.lower():
#         additional['order_by'] = re.search(r"ORDER BY (.+)", clause, re.IGNORECASE).group(1)
#     if 'group by' in clause.lower():
#         additional['group_by'] = re.search(r"GROUP BY (.+)", clause, re.IGNORECASE).group(1)
#     if 'having' in clause.lower():
#         additional['having'] = re.search(r"HAVING (.+)", clause, re.IGNORECASE).group(1)
    
#     return additional

# # Example usage for testing
# if __name__ == "__main__":
#     # Test various SQL statements
#     print(parse_sql("SELECT state, population FROM state_abbreviation WHERE state = 'California';"))
#     print(parse_sql("CREATE TABLE state_population (state_id INT PRIMARY KEY, state_name VARCHAR(100), population INT, year YEAR);"))
#     print(parse_sql("INSERT INTO state_population (state_id, state_name, population, year) VALUES (1, 'California', 39538223, 2020);"))
#     print(parse_sql("DELETE FROM state_population WHERE state_id = 1;"))
