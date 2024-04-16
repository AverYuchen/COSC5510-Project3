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
    elif command_type == 'drop':
        return parse_drop(sql)
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
        'conditions': where_clause.strip() if where_clause else None,
        'additional': parse_additional_clauses(additional.strip()) if additional else None
    }

def parse_create_table(sql):
    """
    Parses a CREATE TABLE SQL statement.
    """
    pattern = r'CREATE TABLE (\w+)\s*\((.*)\)\s*'
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid CREATE TABLE syntax'}

    table_name, columns_part = match.groups()
    columns = parse_column_definitions(columns_part)
    
    return {
        'type': 'create',
        'table': table_name,
        'columns': columns
    }

def parse_column_definitions(columns_part):
    """Parses column definitions in a CREATE TABLE SQL statement."""
    columns = {}
    column_definitions = re.split(r'\s*,\s*(?![^()]*\))', columns_part)
    
    for column_definition in column_definitions:
        column_parts = column_definition.strip().split()
        column_name = column_parts[0]
        column_type = ' '.join(column_parts[1:])
        columns[column_name] = column_type

    return columns

def parse_insert(sql):
    """Parses an INSERT INTO SQL statement."""
    pattern = r'INSERT INTO\s+(\w+)\s+\((.*)\)\s+VALUES\s+\((.*)\)'
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

def parse_drop(sql):
    """Parses a DROP TABLE statement"""
    pattern = r'\bDROP\s+TABLE\s+(\w+)\s*;?\b'
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid DROP TABLE syntax'}
    return {
        'type': 'drop',
        'table': match.group(1)
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
    #for all sql query, when we do parse_sql, we get rid of semicolon. Should we make it back?
    print(parse_sql("SELECT MAX(monthly_state_population) FROM state_population"))
    print(parse_sql("SELECT MIN(count_alldrug) FROM county_count"))
    print(parse_sql("SELECT SUM(population) FROM county_count"))
    print(parse_sql("CREATE TABLE users (id INT, name VARCHAR(100), registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"))
    print(parse_sql("INSERT INTO users (id, name) VALUES (123, Kiki)"))
    print(parse_sql("DROP TABLE users"))
    sql_command = "CREATE TABLE users (id INT, name VARCHAR(100), registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
    print(parse_create_table(sql_command))
