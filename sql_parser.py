import re

def parse_sql(sql):
    """
    Parses an SQL string into a command dictionary.
    """
    sql = sql.strip()
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
        raise ValueError("Unsupported SQL command: " + sql)

def parse_select(sql):
    """
    Parses a SELECT SQL statement.
    """
    pattern = r"SELECT\s+(.*?)\s+FROM\s+(\w+)(\s+WHERE\s+(.*?))?(.*)"
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid SELECT syntax'}

    columns, table, _, where_clause, additional = match.groups()
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
    pattern = r"CREATE TABLE (\w+) \((.*)\);"
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid CREATE TABLE syntax'}

    table_name, columns = match.groups()
    column_defs = {col.split()[0]: ' '.join(col.split()[1:]) for col in columns.split(',')}
    return {
        'type': 'create',
        'table': table_name,
        'columns': column_defs
    }

def parse_insert(sql):
    """
    Parses an INSERT INTO SQL statement.
    """
    pattern = r"INSERT INTO (\w+) \((.*)\) VALUES \((.*)\);"
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid INSERT syntax'}

    table_name, columns, values = match.groups()
    return {
        'type': 'insert',
        'table': table_name,
        'columns': [col.strip() for col in columns.split(',')],
        'values': [value.strip().strip("'") for value in values.split(',')]
    }

def parse_delete(sql):
    """
    Parses a DELETE FROM SQL statement.
    """
    pattern = r"DELETE FROM (\w+)( WHERE (.*))?"
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
    """
    Parses additional clauses in SQL statements like ORDER BY, GROUP BY, and HAVING.
    """
    additional = {}
    if 'order by' in clause.lower():
        additional['order_by'] = re.search(r"ORDER BY (.+)", clause, re.IGNORECASE).group(1)
    if 'group by' in clause.lower():
        additional['group_by'] = re.search(r"GROUP BY (.+)", clause, re.IGNORECASE).group(1)
    if 'having' in clause.lower():
        additional['having'] = re.search(r"HAVING (.+)", clause, re.IGNORECASE).group(1)
    
    return additional

# Example usage for testing
if __name__ == "__main__":
    # Test various SQL statements
    print(parse_sql("SELECT state, population FROM state_abbreviation WHERE state = 'California';"))
    print(parse_sql("CREATE TABLE state_population (state_id INT PRIMARY KEY, state_name VARCHAR(100), population INT, year YEAR);"))
    print(parse_sql("INSERT INTO state_population (state_id, state_name, population, year) VALUES (1, 'California', 39538223, 2020);"))
    print(parse_sql("DELETE FROM state_population WHERE state_id = 1;"))
