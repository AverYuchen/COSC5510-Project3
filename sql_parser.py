import re
import logging

def parse_sql(sql):
    logging.debug(f"Debug Parsing SQL: {sql}")  # Log input SQL for debugging
    sql = sql.strip()
    lower_sql = sql.lower()
    tokens = lower_sql.split()
    parsed_details = {
        'type': None,
        'select_fields': [],
        'tables': [],
        'join_type': None,
        'join_table': None,
        'join_condition': None,
        'where_condition': None,
        'values': None,
        'aggregation': None
    }

    # Detect SQL command type
    command_type = tokens[0]
    parsed_details['type'] = command_type.upper()

    # Parsing logic based on type of SQL command
    if command_type == 'select':
        # Handle SELECT fields and FROM clause
        from_index = lower_sql.find(' from ') + 6
        select_contents = sql[7:from_index - 6].strip()
        parsed_details['select_fields'] = [field.strip() for field in select_contents.split(',')]

        # Handle JOINs
        join_index = lower_sql.find(' join ')
        if join_index != -1:
            join_type_start = lower_sql.rfind(' ', 0, join_index) + 1
            parsed_details['join_type'] = lower_sql[join_type_start:join_index].strip().upper() + ' JOIN'
            on_index = lower_sql.find(' on ', join_index)
            parsed_details['join_table'] = sql[join_index + 6:on_index].strip().split()[0]
            parsed_details['join_condition'] = sql[on_index + 4:].strip()

        # Handle WHERE clause
        where_index = lower_sql.find(' where ')
        if where_index != -1:
            parsed_details['where_condition'] = sql[where_index + 7:].strip()

        # Determine table or tables involved
        table_end = where_index if where_index != -1 else len(sql)
        parsed_details['tables'] = [part.strip().split()[0] for part in sql[from_index:table_end].split(',')]

        # Extend parsing to handle other clauses such as GROUP BY, ORDER BY, etc.
        return parse_select(sql)

    elif command_type == 'insert':
        # Handle INSERT INTO table (fields) VALUES (values)
        into_index = lower_sql.find(' into ') + 6
        values_index = lower_sql.find(' values ') + 8
        table_end = lower_sql.find(' (', into_index)
        parsed_details['tables'].append(sql[into_index:table_end].strip())
        values_str = sql[values_index:].strip()[1:-1]
        parsed_details['values'] = [value.strip().strip("'") for value in values_str.split(',')]
        return parse_insert(sql)

    elif command_type == 'delete':
        # Handle DELETE FROM table WHERE condition
        from_index = lower_sql.find(' from ') + 6
        where_index = lower_sql.find(' where ')
        table_end = where_index if where_index != -1 else len(sql)
        parsed_details['tables'].append(sql[from_index:table_end].strip())
        if where_index != -1:
            parsed_details['where_condition'] = sql[where_index + 7:].strip()
        return parse_delete(sql)

    return parsed_details

def parse_select(sql):
    """Parses a SELECT SQL statement with better handling for aliases and joins."""
    pattern = r'''
    SELECT\s+(.*?)\s+FROM\s+([\w]+(?:\s+AS\s+\w+)?|\w+)   # Capture SELECT fields and main table with optional alias
    (\s+JOIN\s+[\w]+(?:\s+AS\s+\w+)?\s+ON\s+[\w\s\.=]+)?  # Optionally capture JOIN with alias
    (\s+WHERE\s+[\w\s\.\'=]+)?                           # Optionally capture WHERE clause
    (\s+GROUP\s+BY\s+[\w\s\.,]+)?                        # Optionally capture GROUP BY
    (\s+ORDER\s+BY\s+[\w\s\.,]+)?                        # Optionally capture ORDER BY
    (\s+HAVING\s+[\w\s\.\'=]+)?;*                        # Optionally capture HAVING
    '''

    match = re.match(pattern, sql, re.IGNORECASE | re.VERBOSE)
    if not match:
        return {'error': 'Invalid SELECT syntax'}

    select_fields, main_table, join_part, where_part, group_by, order_by, having = match.groups()
    
    # Parsing the table and alias correctly
    if ' AS ' in main_table:
        table_name, alias = main_table.split(' AS ')
        main_table = f"{table_name.strip()} AS {alias.strip()}"
    else:
        main_table = main_table.strip()

    result = {
        'type': 'select',
        'main_table': main_table,
        'columns': [col.strip() for col in select_fields.split(',')],
        'join': join_part.strip() if join_part else None,
        'where_clause': where_part[6:].strip() if where_part else None,
        'group_by': group_by[9:].strip() if group_by else None,
        'order_by': order_by[9:].strip() if order_by else None,
        'having': having[7:].strip() if having else None
    }
    return result

def parse_create_table(sql):
    """
    Parses a CREATE TABLE SQL statement.
    """
    pattern = r'CREATE TABLE (\w+)\s*\((.*)\)\s*;'
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

# if __name__ == "__main__":
#     test_queries = [
#         "SELECT state FROM state_abbreviation",
#         "SELECT * FROM state_abbreviation",
#         "SELECT state FROM state_abbreviation WHERE state = 'Alaska'",
#         "SELECT * FROM state_population WHERE state_code = 'AK' AND year = '2018'",
#         "SELECT state FROM state_abbreviation WHERE state = 'California' OR state = 'Texas'",
#         "SELECT * FROM state_abbreviation WHERE state = 'California' OR state = 'Texas'",
#         "INSERT INTO test_table (id, name) VALUES (2, 'Happy')",
#         "DELETE FROM test_table WHERE id = 1",
#         "SELECT MAX(monthly_state_population) FROM state_population",
#         "SELECT MIN(count_alldrug) FROM county_count",
#         "SELECT SUM(monthly_state_population) FROM state_population",
#         "SELECT a.state_code, b.state FROM state_population AS a JOIN state_abbreviation AS b ON a.state_code = b.state_code"
#     ]

#     for query in test_queries:
#         print(parse_sql(query))

if __name__ == "__main__":
    # Test the parser with various SQL commands
    test_queries = [
        "SELECT state_code, state FROM state_population AS a JOIN state_abbreviation AS b ON a.state_code = b.state_code",
        "INSERT INTO test_table (id, name) VALUES (2, 'Happy')",
        "DELETE FROM test_table WHERE id = 1",
        "SELECT MAX(monthly_state_population) FROM state_population"
    ]

    for query in test_queries:
        result = parse_sql(query)
        print("Parsed SQL Command:", result)

