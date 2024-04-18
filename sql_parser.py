# SQL_PARSER.py

import re
import logging

# from sql_parser import parse_sql
from execution_engine import execute_query
from storage_manager import StorageManager  # Assuming this exists for testing

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

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

     # Parsing logic based on type of SQL command
    elif command_type == 'update':
        # Handle UPDATE
        set_index = lower_sql.find(' set ') + 5
        where_index = lower_sql.find(' where ')
        if where_index != -1:
            set_str = sql[set_index:where_index].strip()
            parsed_details['where_condition'] = sql[where_index + 7:].strip()
        else:
            set_str = sql[set_index:].strip()
            where_index = len(sql)

        # Table name
        parsed_details['tables'].append(sql[7:set_index - 5].strip())

        # Parse set values
        parsed_details['values'] = {}
        for part in set_str.split(','):
            column, value = part.split('=')
            parsed_details['values'][column.strip()] = value.strip().strip("'")

        return parse_update(parsed_details, sql)

        
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
        
    elif command_type == 'drop':
        # Extract table name from the DROP TABLE command
        table_name = tokens[2].strip()
        return {'type': 'drop', 'table_name': table_name}

    elif command_type == 'create' and 'table' in tokens:
        return parse_create_table(sql)
    
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}

    
    return parsed_details

def parse_update(parsed_details, sql):
    # This function could further process or validate the parsed details
    # For now, let's just return what was passed as a demonstration
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

def parse_where_clause(where_clause):
    """Parses the WHERE clause into a list of conditions."""
    if not where_clause:
        return None

    conditions = []
    # Split conditions by ' AND ', ' OR ', considering possible nesting with parentheses
    tokens = re.split(r'\s+(AND|OR)\s+', where_clause, flags=re.IGNORECASE)
    current_op = None

    for token in tokens:
        if token.upper() in ['AND', 'OR']:
            current_op = token.upper()
        else:
            field, op, value = re.split(r'\s*(<=|>=|<>|!=|<|>|=)\s*', token, 1)
            condition = {'field': field.strip(), 'operator': op, 'value': value.strip(), 'logic_operator': current_op}
            conditions.append(condition)
            current_op = None  # Reset after adding condition

    return conditions

def parse_create_table(sql):
    # This is a simplified regex pattern; you might need a more robust implementation
    #pattern = r"CREATE TABLE (\w+) \((.*)\)"
    pattern = r'CREATE TABLE (\w+)\s*\((.*)\)\s*'
    match = re.match(pattern, sql)
    if not match:
        return {'error': 'Invalid CREATE TABLE syntax'}

    table_name, columns_part = match.groups()
    columns = parse_columns(columns_part)  # You will need to implement this to handle columns parsing
    if 'error' in columns:
        return {'error': columns['error']}
    return {'type': 'create', 'table_name': table_name, 'columns': columns}

def parse_columns(columns_part):
    """
    Parses the part of a SQL CREATE TABLE statement that defines columns and additional constraints like indexes.
    """
    columns = {}
    column_definitions = re.split(r',\s*(?![^()]*\))', columns_part)  # Improved splitting logic
    
    for column_def in column_definitions:
        if "INDEX" in column_def.upper():
            # Handle standalone and inline INDEX definitions
            index_match = re.search(r'INDEX\((\w+)\)', column_def, re.IGNORECASE)
            if index_match:
                index_column = index_match.group(1)
                if index_column in columns:
                    columns[index_column]['index'] = True
                else:
                    columns[index_column] = {'type': 'UNKNOWN', 'primary_key': False, 'index': True, 'foreign_key': None}
            continue
        
        column_name, constraints = parse_column_definition(column_def.strip())
        columns[column_name] = constraints
    
    return columns

def parse_column_definition(column_def):
    """
    Parse a single column definition to extract the column name, type, and constraints like primary key or foreign key.
    """
    # Use regex to split column definitions more reliably, considering cases without spaces
    parts = re.split(r'\s+', column_def, maxsplit=1)
    if len(parts) < 2:
        return column_def, {'type': 'UNKNOWN'}  # Handling cases with insufficient data more gracefully

    column_name = parts[0]
    rest_definition = parts[1]

    # Initialize constraints dictionary with the type set to 'UNKNOWN' by default
    constraints = {
        'type': 'UNKNOWN',
        'primary_key': False,
        'index': False,
        'foreign_key': None
    }

    # Extract type and additional constraints from the remaining definition part
    type_and_constraints = rest_definition.split()
    constraints['type'] = type_and_constraints[0]  # Assume first element is the type

    # Check for primary key, index, and foreign key constraints
    if 'PRIMARY KEY' in rest_definition.upper():
        constraints['primary_key'] = True
    if 'INDEX' in rest_definition.upper():
        constraints['index'] = True
    fk_match = re.search(r'FOREIGN KEY REFERENCES (\w+)\((\w+)\)', rest_definition, re.IGNORECASE)
    if fk_match:
        ref_table, ref_column = fk_match.groups()
        constraints['foreign_key'] = {'referenced_table': ref_table, 'referenced_column': ref_column}

    return column_name, constraints

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

def parse_drop(sql):
    """Parses a DROP TABLE statement"""
    pattern = r'\bDROP\s+TABLE\s+(\w+)\s+;?\b'
    match = re.match(pattern, sql, re.IGNORECASE)
    if not match:
        return {'error': 'Invalid DROP TABLE syntax'}
    return {
        'type': 'drop',
        'table': match.group(1)
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
#     logging.info("Starting SQL functionalities tests")

#     # Mock queries to test
#     queries = [
#         # "SELECT state FROM state_abbreviation",
#         # "SELECT * FROM state_abbreviation",
#         # "SELECT state FROM state_abbreviation WHERE state = 'Alaska'",
#         "SELECT monthly_state_population FROM state_population WHERE monthly_state_population > 5886320",
#         "SELECT * FROM state_population WHERE state_code = 'AK' AND year = '2018'",
#         "SELECT state FROM state_abbreviation WHERE state = 'California' OR state = 'Texas'",
#         # "INSERT INTO test_table (id, name) VALUES (45, 'Jihasd')",
#         # "DELETE FROM test_table WHERE id = 9",
#         "SELECT MAX(monthly_state_population) FROM state_population",
#         "SELECT MIN(monthly_state_population) FROM state_population",
#         "SELECT SUM(monthly_state_population) FROM state_population",
#         "SELECT a.state_code, b.state FROM state_population AS a JOIN state_abbreviation AS b ON a.state_code = b.state_code",
#         "SELECT sp.state_code, sa.state, sp.monthly_state_population FROM state_population AS sp JOIN state_abbreviation AS sa ON sp.state_code = sa.state_code WHERE sp.monthly_state_population > 6897721",
#         "SELECT state_name, population, year FROM state_population ORDER BY population DESC",
#         "SELECT state_name FROM state_population GROUP BY month HAVING SUM(population) > 1000000"
#     ]

#     # Mock storage manager for testing
#     storage_manager = StorageManager()

#     for query in queries:
#         logging.debug(f"Testing query: {query}")
#         parsed_command = parse_sql(query)
#         if 'error' in parsed_command:
#             logging.error(f"Parsing error in query: {query}")
#             continue

#         result = execute_query(parsed_command)
#         logging.info(f"Result for query '{query}': {result}")

#     logging.info("SQL functionalities tests completed")

if __name__ == "__main__":
    # Sample test SQL for UPDATE
    test_sql = "SELECT state_code, AVG(monthly_state_population) FROM state_population GROUP BY state_code "
    result = parse_sql(test_sql)
    
    # Print the result to verify the output
    print("Parsed SQL details:")
    print(result)