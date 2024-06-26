# SQL_PARSER.py

import re
import logging

# from sql_parser import parse_sql
from execution_engine import ExecutionEngine

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def parse_sql(sql):
    #logging.debug(f"Debug Parsing SQL: {sql}")  # Log input SQL for debugging
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
    
    if tokens[0] == 'show' and 'tables' in tokens:
        return parse_show_tables(sql)

    # Parsing logic based on type of SQL command
    if command_type == 'select':
        return parse_select(sql)

    elif command_type == 'update':
        # Handle UPDATE
        set_index = lower_sql.find(' set ') + 5
        where_index = lower_sql.find(' where ')
        if where_index != -1:
            set_str = sql[set_index:where_index].strip()
            where_string = sql[where_index + 7:].replace(';','')
            parsed_details['where_condition'] = where_string.strip()
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
        
    elif command_type == 'create' and 'index' in tokens:
        return parse_create_index(sql)
    
    elif command_type == 'drop' and 'index' in tokens:
        return parse_drop_index(sql)
    elif command_type == 'create' and 'table' in tokens:
        return parse_create_table(sql)
    elif command_type == 'drop' and 'table' in tokens:
        return parse_drop_table(sql)
    

    
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}


def parse_show_tables(sql):
    # Simplified regex to match the "SHOW TABLES" command
    if re.match(r"^\s*SHOW\s+TABLES\s*;?\s*$", sql, re.IGNORECASE):
        return {'type': 'show_tables'}
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}


def parse_create_index(sql):
    # Updated regex to handle optional spaces more flexibly
    match = re.match(r"CREATE INDEX\s+(\w+)\s+ON\s+(\w+)\s+\((\w+)\)", sql, re.I)
    if match:
        index_name, table_name, column_name = match.groups()
        return {'type': 'CREATE_INDEX', 'index_name': index_name, 'table_name': table_name, 'column_name': column_name}
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}

def parse_create_index_statement(statement):
    # Example parsing logic; needs a real parser for robustness
    tokens = statement.split()
    index_name = tokens[2]
    table_name = tokens[4]
    column_name = tokens[5].strip('();')
    return index_name, table_name, column_name


def parse_drop_index(sql):
    # Match the 'DROP INDEX index_name ON table_name;' pattern
    match = re.match(r"DROP INDEX\s+(\w+)\s+ON\s+(\w+);", sql.strip(), re.I)
    if match:
        index_name, table_name = match.groups()
        return {'type': 'DROP_INDEX', 'index_name': index_name, 'table_name': table_name}
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}


def parse_drop_table(sql):
    match = re.match(r"DROP TABLE\s+(\w+)\;", sql, re.I)
    if match:
        table_name = match.group(1)
        return {'type': 'DROP_TABLE', 'table_name': table_name}
    else:
        return {'error': 'Unsupported SQL command or malformed SQL', 'sql': sql}


def parse_select(sql):
    #logging.debug(f"Parsing SELECT SQL: {sql}")
    # Simplified and corrected regex pattern to handle SQL syntax variations
    pattern = r'''
    SELECT\s+(.*?)\s+FROM\s+([\w]+(?:\s+AS\s+\w+)?)
    (.*?);?$
    '''

    match = re.match(pattern, sql, re.IGNORECASE | re.VERBOSE)
    if not match:
        #logging.error("Invalid SELECT syntax: " + sql)
        return {'error': 'Invalid SELECT syntax'}

    select_fields, main_table, remaining = match.groups()

    result = {
        'type': 'select',
        'main_table': main_table.strip(),
        'columns': [col.strip() for col in select_fields.split(',')],
        'join': [],
        'where_clause': None,
        'group_by': None,
        'order_by': None,
        'having': None
    }

    # Process remaining clauses dynamically
    if 'JOIN' in remaining.upper():
        join_pattern = r'(LEFT|RIGHT|FULL|INNER|OUTER)?\s+JOIN\s+([\w]+(?:\s+AS\s+\w+)?|\w+)\s+ON\s+([\w\s\.=]+)'
        join_matches = re.finditer(join_pattern, remaining, re.IGNORECASE)
        for jmatch in join_matches:
            join_type, join_table, join_condition = jmatch.groups()
            result['join'].append({
                'join_type': (join_type + ' JOIN').strip() if join_type else 'JOIN',
                'join_table': join_table.strip(),
                'join_condition': join_condition.strip()
            })
    
    # Dynamically find the end index for each clause
    clauses = ['WHERE', 'GROUP BY', 'ORDER BY', 'HAVING']
    clause_positions = {clause: remaining.upper().find(clause) for clause in clauses if remaining.upper().find(clause) != -1}
    sorted_clauses = sorted(clause_positions.items(), key=lambda x: x[1])

    for i, (clause, pos) in enumerate(sorted_clauses):
        # Find the start of the next clause to delimit current clause
        next_pos = len(remaining) if i == len(sorted_clauses) - 1 else sorted_clauses[i + 1][1]
        clause_content = remaining[pos + len(clause):next_pos].strip()

        if clause == 'WHERE':
            result['where_clause'] = clause_content
        elif clause == 'GROUP BY':
            result['group_by'] = clause_content
        elif clause == 'ORDER BY':
            result['order_by'] = clause_content
        elif clause == 'HAVING':
            result['having'] = clause_content


    return result

def parse_update(parsed_details, sql):
    # This function could further process or validate the parsed details
    # For now, let's just return what was passed as a demonstration
    parsed_details['type'] = 'update' #lower the string and keep the format
    parsed_details['tables'] = parsed_details['tables'][0]
    return parsed_details

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
    
def get_table_columns(self, table):
    # Assuming a method that retrieves a list of all column names for a given table
    return list(self.tables[table].columns.keys())



def parse_delete(sql):
    """Parses a DELETE FROM SQL statement."""
    pattern = r'DELETE FROM (\w+)( WHERE (.*))?\;'
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


# Test various SQL queries
if __name__ == "__main__":
    queries = [
        "SHOW TABLES"

        
    ]

    for query in queries:
        result = parse_sql(query)
        print("Testing Query:")
        print(query)
        print("Result from parse_sql function:")
        print(result)
        print("\n")