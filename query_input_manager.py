from sql_parser import parse_sql
from execution_engine import execute_query
from ddl import create_table
from dml import insert, select, delete

def handle_input(user_input):
    """
    Handles a SQL command as input, parses it, and executes the appropriate action.

    Parameters:
        user_input (str): The SQL command entered by the user.

    Returns:
        str: The result or output of the executed command.
    """
    try:
        # Parse the SQL command
        parsed_command = parse_sql(user_input)

        # Dispatch the command to the appropriate function
        if parsed_command['type'] == 'select':
            result = select(**parsed_command)
        elif parsed_command['type'] == 'insert':
            result = insert(**parsed_command)
        elif parsed_command['type'] == 'create':
            result = create_table(**parsed_command)
        elif parsed_command['type'] == 'delete':
            result = delete(**parsed_command)
        else:
            result = "Error: Unsupported SQL command type."

        # Return formatted result for display
        return format_result(result)
    except Exception as e:
        # Return error message if an exception occurs
        return f"An error occurred: {e}"

def format_result(result):
    """
    Formats the result of an SQL command for display.
    """
    if isinstance(result, list):
        return "\n".join(str(row) for row in result)
    elif isinstance(result, dict):
        return "\n".join(f"{key}: {value}" for key, value in result.items())
    else:
        return str(result)

# Example usage for testing based on the provided SQL queries
if __name__ == "__main__":
    print(handle_input("SELECT state FROM state_abbreviation;"))
    print(handle_input("SELECT a.county_name, b.fips_number FROM county_count AS a JOIN fips_code AS b ON a.county_name = b.county_name;"))
    print(handle_input("SELECT county_name, total_count FROM county_count;"))
    print(handle_input("CREATE TABLE state_population (state_id INT PRIMARY KEY, state_name VARCHAR(100), population INT, year YEAR);"))
    print(handle_input("INSERT INTO state_population (state_id, state_name, population, year) VALUES (1, 'California', 39538223, 2020);"))
    print(handle_input("INSERT INTO state_population (state_id, state_name, population, year) VALUES (2, 'California', 38834137, 2021);"))
    print(handle_input("INSERT INTO state_population (state_id, state_name, population, year) VALUES (3, 'California', 38243082, 2022);"))
    print(handle_input("SELECT * FROM state_abbreviation WHERE state_name = 'California' OR state_name = 'Texas';"))
    print(handle_input("SELECT state_name, population, year FROM state_population ORDER BY population DESC;"))
    print(handle_input("SELECT state_name, SUM(population) AS total_population FROM state_population GROUP BY state_name HAVING SUM(population) > 10000000;"))
