from execution_engine import execute_query
from sql_parser import parse_sql

def handle_input(user_input):
    print(f"Query_Input_debug: {user_input}")  # Log input SQL for debugging
    command = parse_sql(user_input)

    if command:
        if 'error' in command:
            return None, command['error']  # Return None for result, error message for error
        result = execute_query(command)
        if result is None:
            return None, "No result found or error occurred"
        return result, None  # Return result, None for error
    else:
        return None, "Error parsing SQL command."

if __name__ == "__main__":
    # Test your SQL handling
    result, error = handle_input("INSERT INTO test_table (id, name) VALUES (3, 'Bob');")
    print("Result:", result, "Error:", error)
    result, error = handle_input("SELECT * FROM test_table;")
    print("Result:", result, "Error:", error)
