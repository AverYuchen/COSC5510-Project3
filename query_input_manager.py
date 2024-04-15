from execution_engine import execute_query
from sql_parser import parse_sql

def handle_input(user_input):
    command = parse_sql(user_input)
    if command.get('error'):
        return command['error']
    return execute_query(command)

if __name__ == "__main__":
    # Test your SQL handling
    print(handle_input("INSERT INTO test_table (id, name) VALUES (3, 'Bob');"))
    print(handle_input("SELECT * FROM test_table;"))
