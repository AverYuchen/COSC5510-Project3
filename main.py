# main.py

from query_input_manager import handle_input
from storage import StorageManager

def main():
    print("Welcome to MyDBMS")
    print("Type SQL commands or 'exit' to quit.")
    
    storage_manager = StorageManager()  # Initialize storage manager
    storage_manager.define_schemas()  # Define schemas explicitly if not loaded from CSV
    
    while True:
        user_input = input("dbms> ").strip()
        if user_input.lower() == 'exit':
            print("Exiting MyDBMS.")
            break
        if user_input.startswith("'") and user_input.endswith("'"):
            user_input = user_input[1:-1]  # Remove single quotes around the command
        result, error = handle_input(user_input)
        if error:
            print("Error:", error)
        elif result:
            print("Query results:", result)
        else:
            print("No results returned.")

if __name__ == "__main__":
    main()
