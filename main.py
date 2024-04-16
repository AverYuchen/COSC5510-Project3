from query_input_manager import handle_input

def main():
    print("Welcome to MyDBMS")
    print("Type SQL commands or 'exit' to quit.")
    while True:
        user_input = input("dbms> ").strip()
        if user_input.lower() == 'exit':
            print("Exiting MyDBMS.")
            break
        if user_input.startswith("'") and user_input.endswith("'"):
            user_input = user_input[1:-1]  # Remove single quotes around the command
        result = handle_input(user_input)
        if result:
            print("Query results:", result)
        else:
            print("No results or feedback received. Check your input or system configuration.")

if __name__ == "__main__":
    main()
