from query_input_manager import handle_input

def main():
    print("Welcome to MyDBMS")
    print("Type SQL commands or 'exit' to quit.")
    
    while True:
        try:
            # Prompting user for input
            user_input = input("dbms> ")
            if user_input.lower() == "exit":
                print("Exiting MyDBMS.")
                break
            
            # Handling the user input
            handle_input(user_input)

        except Exception as e:
            # Handling unexpected errors gracefully
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()
