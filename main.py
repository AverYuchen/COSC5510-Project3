from query_input_manager import handle_input

# def main():
#     print("Welcome to MyDBMS")
#     print("Type SQL commands or 'exit' to quit.")
#     while True:
#         user_input = input("dbms> ").strip()  # Strip whitespace from the input
#         if user_input.lower() == 'exit':
#             print("Exiting MyDBMS.")
#             break
#         if user_input:  # Check if input is not empty
#             result = handle_input(user_input)
#             if result:
#                 print(result)
#         else:
#             print("Please enter a valid SQL command or type 'exit' to quit.")

# if __name__ == "__main__":
#     main()

# main.py

print(handle_input("test"))  # Should print "Hello, world!"
