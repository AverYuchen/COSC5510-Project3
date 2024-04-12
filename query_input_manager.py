import csv
import os
import logging
# from sql_parser import parse_sql  # Assume a function to parse SQL commands
# from execution_engine import execute_query  # Assume a function to execute queries

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

print(os.getcwd())

# def handle_input(user_input):
#     """
#     Process user input, parse SQL commands, and execute them.
    
#     Parameters:
#         user_input (str): SQL command or query from the user.

#     Returns:
#         str: Result of the command execution or error message.
#     """
#     try:
#         # Assuming parse_sql parses the user input into a structured command
#         parsed_command = parse_sql(user_input)
        
#         # Assuming execute_query executes the parsed SQL command using StorageManager
#         result = execute_query(parsed_command)
        
#         return format_result(result)
#     except Exception as e:
#         logging.error("Failed to process user input", exc_info=True)
#         return f"An error occurred: {str(e)}"

# def format_result(result):
#     """
#     Formats the execution results for display.
#     """
#     if isinstance(result, list):
#         return "\n".join(str(row) for row in result)
#     elif isinstance(result, dict):
#         return "\n".join(f"{key}: {value}" for key, value in result.items())
#     return str(result)

def handle_input(user_input):
         return "Hello, world!"

class StorageManager:
    def __init__(self, data_directory="data"):
        """
        Initialize the Storage Manager with a directory to store and manage CSV files.
        """
        self.data_directory = data_directory
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        self.data = self.load_all_data()
        # This dictionary stores all the table data
        
        # query_input_manager.py


    def get_table_data(self, table_name):
        """
        Retrieve data for a specific table.
        """
        return self.data.get(table_name, [])

    def update_table_data(self, table_name, data):
        """
        Update the in-memory and disk storage for a given table.
        """
        if data and isinstance(data, list) and isinstance(data[0], dict):
            self.data[table_name] = data
            self.write_csv(table_name + '.csv', data)
        else:
            logging.warning(f"No data to write for {table_name} or data format incorrect.")

    def read_csv(self, filename, encodings=('utf-8-sig', 'ISO-8859-1')):
        """
        Attempts to read a CSV file using a list of encodings until one succeeds.
        """
        path = os.path.join(self.data_directory, filename)
        for encoding in encodings:
            try:
                with open(path, mode='r', newline='', encoding=encoding) as file:
                    reader = csv.DictReader(file)
                    return [row for row in reader]
            except UnicodeDecodeError as e:
                logging.error(f"Failed to decode {filename} with {encoding}: {e}")
            except Exception as e:
                logging.error(f"An error occurred while reading {filename} with {encoding}: {e}")
        logging.error(f"All encodings failed for {filename}. Data may be corrupted or unreadable.")
        return []

    def write_csv(self, filename, data):
        """
        Write data back to a CSV file after updates.
        """
        path = os.path.join(self.data_directory, filename)
        if data:
            keys = data[0].keys()
            with open(path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)
        else:
            logging.info(f"No data to write for {filename}")

    def load_all_data(self):
        """
        Load data from all CSV files in the directory.
        """
        data = {}
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                table_name = filename[:-4]
                table_data = self.read_csv(filename)
                if table_data:
                    data[table_name] = table_data
                    logging.info(f"Loaded {len(table_data)} rows from {table_name}")
                else:
                    logging.warning(f"Failed to load data from {filename}")
        return data

# Example usage of StorageManager
# if __name__ == "__main__":
#     storage = StorageManager()
#     data = storage.load_all_data()
#     for table_name, rows in data.items():
#         logging.info(f"Loaded {len(rows)} rows from {table_name}")

if __name__ == "__main__":
    storage = StorageManager()
    data = storage.load_all_data()
    for table_name, rows in data.items():
        logging.info(f"Loaded {len(rows)} rows from {table_name}")