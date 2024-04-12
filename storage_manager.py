import csv
import os
import logging

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class StorageManager:
    def __init__(self, data_directory="data"):
        """
        Initialize the Storage Manager with a directory to store and manage CSV files.
        """
        self.data_directory = data_directory
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        self.tables = self.load_all_data()

    def read_csv(self, filename, encodings=('utf-8-sig', 'ISO-8859-1')):
        """
        Attempts to read a CSV file using a list of encodings until one succeeds.
        Includes detailed logging to diagnose encoding issues.
        """
        path = os.path.join(self.data_directory, filename)
        for encoding in encodings:
            try:
                with open(path, mode='r', newline='', encoding=encoding) as file:
                    reader = csv.DictReader(file)
                    return [row for row in reader]  # Successfully read and return the data
            except UnicodeDecodeError as e:
                logging.error(f"Failed to decode {filename} with {encoding}: {e}")
            except Exception as e:
                logging.error(f"An error occurred while reading {filename} with {encoding}: {e}")
        # If all encoding attempts fail, log and return an empty list
        logging.error(f"All encodings failed for {filename}. Check the file for data issues.")
        return []

    def update_table(self, table_name, data):
        """
        Update the in-memory and disk storage for a given table.

        Parameters:
            table_name (str): The name of the table to update.
            data (list of dict): The complete updated data list for the table.
        """
        self.tables[table_name] = data
        self.write_csv(table_name + '.csv', data)

    def write_csv(self, filename, data):
        """
        Write data back to a CSV file after updates.

        Parameters:
            filename (str): The name of the file to write.
            data (list of dict): Data to write.
        """
        path = os.path.join(self.data_directory, filename)
        if data:
            keys = data[0].keys()
            with open(path, 'w', newline='', encoding='utf-8') as file:
                writer = csv.DictWriter(file, fieldnames=keys)
                writer.writeheader()
                writer.writerows(data)

    def load_all_data(self):
        """
        Load data from all CSV files in the directory, attempting multiple encodings.

        Returns:
            dict: A dictionary where keys are filenames and values are lists of rows.
        """
        data = {}
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                data[filename[:-4]] = self.read_csv(filename)
        return data

# Example usage of StorageManager
if __name__ == "__main__":
    storage = StorageManager()
    data = storage.load_all_data()
    for table_name, rows in data.items():
        logging.info(f"Loaded {len(rows)} rows from {table_name}")
