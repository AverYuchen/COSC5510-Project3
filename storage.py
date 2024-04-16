import csv
import os
import logging

class StorageManager:
    def read_csv(self, filename):
        path = os.path.join(self.data_directory, filename)
        try:
            with open(path, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                rows = [row for row in reader]
                logging.info(f"{filename} loaded with {len(rows)} rows")
                print(f"Reading {filename}: {rows}")  # Add this line for direct output
                return rows
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            print(f"Error reading {filename}: {e}")  # Add this line for direct output
            return []

    def load_all_data(self):
        data = {}
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                table_name = filename[:-4]
                data[table_name] = self.read_csv(filename)
                # print(f"Loaded {table_name}: {data[table_name]}")  # Debug output
        return data

    def get_table_data(self, table_name):
        table_data = self.data.get(table_name, [])
        # print(f"Table Data for {table_name}: {table_data}")  # Debugging statement
        return table_data

    def __init__(self, data_directory="data"):
        self.data_directory = data_directory
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        self.data = self.load_all_data()

    def update_table_data(self, table_name, data):
        self.data[table_name] = data
        self.write_csv(table_name + '.csv', data)


    def write_csv(self, filename, data):
        path = os.path.join(self.data_directory, filename)
        with open(path, 'w', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sm = StorageManager("data")
    data = sm.load_all_data()
    print("Data loaded from storage:", data)
