import csv
import os
import logging

class StorageManager:
    def __init__(self, data_directory="data"):
        self.data_directory = data_directory
        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        self.data = self.load_all_data()

    def get_table_data(self, table_name):
        return self.data.get(table_name, [])

    def update_table_data(self, table_name, data):
        self.data[table_name] = data
        self.write_csv(table_name + '.csv', data)

    def read_csv(self, filename):
        path = os.path.join(self.data_directory, filename)
        try:
            with open(path, mode='r', encoding='utf-8-sig') as file:
                reader = csv.DictReader(file)
                return [row for row in reader]
        except Exception as e:
            logging.error(f"Failed to read {filename}: {e}")
            return []

    def write_csv(self, filename, data):
        path = os.path.join(self.data_directory, filename)
        with open(path, 'w', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

    def load_all_data(self):
        data = {}
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.csv'):
                table_name = filename[:-4]
                data[table_name] = self.read_csv(filename)
        return data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sm = StorageManager("data")
    data = sm.load_all_data()
    print("Data loaded from storage:", data)
