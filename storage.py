import csv
import os
import logging

class StorageManager:
    def __init__(self, data_directory="data"):
        self.data_directory = data_directory
        self.data = self.load_all_data()

    # Include all methods related to data handling here

    def load_all_data(self):
        # Implementation
        pass
