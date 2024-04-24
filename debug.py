import logging
import os

# Assuming StorageManager is defined in storage.py
from storage import StorageManager

def test_schema_loading(storage_manager):
    print("Testing Schema Loading...")
    # Expected to load schemas or handle missing file
    assert isinstance(storage_manager.schemas, dict), "Schemas should be initialized as a dictionary."
    print("Schema loading test passed.")

def test_data_reading(storage_manager):
    print("Testing Data Reading...")
    # Prepare a test CSV file
    test_filename = 'test_data.csv'
    test_path = os.path.join(storage_manager.data_directory, test_filename)
    with open(test_path, 'w', newline='') as file:
        file.write("id,name\n1,Alice\n2,Bob")
    
    data = storage_manager.read_csv(test_filename)
    assert len(data) == 2, "Should read 2 rows of data."
    os.remove(test_path)  # Cleanup test file
    print("Data reading test passed.")

def test_data_writing(storage_manager):
    print("Testing Data Writing...")
    test_data = [{'id': '3', 'name': 'Charlie'}, {'id': '4', 'name': 'Dana'}]
    test_filename = 'write_test.csv'
    storage_manager.write_csv(test_filename, test_data)
    test_path = os.path.join(storage_manager.data_directory, test_filename)
    assert os.path.exists(test_path), "File should be written to the disk."
    
    # Read back the data to ensure correctness
    with open(test_path, 'r') as file:
        lines = file.readlines()
        assert len(lines) == 3, "File should include header and two data rows."
    os.remove(test_path)  # Cleanup test file
    print("Data writing test passed.")

def test_error_handling(storage_manager):
    print("Testing Error Handling...")
    result = storage_manager.read_csv("non_existent_file.csv")
    assert result == [], "Should return an empty list on file read error."
    print("Error handling test passed.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    data_directory = "test_data"
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    
    # Initialize StorageManager with a test directory
    sm = StorageManager(data_directory)
    sm.load_schemas()  # Optionally test schema loading if schemas.json is provided

    # Perform tests
    test_schema_loading(sm)
    test_data_reading(sm)
    test_data_writing(sm)
    test_error_handling(sm)
    print("All tests passed!")
