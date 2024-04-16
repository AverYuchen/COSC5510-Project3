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

 
# import unittest
# from sql_parser import parse_sql  # Adjust this import based on your actual file and function names

# class TestSQLParser(unittest.TestCase):
#     def test_select_simple(self):
#         sql = "SELECT * FROM test_table"
#         result = parse_sql(sql)
#         self.assertEqual(result['type'], 'SELECT')
#         self.assertIn('test_table', result['tables'])

#     def test_insert(self):
#         sql = "INSERT INTO test_table (id, name) VALUES (1, 'Hachii')"
#         result = parse_sql(sql)
#         self.assertEqual(result['type'], 'INSERT')
#         self.assertIn('test_table', result['tables'])
#         self.assertIn('1', result['values'])
#         self.assertIn('Hachii', result['values'])

#     def test_delete(self):
#         sql = "DELETE FROM test_table WHERE id = 1"
#         result = parse_sql(sql)
#         self.assertEqual(result['type'], 'DELETE')
#         self.assertIn('test_table', result['tables'])
#         self.assertEqual(result['where_condition'], 'id = 1')

#     def test_join_simple(self):
#         sql = "SELECT a.state, b.capital FROM state_table a JOIN capital_table b ON a.state_id = b.state_id"
#         result = parse_sql(sql)
#         self.assertEqual(result['join_type'], 'JOIN')
#         self.assertIn('state_table', result['tables'])
#         self.assertIn('capital_table', result['join_table'])
#         self.assertEqual(result['join_condition'], "a.state_id = b.state_id")

#     def test_select_simple(self):
#         sql = "SELECT state FROM state_abbreviation"
#         result = parse_sql(sql)
#         self.assertEqual(result['type'], 'SELECT')
#         self.assertIn('state_abbreviation', result['tables'])
#         self.assertIn('state', result['select_fields'])

#     def test_and_condition(self):
#         sql = "SELECT * FROM state_population WHERE state_code = 'AK' AND year = '2018'"
#         result = parse_sql(sql)
#         self.assertIn('AND', result['where_condition'])

#     def test_or_condition(self):
#         sql = "SELECT state FROM state_abbreviation WHERE state = 'California' OR state = 'Texas'"
#         result = parse_sql(sql)
#         self.assertIn('OR', result['where_condition'])

#     def test_where_condition(self):
#         sql = "SELECT state FROM state_abbreviation WHERE state = 'Alaska'"
#         result = parse_sql(sql)
#         self.assertEqual(result['where_condition'], "state = 'Alaska'")

#     def test_max_function(self):
#         sql = "SELECT MAX(monthly_state_population) FROM state_population"
#         result = parse_sql(sql)
#         self.assertEqual(result['aggregation'], 'MAX')
#         self.assertIn('monthly_state_population', result['select_fields'])

#     def test_min_function(self):
#         sql = "SELECT MIN(count_alldrug) FROM county_count"
#         result = parse_sql(sql)
#         self.assertEqual(result['aggregation'], 'MIN')
#         self.assertIn('count_alldrug', result['select_fields'])

#     def test_sum_function(self):
#         sql = "SELECT SUM(monthly_state_population) FROM state_population"
#         result = parse_sql(sql)
#         self.assertEqual(result['aggregation'], 'SUM')
#         self.assertIn('monthly_state_population', result['select_fields'])

# # More tests can be added for complex queries and different types of JOINs.

# if __name__ == '__main__':
#     unittest.main()
