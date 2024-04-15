from dml import DMLManager

def execute_query(command):
    from dml import DMLManager
    dml_manager = DMLManager()

    if 'type' in command:  # Check if it's a DML query
        if command['type'] == 'insert':
            return dml_manager.insert(command['table'], command['data'])
        elif command['type'] == 'delete':
            return dml_manager.delete(command['table'], command['conditions'])
        elif command['type'] == 'select':
            # Handling aggregation functions specifically
            if any(func in command['columns'][0] for func in ['MAX', 'MIN', 'SUM']):
                return handle_aggregations(command, dml_manager)
            else:
                return dml_manager.select(command['table'], command['columns'], command['conditions'])
    else:  # It's an aggregation query
        if command['function'] == 'MAX':
            return aggregate_max(command, dml_manager)
        elif command['function'] == 'MIN':
            return aggregate_min(command, dml_manager)
        elif command['function'] == 'SUM':
            return aggregate_sum(command, dml_manager)
    
    return None

def handle_aggregations(command, dml_manager):
    # Extract the aggregation function and column from the query
    import re
    match = re.match(r"(\w+)\((\w+)\)", command['columns'][0])
    if not match:
        return "Invalid aggregation function syntax"

    func, column = match.groups()
    data = dml_manager.select(command['table'], [column], command['conditions'])
    if func.upper() == 'MAX':
        return max(row[column] for row in data)
    elif func.upper() == 'MIN':
        return min(row[column] for row in data)
    elif func.upper() == 'SUM':
        return sum(row[column] for row in data)

def aggregate_max(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    return max(row[command['column']] for row in data)

def aggregate_min(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    return min(row[command['column']] for row in data)

def aggregate_sum(command, dml_manager):
    data = dml_manager.select(command['table'], [command['column']], command.get('conditions'))
    total = 0
    for row in data:
        try:
            # Convert to int before summing; use float if decimals are possible.
            total += int(row[command['column']])
        except ValueError:
            # Log or handle the case where conversion is not possible
            print(f"Warning: Non-numeric data encountered in {command['column']} and will be ignored: {row[command['column']]}")
    return total


if __name__ == "__main__":
    print(execute_query({"function": "MAX", "table": "state_population", "column": "monthly_state_population"}))
    print(execute_query({"function": "MIN", "table": "county_count", "column": "count_alldrug"}))
    print(execute_query({"function": "SUM", "table": "population", "column": "county_count"}))
