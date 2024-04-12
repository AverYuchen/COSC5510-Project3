import chardet

def detect_encoding(file_path):
    try:
        with open(file_path, 'rb') as file:
            raw_data = file.read(10000)  # Read first 10000 bytes to guess encoding
            if not raw_data:
                return "No data read from file."
            result = chardet.detect(raw_data)
            return result
    except Exception as e:
        return f"An error occurred: {str(e)}"

# Example usage
file_path = './data/state_population.csv'
encoding_info = detect_encoding(file_path)
print("Encoding info:", encoding_info)
