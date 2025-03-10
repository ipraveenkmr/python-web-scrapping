import csv

def read_csv(file_path):
    """
    Reads a CSV file and returns its content as a list of dictionaries.
    
    :param file_path: Path to the CSV file
    :return: List of rows (dictionaries)
    """
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        return [row for row in reader]

def compare_csv(csv1_data, csv2_data):
    """
    Compares two CSV datasets and returns rows present in the first CSV but not in the second.

    :param csv1_data: List of rows (dictionaries) from the first CSV
    :param csv2_data: List of rows (dictionaries) from the second CSV
    :return: List of rows in csv1_data not found in csv2_data
    """
    # Convert rows to tuples for comparison
    csv2_set = {tuple(row.items()) for row in csv2_data}
    return [row for row in csv1_data if tuple(row.items()) not in csv2_set]

# Example usage
csv1_path = "file1.csv"
csv2_path = "file2.csv"

csv1_data = read_csv(csv1_path)
csv2_data = read_csv(csv2_path)

rows_not_in_csv2 = compare_csv(csv1_data, csv2_data)

if rows_not_in_csv2:
    print("Rows in first CSV but not in second CSV:")
    for row in rows_not_in_csv2:
        print(row)
else:
    print("All rows from the first CSV are present in the second CSV.")
