#useful functions to improve usability among other scripts
import csv

def find_column_number(file_path, target_column_name):
    # Open the CSV file and read the first row
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        first_row = next(reader)

    # Find the index (column number) of the target column
    column_number = next((i for i, col in enumerate(first_row) if target_column_name.lower() in col.lower()), None)

    return column_number


import csv

def find_column_numbers(file_path, target_column_names):
    # Open the CSV file and read the first row
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        first_row = next(reader)

    # Find the indices (column numbers) of the target columns
    column_numbers = [next((i for i, col in enumerate(first_row) if target.lower() in col.lower()), None) for target in target_column_names]

    return column_numbers
