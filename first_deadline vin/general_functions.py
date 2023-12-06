#useful functions to improve usability among other scripts
import csv

if __name__ == '__main__':
    
    def find_column_number(file_path, target_column_name):
        # Open the CSV file and read the first row
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            first_row = next(reader)

        # Find the index (column number) of the target column
        column_number = next((i for i, col in enumerate(first_row) if target_column_name.lower() in col.lower()), None)

        return column_number


    def find_column_numbers(file_path, target_column_names):
        # Open the CSV file and read the first row
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            first_row = next(reader)

        # Find the indices (column numbers) of the target columns
        column_numbers = [next((i for i, col in enumerate(first_row) if target.lower() in col.lower()), None) for target in target_column_names]

        return column_numbers


    def calculate_crime_gravity(participant_age, participant_type, participant_status, age_data, type_data, status_data):
        
        F1 = age_data.get(participant_age, 1.0)  # Default to 1.0 if not found
        F2 = type_data.get(participant_type, 1.0)
        F3 = status_data.get(participant_status, 1.0)

        # Calculate crime gravity using the provided formula
        crime_gravity = F1 * F2 * F3
        return crime_gravity
    
    def delete_table_contents(connection, table_name):
        try:
            # Create a cursor from the connection
            cursor = connection.cursor()

            # Execute the DELETE statement
            delete_query = f'DELETE FROM {table_name}'
            cursor.execute(delete_query)

            # Commit the transaction
            connection.commit()

            print(f'Contents of table {table_name} deleted successfully.')

        except pyodbc.Error as e:
            print(f'Error: {e}')

        finally:
            # Close the cursor (connection will be closed outside the function)
            cursor.close()