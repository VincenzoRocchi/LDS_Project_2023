import csv
import pyodbc
import os

if __name__ == '__main__':

        # Function to insert data into the database in batches
        def insert_data_into_db_batched(conn, cursor, table_name, data_batch):
            placeholders = ', '.join(['?'] * len(data_batch[0]))
            query = f'INSERT INTO {table_name} VALUES ({placeholders})'
            cursor.executemany(query, data_batch)
            conn.commit()

        # Connection string
        server = 'tcp:lds.di.unipi.it'
        username = 'Group_ID_183'
        password = 'TH023H6M'
        database = 'Group_ID_183_DB'
        connectionString = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

        # Connect to the SQL Server database
        conn = pyodbc.connect(connectionString)
        cursor = conn.cursor()

        # List of table names in your database
        table_names = ['Geography', 'Gun', 'Participant', 'Date', 'Incident', 'Custody']

        # Clean the tables by deleting all records
        for table_name in table_names:
            cursor.execute(f'DELETE FROM {table_name}')
            conn.commit()

        # Preprocess and write CSV files
        csv_folder = 'csv_folder'  # Replace with the path to your CSV folder
        for table_name in table_names:
            csv_file_path = os.path.join(csv_folder, f'{table_name}.csv')
            with open(csv_file_path, 'r') as csv_file:
                csv_reader = csv.reader(csv_file)
                next(csv_reader)  # Skip the header row

                # Initialize data batches
                data_batch = []
                for row in csv_reader:
                    data_batch.append(row)
                    if len(data_batch) == 1000:
                        # Insert data into the database in batches
                        insert_data_into_db_batched(conn, cursor, table_name, data_batch)
                        data_batch = []
                # Insert the remaining data
                if len(data_batch) > 0:
                    insert_data_into_db_batched(conn, cursor, table_name, data_batch)

        # Close the database connection
        conn.close()
