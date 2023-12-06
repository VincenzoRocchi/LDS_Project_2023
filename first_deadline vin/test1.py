
import csv
import pyodbc
import tqdm

# delete the contents of a table
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

def process_geography_batch(conn, batch):

    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Geography table
        conn.cursor().executemany('''
            INSERT INTO Geography (geo_id, latitude, longitude, city, state, continent)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', batch)

        conn.commit()
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()

def populate_geography_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):

    # Read data from CSV file
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)

        # Skip the header
        next(csv_reader)

        # Extract rows from the CSV file
        rows = [
            (row[0], row[1], row[2], row[3], row[4], row[5]) for row in csv_reader
        ]

    # Split rows into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    # Use ThreadPoolExecutor for parallelization
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Geography table') as pbar:
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_geography_batch, connection, batch) for batch in batches]

            # Wait for all futures to complete
            for future in futures:
                future.result()

                # Update the progress bar
                pbar.update(len(batch))

# Connection data
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_183'
password = 'TH023H6M'
database = 'Group_ID_183_DB'

# Connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

delete_table_contents(conn, 'Geography') # Delete the contents of the table before populating it
populate_geography_from_csv_batch('DATA/Geography.csv', conn)
