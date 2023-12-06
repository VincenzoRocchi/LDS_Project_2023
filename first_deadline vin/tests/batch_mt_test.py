import csv
import pyodbc
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

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

def process_gun_batch(batch):
    try:
        # Create a new connection for each thread
        conn = pyodbc.connect(conn_str)
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Gun table
        conn.cursor().executemany('''
            INSERT INTO Gun (gun_id, is_stolen, gun_type)
            VALUES (?, ?, ?)
        ''', batch)

        conn.commit()
        conn.close()  # Close the connection after successful commit
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()
        
def populate_gun_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):

    # Read data from CSV file
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)

        # Skip the header
        next(csv_reader)

        # Extract rows from the CSV file
        rows = [
            (row[0], row[1], row[2]) for row in csv_reader
        ]

    # Split rows into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    # Use ThreadPoolExecutor for parallelization
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Gun table') as pbar:
        futures = [executor.submit(process_gun_batch, batch) for batch in batches]

        # Wait for all futures to complete
        for future in futures:
            future.result()
            pbar.update(len(batch))

# Connection data
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_183'
password = 'TH023H6M'
database = 'Group_ID_183_DB'

# Connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(conn_str)

delete_table_contents(conn, 'Geography') # Delete the contents of the table before populating it
populate_gun_from_csv_batch('DATA/Geography.csv', conn)
