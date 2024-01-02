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

# Populate the geography table
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

# Populate the gun table
def process_gun_batch(conn, batch):

    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Gun table
        conn.cursor().executemany('''
            INSERT INTO Gun (gun_id, is_stolen, gun_type)
            VALUES (?, ?, ?)
        ''', batch)

        conn.commit()
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
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_gun_batch, connection, batch) for batch in batches]

            # Wait for all futures to complete
            for future in futures:
                future.result()

                # Update the progress bar
                pbar.update(len(batch))

# Populate the date table
def process_date_batch(conn, batch):
    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Date table
        conn.cursor().executemany('''
            INSERT INTO Date (date_id, date, day, month, year, quarter, day_of_week)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', batch)

        conn.commit()
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()

def populate_date_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):

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
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Date table') as pbar:
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_date_batch, connection, batch) for batch in batches]

            # Wait for all futures to complete
            for future in futures:
                future.result()

                # Update the progress bar
                pbar.update(len(batch))

# Populate the incident table
def process_incident_batch(conn, batch):

    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Incident table
        conn.cursor().executemany('''
            INSERT INTO Incident (incident_id)
            VALUES (?)
        ''', batch)

        conn.commit()
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()

def populate_incident_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):

    # Read data from CSV file
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)

        # Skip the header
        next(csv_reader)

        # Extract rows from the CSV file
        rows = [
            (row[0],) for row in csv_reader  # Assuming the first column is the incident_id
            # Add other columns as needed
        ]

    # Split rows into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    # Use ThreadPoolExecutor for parallelization
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Incident table') as pbar:
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_incident_batch, connection, batch) for batch in batches]

            # Wait for all futures to complete
            for future in futures:
                future.result()

                # Update the progress bar
                pbar.update(len(batch))

# Populate the participant table
def process_participant_batch(conn, batch):

    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Participant table
        conn.cursor().executemany('''
            INSERT INTO Participant (participant_id, age_group, gender, status, type)
            VALUES (?, ?, ?, ?, ?)
        ''', batch)

        conn.commit()
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()

def populate_participant_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):

    # Read data from CSV file
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)

        # Skip the header
        next(csv_reader)

        # Extract rows from the CSV file
        rows = [
            (row[0], row[1], row[2], row[3], row[4]) for row in csv_reader  # Assuming the columns are in the correct order
            # Add other columns as needed
        ]

    # Split rows into batches
    batches = [rows[i:i + batch_size] for i in range(0, len(rows), batch_size)]

    # Use ThreadPoolExecutor for parallelization
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Participant table') as pbar:
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_participant_batch, connection, batch) for batch in batches]

            # Wait for all futures to complete
            for future in futures:
                future.result()

                # Update the progress bar
                pbar.update(len(batch))

# populate the custody table
def process_custody_batch(conn, batch):

    try:
        conn.execute('BEGIN TRANSACTION')

        # Insert the batch into the Custody table
        conn.cursor().executemany('''
            INSERT INTO Custody (custody_id, participant_id, gun_id, geo_id, date_id, crime_gravity)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', batch)

        conn.commit()
    except Exception as e:
        print(f"An error occurred during batch insertion: {e}")
        conn.rollback()

def populate_custody_from_csv_batch(file_path, connection, batch_size=100, num_workers=2):
 
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
    with ThreadPoolExecutor(max_workers=num_workers) as executor, tqdm(total=len(rows), desc='Inserting into Custody table') as pbar:
        for batch in batches:
            # Process batches in parallel
            futures = [executor.submit(process_custody_batch, connection, batch) for batch in batches]

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

delete_table_contents(conn, 'Gun')  # Delete the contents of the table before populating it
populate_gun_from_csv_batch('DATA/Gun.csv', conn)

delete_table_contents(conn, 'Date')  # Delete the contents of the table before populating it
populate_date_from_csv_batch('DATA/Date.csv', conn)

delete_table_contents(conn, 'Incident')  # Delete the contents of the table before populating it
populate_incident_from_csv_batch('DATA/Incident.csv', conn )

delete_table_contents(conn, 'Participant')  # Delete the contents of the table before populating it
populate_participant_from_csv_batch('DATA/Partecipant.csv', conn)

delete_table_contents(conn, 'Custody')  # Delete the contents of the table before populating it
populate_custody_from_csv_batch('DATA/Custody.csv', conn)

cursor.close()
conn.close()