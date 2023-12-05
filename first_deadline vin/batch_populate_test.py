import csv
import pyodbc
import tqdm

def populate_gun_from_csv_batch(file_path, batch_size=100):
    with open(file_path, 'r') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        next(csv_reader)
        rows = [(
            row[0],
            row[1],
            row[2]
        ) for row in csv_reader]

    # Insert rows in batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        cursor.executemany('''
            INSERT INTO Gun (gun_id, is_stolen, gun_type)
            VALUES (?, ?, ?)
        ''', batch)

    conn.commit()
    

# Connection data
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_183'
password = 'TH023H6M'
database = 'Group_ID_183_DB'

#connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

delete_query = f'TRUNCATE TABLE {"Gun"};'
# Execute the query
cursor.execute(delete_query)

populate_gun_from_csv_batch('DATA\Gun.csv')