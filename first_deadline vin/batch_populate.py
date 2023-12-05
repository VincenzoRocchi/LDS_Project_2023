from populating_functions import (
    populate_custody_from_csv_batch,
    populate_geography_from_csv_batch,
    populate_gun_from_csv_batch,
    populate_date_from_csv_batch,
    populate_incident_from_csv_batch,
    populate_participant_from_csv_batch
)

from populating_functions import populate_custody_from_csv_batch

# Connection data
server = 'tcp:lds.di.unipi.it'
username = 'Group_ID_183'
password = 'TH023H6M'
database = 'Group_ID_183_DB'

# Connection string
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

populate_custody_from_csv_batch('DATA/Custody.csv')

populate_geography_from_csv_batch('DATA/Geography.csv')

populate_gun_from_csv_batch('DATA/Gun.csv')

populate_date_from_csv_batch('DATA/Date.csv')

populate_incident_from_csv_batch('DATA/Incident.csv',)

populate_participant_from_csv_batch('DATA/Partecipant.csv')


cursor.close()
conn.close()