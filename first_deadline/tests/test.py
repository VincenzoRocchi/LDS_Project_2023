import xml.etree.ElementTree as ET
import csv
import json
import pyodbc
import time
# from geopy.exc import GeocoderTimedOut
# from geopy.geocoders import Nominatim
from datetime import datetime
import reverse_geocoder as rg

# Function to insert data into the database
def insert_data_into_db(conn, cursor, table_name, data):
    placeholders = ', '.join(['?'] * len(data))
    query = f'INSERT INTO {table_name} VAexit()LUES ({placeholders})'
    cursor.execute(query, data)
    conn.commit()

# Function to compute additional date-related data
def compute_date_data(date_str):
    date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
    date = date_obj.date()
    day = date_obj.day
    month = date_obj.month
    year = date_obj.year
    quarter = (date_obj.month - 1) // 3 + 1
    day_of_week = date_obj.strftime('%A')
    return date, day, month, year, quarter, day_of_week

if __name__ == '__main__':
    ### DB connection & table checks  

    # # Connection string
    # server = 'tcp:lds.di.unipi.it'
    # username = 'Group_ID_183'
    # password = 'TH023H6M'
    # database = 'Group_ID_183_DB'
    # connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password

    # # Connect to the SQL Server database
    # conn = pyodbc.connect(connectionString)
    # cursor = conn.cursor()

    # List of table names in your database
    # table_names = ['Custody', 'Geography', 'Gun', 'Date', 'Incident', 'Participant']

    # # Clean the tables by deleting all records
    # for table_name in table_names:
    #     cursor.execute(f'DELETE FROM {table_name}')
    #     conn.commit()

    ### DB connection & table checks

    # Load the participant age JSON data
    with open("DATA\\dict_partecipant_age.json", 'r') as age_file:
        age_data = json.load(age_file)

    # Load the participant status JSON data
    with open('DATA\\dict_partecipant_status.json', 'r') as status_file:
        status_data = json.load(status_file)

    # Load the participant type JSON data
    with open('DATA\\dict_partecipant_type.json', 'r') as type_file:
        type_data = json.load(type_file)

    # Create dictionaries to map CSV values to IDs
    geography_id_dict = {}
    gun_id_dict = {}
    participant_id_dict = {}
    date_fk_processed=[]
    incident_id_processed=[]

    # Create a dictionary to map date_pk to date
    date_dict = {}

    # Initialize ID's
    geo_id = 1
    gun_id = 1
    participant_id = 1

    # # Initialize a geocoder for reverse geocoding
    # geolocator = Nominatim(user_agent="reverse_geocode")

    # extract 
    tree = ET.parse('DATA/dates.xml')
    root = tree.getroot()
    for row in root.findall('row'):
        date_pk = int(row.find('date_pk').text)
        date_value = row.find('date').text
        date_dict[date_pk] = date_value

    with open('DATA/Police.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        
        output_csv_geo = open('DATA/geography_table.csv', 'w', newline='')
        csv_writer_geo = csv.writer(output_csv_geo)
        header_geo = ['Geo ID', 'City', 'State', 'Continent']
        csv_writer_geo.writerow(header_geo)
        
        output_csv_gun = open('DATA/gun_table.csv', 'w', newline='')
        csv_writer_gun = csv.writer(output_csv_gun)
        header_gun = ["gun_id", "gun_stolen_bit", "gun_type"]
        csv_writer_gun.writerow(header_gun)
        
        output_csv_participant = open('DATA/participant_table.csv', 'w', newline='')
        csv_writer_participant = csv.writer(output_csv_participant)
        header_participant = ["participant_id", "participant_age_group", "participant_gender", "participant_status"]
        csv_writer_participant.writerow(header_participant)
        
        output_csv_date = open('DATA/date_table.csv', 'w', newline='')
        csv_writer_date = csv.writer(output_csv_date)
        header_date = ["date_id", "date", "day", "month", "year", "quarter", "day_of_week"]
        csv_writer_date.writerow(header_date)
        
        output_csv_incident = open('DATA/incident_table.csv', 'w', newline='')
        csv_writer_incident = csv.writer(output_csv_incident)
        header_incident = ["incident_id"]
        csv_writer_incident.writerow(header_incident)
        
        output_csv_custody = open('DATA/custody_table.csv', 'w', newline='')
        csv_writer_custody = csv.writer(output_csv_custody)
        header_custody = ["custody_id", "participant_id", "gun_ipyd", "geo_id", "date_id", "crime_gravity"]
        csv_writer_custody.writerow(header_custody) 
        
        row_count = 0 
        for row in csv_reader:
            custody_id, participant_age_group, participant_gender, participant_status, participant_type, latitude, longitude, gun_stolen, gun_type, incident_id, date_fk = row
            
            # Reverse geocode to get city, state, and continent
            location = rg.search((latitude, longitude),)
            
            # Extract the required information from the location object
            for loc in location:
                city = loc.get('name', '')
                state = loc.get('admin1', '')
                continent = loc.get('cc', '')
                
            # Check if the (city, state, continent) tuple exists in the 'geography_id_dict'
            geo_key = (city, state, continent)
            if geo_key in geography_id_dict:
                geo_id = geography_id_dict[geo_key]
            else:
                # If not, assign a new unique geo_id
                geo_id = len(geography_id_dict) + 1
                geography_id_dict[geo_key] = geo_id
                # Write the row to the output CSV file    
                csv_writer_geo.writerow([geo_id, latitude, longitude, city, state, continent])
            
            # Convert 'gun_stolen' to a bit value (0 or 1)
            gun_stolen_bit = 1 if gun_stolen == 'Stolen' else 0

            # Create a unique key based on (gun_stolen_bit, gun_type) to represent the gun
            gun_key = (gun_stolen_bit, gun_type)

            # Check if the (gun_stolen_bit, gun_type) combination exists in the 'gun_id_dict'
            if gun_key in gun_id_dict:
                gun_id = gun_id_dict[gun_key]
            else:
                # If not, assign a new unique gun_id for this combination
                gun_id = len(gun_id_dict) + 1
                # Write the row to the output CSV file
                csv_writer_gun.writerow([gun_id, gun_stolen_bit, gun_type])
                gun_id_dict[gun_key] = gun_id
            
            # Check if the (participant_age_group, participant_gender, participant_status) tuple exists in the 'participant_id_dict'
            participant_key = (age_data[participant_age_group], participant_gender, status_data[participant_status])
            if participant_key in participant_id_dict:
                participant_id = participant_id_dict[participant_key]
            else:
                # If not, assign a new unique participant_id
                participant_id = len(participant_id_dict) + 1
                participant_id_dict[participant_key] = participant_id
                # Insert data into the Participant table
                csv_writer_participant.writerow([participant_id, age_data[participant_age_group], participant_gender, status_data[participant_status]])

            # Map date_fk directly to date_id
            date_id = int(date_fk)

            # Compute date-related data
            date_value = date_dict.get(date_id)
            date, day, month, year, quarter, day_of_week = compute_date_data(date_value)
            
            if date_fk not in date_fk_processed:
                # Insert data into the Date table
                csv_writer_date.writerow([date_id, date, day, month, year, quarter, day_of_week])
                date_fk_processed.append(date_fk)
            
            if incident_id not in incident_id_processed:
                # Insert data into the Incident table
                csv_writer_incident.writerow([incident_id])
                incident_id_processed.append(incident_id)
            
            # Insert data into the Custody table
            csv_writer_date.writerow([custody_id, participant_id, gun_id, geo_id, date_id, 0.0]) # Initialize crime_gravity to 0.0
    
            print(row_count)
            row_count += 1
            if row_count % 1000 == 0:
                print(f"Processed {row_count} rows")

    # Close the output CSV file
    output_csv_geo.close()
    output_csv_gun.close()
    output_csv_participant.close()
    output_csv_date.close()
    output_csv_incident()
    output_csv_custody.close()
