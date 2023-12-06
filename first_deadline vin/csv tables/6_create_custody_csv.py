import csv
import json
from tqdm import tqdm

# Function to calculate crime gravity based on participant age, type, and status
def calculate_crime_gravity(participant_age, participant_type, participant_status):
    F1 = participant_age  # Default to 1.0 if not found
    F2 = participant_type
    F3 = participant_status

    # Calculate crime gravity using the provided formula
    crime_gravity = F1 * F2 * F3
    return crime_gravity

with open('DATA\dict_partecipant_age.json', 'r') as age_file, \
     open('DATA\dict_partecipant_status.json', 'r') as status_file, \
     open('DATA\dict_partecipant_type.json', 'r') as type_file, \
     open('DATA\Geography.csv', 'r') as geo_csv, \
     open('DATA\Gun.csv', 'r') as gun_csv, \
     open('DATA\Participant.csv', 'r') as participant_csv, \
     open('DATA\Date.csv', 'r') as date_csv, \
     open('DATA\Incident.csv', 'r') as incident_csv:

    age_data = json.load(age_file)
    status_data = json.load(status_file)
    type_data = json.load(type_file)

    # creating dictonaries for appending keys in the custody.csv
    geo_data = { (row['Latitude'], row['Longitude']): row['GeographyID'] for row in csv.DictReader(geo_csv) }
    gun_data = { (int(row['IsStolen']), row['GunType']): row['GunID'] for row in csv.DictReader(gun_csv) }
    participant_data = { (int(row['AgeGroup']), row['Gender'], int(row['Status']),int(row['Type'])): row['ParticipantID'] for row in csv.DictReader(participant_csv) }
    date_data = { row['DateID']: row['DateID'] for row in csv.DictReader(date_csv) }

    custody_id_set = set()
    # Load existing participant IDs from Participant.csv

    with open('DATA\Police.csv', 'r') as csv_file, \
        open('DATA\Custody.csv', 'w', newline='') as custody_csv:

        csv_reader = csv.reader(csv_file)
        custody_writer = csv.writer(custody_csv)
        # Write the header row
        custody_writer.writerow(['CustodyID', 'ParticipantID', 'GunID', 'GeographyID', 'DateID', 'CrimeGravity'])
        next(csv_reader)  # Skip the header row in the input file
        
        for row in tqdm(csv_reader):
            custody_id, participant_age_group, participant_gender, participant_status, participant_type, latitude, longitude, gun_stolen, gun_type, incident_id, date_fk = row
            if custody_id not in custody_id_set:
                
                # Find the participant ID
                participant_type = type_data.get(participant_type, participant_type)
                participant_age_group = age_data.get(participant_age_group, participant_age_group)
                participant_status = status_data.get(participant_status, participant_status)
                participant_key = (participant_age_group, participant_gender, participant_status, participant_type)
                participant_id = participant_data.get(participant_key)

                # Find the gun ID
                gun_stolen_bit = 1 if gun_stolen == 'Stolen' else 0
                gun_key = (gun_stolen_bit, gun_type)
                gun_id = gun_data.get(gun_key)

                # Find the geography ID
                geo_key = (latitude, longitude)
                geo_id = geo_data.get(geo_key)

                # Find the date ID
                date_id = date_data.get(date_fk)

                # Calculate crime gravity
                crime_gravity = calculate_crime_gravity(participant_age_group, participant_type, participant_status)

                if participant_id is not None:
                    custody_id_set.add(custody_id)
                    # Write the row to the custody CSV file
                    custody_writer.writerow([custody_id, participant_id, gun_id, geo_id, date_id, crime_gravity])
                else:
                    print("Null partecipant ID: " + custody_id)
            else:
                print("Duplicate custody ID: " + custody_id)
