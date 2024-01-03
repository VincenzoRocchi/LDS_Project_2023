import csv
import json
import math
from tqdm import tqdm

# Function to calculate crime gravity
def calculate_crime_gravity(participant_age, participant_type, participant_status):
    F1 = participant_age
    F2 = participant_type
    F3 = participant_status
    crime_gravity = F1 * F2 * F3
    return crime_gravity

# Function to calculate distance between two points using the Haversine formula
def calculate_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of the Earth in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = (math.sin(dLat / 2) * math.sin(dLat / 2) +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dLon / 2) * math.sin(dLon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

# Read geography data
geography_data = []
with open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Geography.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        geography_data.append(row)

# Read other data files
with open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\dict_partecipant_age.json', 'r') as age_file, \
     open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\dict_partecipant_status.json', 'r') as status_file, \
     open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\dict_partecipant_type.json', 'r') as type_file, \
     open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Gun.csv', 'r') as gun_csv, \
     open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Participant.csv', 'r') as participant_csv, \
     open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Date.csv', 'r') as date_csv:

    age_data = json.load(age_file)
    status_data = json.load(status_file)
    type_data = json.load(type_file)

    gun_data = { (int(row['IsStolen']), row['GunType']): row['GunID'] for row in csv.DictReader(gun_csv) }
    participant_data = { (int(row['AgeGroup']), row['Gender'], int(row['Status']), int(row['Type'])): row['ParticipantID'] for row in csv.DictReader(participant_csv) }
    date_data = { row['DateID']: row['DateID'] for row in csv.DictReader(date_csv) }

    custody_id_set = set()

    with open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Police.csv', 'r') as csv_file, \
         open(r'C:\Users\Vincenzo\Projects\LDS_Project_23-24\DATA\Custody.csv', 'w', newline='') as custody_csv:

        csv_reader = csv.reader(csv_file)
        custody_writer = csv.writer(custody_csv)
        custody_writer.writerow(['CustodyID', 'ParticipantID', 'GunID', 'GeographyID', 'DateID', 'CrimeGravity', 'IncidentID'])
        next(csv_reader)  # Skip the header row in the input file
        
        for row in tqdm(csv_reader):
            custody_id, participant_age_group, participant_gender, participant_status, participant_type, latitude, longitude, gun_stolen, gun_type, incident_id, date_fk = row

            # Find the closest geography entry for each police record
            lat, lon = float(latitude), float(longitude)
            min_distance = float('inf')
            closest_geo_id = None
            for geo_row in geography_data:
                geo_id, geo_lat, geo_lon = geo_row[0], float(geo_row[1]), float(geo_row[2])
                distance = calculate_distance(lat, lon, geo_lat, geo_lon)
                if distance < min_distance:
                    min_distance = distance
                    closest_geo_id = geo_id

            participant_type = type_data.get(participant_type, participant_type)
            participant_age_group = age_data.get(participant_age_group, participant_age_group)
            participant_status = status_data.get(participant_status, participant_status)
            participant_key = (participant_age_group, participant_gender, participant_status, participant_type)
            participant_id = participant_data.get(participant_key)

            gun_stolen_bit = 1 if gun_stolen == 'Stolen' else 0
            gun_key = (gun_stolen_bit, gun_type)
            gun_id = gun_data.get(gun_key)

            date_id = date_data.get(date_fk)

            crime_gravity = calculate_crime_gravity(participant_age_group, participant_type, participant_status)

            if participant_id is not None and custody_id not in custody_id_set:
                custody_writer.writerow([custody_id, participant_id, gun_id, closest_geo_id, date_id, crime_gravity, incident_id])
                custody_id_set.add(custody_id)
            elif participant_id is None:
                print("Null participant ID: " + custody_id)
            else:
                print("Duplicate custody ID: " + custody_id)
