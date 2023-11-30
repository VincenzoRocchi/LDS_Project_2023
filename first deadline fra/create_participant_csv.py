import csv
import json

# Load JSON data
with open('dict_partecipant_age.json', 'r') as age_file:
    age_data = json.load(age_file)

with open('dict_partecipant_status.json', 'r') as status_file:
    status_data = json.load(status_file)

with open('dict_partecipant_type.json', 'r') as type_file:
    type_data = json.load(type_file)

# Open the CSV file for writing
with open('Participant.csv', 'w', newline='') as participant_csv:
    # Remove the contents of the CSV file
    participant_csv.truncate(0)
    fieldnames = ['ParticipantID', 'AgeGroup', 'Gender', 'Status', 'Type']  # Add 'Type' to fieldnames
    participant_writer = csv.DictWriter(participant_csv, fieldnames=fieldnames)
    participant_writer.writeheader()
    with open('Police.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        participant_id_dict = {}
        for row in csv_reader:
            participant_age_group = row[1]
            participant_gender = row[2]
            participant_status = row[3]
            participant_type = row[4]

            # Get the participant type based on age and status
            participant_type = type_data.get(participant_type, participant_type)
            participant_age_group = age_data.get(participant_age_group, participant_age_group)
            participant_status = status_data.get(participant_status, participant_status)
            # Create a unique participant key
            participant_key = (participant_age_group, participant_gender, participant_status, participant_type)
            if participant_key in participant_id_dict:
                participant_id = participant_id_dict[participant_key]
            else:
                # If not, assign a new unique participant_id
                participant_id = len(participant_id_dict) + 1
                participant_id_dict[participant_key] = participant_id
                participant_writer.writerow({'ParticipantID': participant_id, 'AgeGroup': participant_age_group, 'Gender': participant_gender, 'Status': participant_status, 'Type': participant_type})  # Add 'Type' to the row
            
