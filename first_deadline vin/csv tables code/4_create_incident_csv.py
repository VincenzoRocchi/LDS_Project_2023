import csv

with open('DATA\Incident.csv', 'w', newline='') as incident_csv:
    incident_writer = csv.writer(incident_csv)
    # Write the header row
    incident_writer.writerow(['IncidentID'])
    
    with open('DATA\Police.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row
        
        incident_id_processed = set()
        
        for row in csv_reader:
            incident_id = row[9]
            if incident_id not in incident_id_processed:
                # Insert data into the Incident table
                incident_id_processed.add(incident_id)
                incident_writer.writerow([incident_id])
            