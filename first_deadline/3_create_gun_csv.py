import csv
# Preprocess and write CSV files
with open('DATA\Gun.csv', 'w', newline='') as gun_csv:
    # Create a dictionary to map gun_id
    gun_writer = csv.writer(gun_csv)   
    gun_writer.writerow(['GunID', 'IsStolen', 'GunType'])

        
    with open('DATA\Police.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip the header row

        gun_id_dict = {}
        for row in csv_reader:
            gun_stolen = row[7]
            gun_type = row[8]

            # Convert 'gun_stolen' to a bit value (0 or 1)
            gun_stolen_bit = 1 if gun_stolen == 'Stolen' else 0
            # Create a unique key based on (gun_stolen_bit, gun_type) to represent the gun
            gun_key = (gun_stolen_bit, gun_type)

            # Check if the (gun_stolen_bit, gun_type) combination exists in the 'gun_id_dict'
            if gun_key in gun_id_dict:
                gun_id = gun_id_dict[gun_key]
            else:
                # If not, assign a new unique gun_id for this combination
                gun_id = len(set(gun_id_dict.values())) + 1
                gun_id_dict[gun_key] = gun_id
                gun_writer.writerow([gun_id, gun_stolen_bit, gun_type])

