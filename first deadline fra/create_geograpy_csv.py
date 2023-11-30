if __name__ == '__main__':
    import csv
    import reverse_geocoder as rg
    # Create a dictionary to map geography_id
    geography_id_dict = {}

    # Open the CSV file for writing
    with open('Geography.csv', 'w', newline='') as geo_csv:
        # Remove the contents of the CSV file
        geo_csv.truncate(0)
        fieldnames = ['GeographyID', 'Latitude', 'Longitude', 'City', 'State', 'Continent']
        geo_writer = csv.DictWriter(geo_csv, fieldnames=fieldnames)
        geo_writer.writeheader()

        # Extract unique coordinates from the Police.csv
        unique_coordinates = set()
        with open('Police.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row

            for row in csv_reader:
                latitude, longitude = row[5], row[6]
                unique_coordinates.add((latitude, longitude))

        # Perform a single search in reverse geocoder for all unique coordinates
        results = rg.search(list(unique_coordinates))

        # Write data to the CSV file
        for i, row in enumerate(results):
            unique_coordinates = list(unique_coordinates)
            latitude, longitude = unique_coordinates[i]
            city, state, continent = row.get('name', ''), row.get('admin1', ''), row.get('cc', '')
            geography_id = i+1  # Generate a unique geography_id
            geo_writer.writerow({'GeographyID': geography_id, 'Latitude': latitude, 'Longitude': longitude, 'City': city, 'State': state, 'Continent': continent})
