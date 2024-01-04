if __name__ == '__main__':
    import csv
    import reverse_geocoder as rg
    import io

    # Define the input and output file paths
    input_file = r'..\DATA\uscities.csv'
    output_file = r'..\DATA\uscities_rg.csv'

    # Define the header mapping
    header_mapping = {
        "lat": "lat",
        "lon": "lng",
        "name": "city",
        "admin1": "state_name",
        "admin2": "county_name",
    }

    # Read the input CSV file and write to the output CSV file with the new format
    with open(input_file, 'r') as csv_input, open(output_file, 'w', newline='') as csv_output:
        reader = csv.DictReader(csv_input)
        fieldnames = list(header_mapping.keys()) + ['cc']
        
        # Write the new header
        csv_writer = csv.DictWriter(csv_output, fieldnames=fieldnames)
        csv_writer.writeheader()

        # Transform and write each row
        for row in reader:
            new_row = {new_key: row[old_key] for new_key, old_key in header_mapping.items()}
            new_row['cc'] = 'US'  # Set 'cc' to 'US' for the ISO 3166-1 alpha-2 country code for the USA
            csv_writer.writerow(new_row)
    
    print("CSV transformation complete.")

    geo = rg.RGeocoder(mode=2, verbose=True, stream=io.StringIO(open(r'..\DATA\uscities_rg.csv', encoding='utf-8').read()))

    # Create a dictionary to map unique city-state-country combinations to GeographyID
    geography_id_dict = {}

    # Open the CSV file for writing
    with open(r'..\DATA\Geography.csv', 'w', newline='') as geo_csv:
        # Remove the contents of the CSV file
        geo_csv.truncate(0)
        fieldnames = ['GeographyID', 'Latitude', 'Longitude', 'City', 'State', 'Continent']
        geo_writer = csv.DictWriter(geo_csv, fieldnames=fieldnames)
        geo_writer.writeheader()

        # Extract unique coordinates from the Police.csv
        unique_coordinates = set()
        with open(r'..\DATA\Police.csv', 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip the header row

            for row in csv_reader:
                latitude, longitude = row[5], row[6]
                unique_coordinates.add((latitude, longitude))

        # Perform a single search in reverse geocoder for all unique coordinates
        results = geo.query(list(unique_coordinates))

        geography_id = 1  # Initialize GeographyID

        # Write data to the CSV file
        for result in results:
            # Get the city, state, and continent information
            city, state, continent = result.get('name', ''), result.get('admin1', ''), result.get('cc', '')
            # Create a unique key for each city-state-country combination
            unique_key = (city, state, continent)

            # Check if this combination is already processed
            if unique_key not in geography_id_dict:
                # If not processed, add to the dictionary and write to CSV
                geography_id_dict[unique_key] = geography_id
                geo_writer.writerow({
                    'GeographyID': geography_id,
                    'Latitude': result['lat'],
                    'Longitude': result['lon'],
                    'City': city,
                    'State': state,
                    'Continent': continent
                })
                geography_id += 1

    print("CSV processing complete.")

