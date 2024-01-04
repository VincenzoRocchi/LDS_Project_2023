import csv
import xml.etree.ElementTree as ET
from datetime import datetime

# Create a dictionary to map date_pk to date
date_dict = {}
tree = ET.parse('..\DATA\dates.xml')
root = tree.getroot()
for row in root.findall('row'):
    date_pk = int(row.find('date_pk').text)
    date_value = row.find('date').text
    date_dict[date_pk] = date_value

with open('..\DATA\Date.csv', 'w', newline='') as date_csv:
    date_writer = csv.writer(date_csv)
    date_fk_processed = []

    date_writer.writerow(['DateID', 'Date', 'Day', 'Month', 'Year', 'Quarter', 'DayOfWeek'])
    with open('..\DATA\Police.csv', 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader) #skip first row (header)

        for row in csv_reader:
            date_fk = int(row[10])
            date_value = date_dict.get(date_fk)
            if date_fk not in date_fk_processed:
                # Insert data into the Date table
                date_fk_processed.append(date_fk)

                # Convert date_value to a datetime object
                date_obj = datetime.strptime(date_value, '%Y-%m-%d %H:%M:%S')  # Adjust the format string to match your date format

                # Now you can access date attributes
                day = date_obj.day
                month = date_obj.month
                year = date_obj.year
                # Compute quarter
                quarter = (date_obj.month - 1) // 3 + 1

                # Compute day of week (0 = Monday, 6 = Sunday)
                dayofweek = date_obj.weekday()
                date_writer.writerow([date_fk, date_value, day, month, year, quarter, dayofweek])
      