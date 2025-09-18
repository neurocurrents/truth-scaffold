import csv

input_file = 'data/records.csv'
output_file = 'data/records.csv'

with open(input_file, 'r', newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    for row in reader:
        new_row = {}
        for key, value in row.items():
            if value == 'True':
                new_row[key] = '1'
            elif value == 'False':
                new_row[key] = '0'
            else:
                new_row[key] = value
        writer.writerow(new_row)
print(f"Converted CSV written to {output_file}")
