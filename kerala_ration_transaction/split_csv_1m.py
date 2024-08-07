import csv
import os

input_file = r'D:\BGA\kerala_transactions\kerala_tra\output\3\kerala_transactions_2024_2.csv'
output_directory = 'output/'
records_per_file = 1000000
file_counter = 0
current_records = 0
output_file = None
os.makedirs(output_directory, exist_ok=True)
with open(input_file, 'r', newline='') as csvfile:
    reader = csv.reader(csvfile)
    header = next(reader)  
    for row in reader:
        if current_records % records_per_file == 0:
            if output_file:
                output_file.close()
            file_counter += 1
            output_file = open(f'{output_directory}kerala_transactions_2_2024_{file_counter}.csv', 'w', newline='')
            writer = csv.writer(output_file)
            writer.writerow(header)
        writer.writerow(row)
        current_records += 1
if output_file:
    output_file.close()
print("Splitting complete.")
