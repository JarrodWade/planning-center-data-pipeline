from airflow.decorators import task
import logging
import json
import csv
import io

# Task to create CSV files from the people list in-memory
@task
def make_csv(people_list: dict):
    field_names = ['name', 'primary_email', 'primary_phone_number', 'grade', 'age']

    with open('/opt/airflow/CSVs/CSV_fmt.json', 'r') as json_file:
        csv_fmt = json.load(json_file)

    csv_data = {}

    for key, value in people_list.items():
        if key in csv_fmt:
            csv_name = csv_fmt[key]
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=field_names, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(value)
            csv_data[csv_name] = csv_buffer.getvalue()
            
            logging.info(f"Completed writing {csv_name} to in-memory CSV")

    return csv_data