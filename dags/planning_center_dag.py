# Imports
import pypco
import csv
import sys
import gspread
import glob
import logging
import json
from datetime import datetime, date, timedelta
import boto3
import airflow
import pendulum
from airflow.decorators import dag, task
import os
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials
import io  # Import io for in-memory file handling

# Import class Person to assist in tracking Youths and utilize class data structures
from classes.Person import Person

# Script with functions to scrape PlanningCenter.com and return list counts as displayed on the website (for validation)
import webscraper as wsc

# Centralized logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)

# Define the DAG with a weekly schedule, starting on January 1, 2024
@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="MST"),
    catchup=False,
    tags=["JJW"],
)
def pco_taskflow():

    # Task to log the start of the DAG run
    @task
    def log_start():
        logging.info("\n"
                     "__________________________________________________________________________________________________\n"
                     f"Planning Center data pull is starting new run on {datetime.now()} ....\n"
                     "__________________________________________________________________________________________________\n")

    # Task to scrape and validate data from Planning Center
    @task
    def scrape_validate():
        html = wsc.get_html(user_id=wsc.get_credentials()[0], password=wsc.get_credentials()[1],
                            url='https://people.planningcenteronline.com/lists', secret=wsc.get_secret())
        validation_set = wsc.scrape(html)
        logging.info("\n"
                     "__________________________________________________________________________________________________\n"
                     f"Planning Center validation web scrape completed on {datetime.now()} ....\n"
                     "__________________________________________________________________________________________________\n")
        return validation_set

    # Task to pull data from Planning Center API
    @task
    def pull_data(validation_set: dict):
        pco = pypco.PCO(os.getenv('PCO_ID'), os.getenv('PCO_SECRET'))

        people_list = {}

        # Iterate through lists in Planning Center
        for pc_list in pco.iterate('/people/v2/lists'):
            # For our purposes, we are focusing on the Youth Program
            # Please adapt for your target Planning Center Lists
            if "Youth" in pc_list["data"]["attributes"]["name"]:
                list_name = pc_list["data"]["attributes"]["name"]
                list_id = pc_list["data"]["id"]
                list_path = pc_list["data"]["links"]["self"]

                logging.info(f"CURRENT LIST: {list_name}")
                logging.info(f"     It has unique list ID {list_id}. API path is: {list_path}")

                list_link = list_path + "/list_results"
                people_list[list_name] = []

                # Iterate through people in the list
                for result in pco.iterate(list_link):
                    person_id = result["data"]["relationships"]["person"]["data"]["id"]
                    person = pco.get(f'/people/v2/people/{person_id}')
                    person_attributes = person['data']['attributes']

                    # Create a Person object for each person in the list
                    youth = Person(
                        person_id=person_id,
                        person_list=list_name,
                        name=person_attributes['name'],
                        primary_email=get_primary_email(person_id, pco),
                        primary_phone_number=get_primary_phone(person_id, pco),
                        grade=stringify_grade(person_attributes['grade']),
                        age=calc_age(person_attributes['birthdate'])
                    )

                    people_list[list_name].append(youth.__dict__)

                list_len = len(people_list[list_name])

                # Validate the list length against the validation set
                if wsc.validate(validation_set, list_name, list_len) == 1:
                    logging.info(f"     Query has completed running and has a total of {list_len} people. Matches number on"
                                 f" PlanningCenter.com? YES, VALID.")
                else:
                    logging.warning(f"     Query has completed running and has a total of {list_len} people. Matches number"
                                    f" on PlanningCenter.com? NO, INVALID.")

        return people_list

    # Helper function to get the primary email of a person
    def get_primary_email(person_id, pco):
        primary_email_dict = pco.get(f'/people/v2/people/{person_id}/emails', **{"where[primary]": True})
        return primary_email_dict['data'][0]['attributes']['address'] if primary_email_dict['data'] else ''

    # Helper function to get the primary phone number of a person
    def get_primary_phone(person_id, pco):
        primary_phone_dict = pco.get(f'/people/v2/people/{person_id}/phone_numbers', **{"where[primary]": True})
        return primary_phone_dict['data'][0]['attributes']['national'] if primary_phone_dict['data'] else ''

    # Helper function to calculate the age of a person
    def calc_age(birth_date):
        if birth_date:
            birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
            age_pre = date.today().year - birth_date.year - ((date.today().month, date.today().day) < (birth_date.month, birth_date.day))
            return f"{age_pre} years"
        return ''

    # Helper function to convert grade data to a string
    def stringify_grade(data_grade):
        return f"Grade {data_grade}" if data_grade else ''

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

    # Task to upload CSV files to S3 from memory
    @task
    def upload_to_s3(csv_data: dict):
        s3 = boto3.client('s3')
        for csv_name, csv_content in csv_data.items():
            s3.put_object(Bucket="planningcenter", Key=f"CSVs/{csv_name}.csv", Body=csv_content)
            logging.info(f"Completed writing {csv_name}.csv to AWS S3 bucket")

    # Task to upload CSV files to Google Sheets from memory 
    @task
    def make_google_sheet(csv_data: dict):
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '/opt/airflow/Sheets_API_JSON/client_secret.json', scope)
        client = gspread.authorize(credentials)

        for csv_name, csv_content in csv_data.items():
            spreadsheet = client.open(csv_name)
            client.import_csv(spreadsheet.id, data=csv_content)
            logging.info(f"{csv_name} has completed uploading to Google Sheets")

        logging.info("\n\n"
                     "**************************************************************************************************\n"
                     f"Planning Center data pull completed on {datetime.now()} ....\n"
                     "**************************************************************************************************\n")

    # Define task dependencies
    log_res = log_start()
    validation_set = scrape_validate()
    people_list = pull_data(validation_set)
    csv_data = make_csv(people_list)
    upload_to_s3_res = upload_to_s3(csv_data)
    google_sheet_res = make_google_sheet(csv_data)

    log_res >> validation_set >> people_list >> csv_data >> upload_to_s3_res >> google_sheet_res

# Instantiate the DAG
pco_taskflow()
