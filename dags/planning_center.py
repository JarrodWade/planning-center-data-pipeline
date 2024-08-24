# Imports
import pypco
import csv
import sys
import gspread
import glob
import logging
import json
from _datetime import datetime
from datetime import date
import boto3
import airflow
import json
import pendulum
from airflow.decorators import dag, task
from datetime import datetime
from datetime import timedelta
import os
import glob
from pathlib import Path
from oauth2client.service_account import ServiceAccountCredentials

# import class Person to assist in tracking Youths and utilize class data structures
from Person import Person

# script with functions to scrape PlanningCenter.com and return list counts as displayed on the website (for validation)
import webscraper as wsc


@dag(
    schedule="@weekly",
    start_date=pendulum.datetime(2024, 1, 1, tz="MST"),
    catchup=False,
    tags=["JJW"],
)
def pco_taskflow():

    # initializes / creates logger for tracking log info / run time / status updates
    @task
    def log():

        # dir_path = os.path.dirname(os.path.realpath(__file__))

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.StreamHandler(stream=sys.stdout)
            ]
        )

        logging.info("\n"
                    "__________________________________________________________________________________________________\n"
                    f"Planning Center data pull is starting new run on {datetime.now()} ....\n"
                    "__________________________________________________________________________________________________\n")
        


    @task
    def scrape_validate():
        
        # WecScraper use webscraper to get {list name: count} - this will be used to validate our use of the API
        html = wsc.get_html(user_id=wsc.get_credentials()[0], password=wsc.get_credentials()[1],
                            url='https://people.planningcenteronline.com/lists', secret=wsc.get_secret())
        validation_set = wsc.scrape(html)

        logging.info("\n"
                    "__________________________________________________________________________________________________\n"
                    f"Planning Center validation web scrape completed on {datetime.now()} ....\n"
                    "__________________________________________________________________________________________________\n")

        return validation_set

    @task
    def pull_data(validation_set :dict):
        # pulls data from Planning Center using API. Produces a dictionary
        # "data" in format
        #          {"list #1 name":
        #             [ {person#1}, {person#2}, {person(s)...}  ],
        #           "list #2 name":
        #             [ {person#1}, {person#2}, {person(s)...}  ],
        #           "list #x name":
        #             [ {person#1}, {person#2}, {person(s)...}  ],
        #           } #end dict


        # initialize PCO obj which serves as interface for accessing data
        with open('/opt/airflow/Documents/pco_id.txt', 'r') as a, \
            open('/opt/airflow/Documents/pco_sec.txt', 'r') as b:
            pco = pypco.PCO(a.read(), b.read())


        # master dict, keeping track of list : People objects
        people_list = {}

        # return only Youth Lists (with "Youth" in list name). There may (likely will) be other lists, but we aren't concerned with them
        for pc_list in pco.iterate('/people/v2/lists'):
            if "Youth" in pc_list["data"]["attributes"]["name"]:

                list_name = pc_list["data"]["attributes"]["name"]
                list_id = pc_list["data"]["id"]
                list_path = pc_list["data"]["links"]["self"]

                logging.info(
                    f"CURRENT LIST: {list_name}"
                    )

                # show list name, list id, and unique list link/path
                logging.info(f"     It has unique list ID {list_id}. API path is: {list_path}")

                # set variable containing unique list_results path. Basically link to list ID + list Results
                list_link = list_path + "/list_results"

                # default dictionary value with a blank list. Will append to it in following step
                people_list[list_name] = []

                # iterate through results per each Youth list
                for result in pco.iterate(list_link):
                    person_id = result["data"]["relationships"]["person"]["data"]["id"]

                    # first get the person json / dict
                    person = pco.get(f'/people/v2/people/{person_id}')
                    # then narrow down to the attributes we are interested in (nested down further in json)
                    person_attributes = person['data']['attributes']

                    # let's make a Youth object for each person (will be duplicates for those in mult lists)
                    # could be a simple dictionary here too, but testing with object.__dict__ for now
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
                    # print(youth.__dict__)

                list_len = len(people_list[list_name])

                if wsc.validate(validation_set, list_name, list_len) == 1:

                    # print(people_list)
                    logging.info(f"     Query has completed running and has a total of {list_len} people. Matches number on"
                                f" PlanningCenter.com? YES, VALID.")

                else:
                    logging.warning(f"     Query has completed running and has a total of {list_len} people. Matches number"
                                    f" on PlanningCenter.com? NO, INVALID.")

        return people_list


    def get_primary_email(person_id, pco):
        # grab only the primary emails
        primary_email_dict = pco.get(f'/people/v2/people/{person_id}/emails',
                                    **{"where[primary]": True})
        # handle nulls. if exists, basically
        if len(primary_email_dict['data']) > 0:
            primary_email_final = primary_email_dict['data'][0]['attributes']['address']
        else:
            primary_email_final = ''
        # print(primary_email_final)

        return primary_email_final


    def get_primary_phone(person_id, pco):
        # grab only the primary phone numbers
        primary_phone_dict = pco.get(f'/people/v2/people/{person_id}/phone_numbers',
                                    **{"where[primary]": True})
        # handle nulls. if exists, basically
        if len(primary_phone_dict['data']) > 0:
            primary_phone_final = primary_phone_dict['data'][0]['attributes']['national']
        else:
            primary_phone_final = ''

        return primary_phone_final


    def calc_age(birth_date):
        today = date.today()
        # age
        if birth_date is not None:
            birth_date = datetime.strptime(birth_date, '%Y-%m-%d')
            # create age
            age_pre = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            age = str(age_pre) + " years"
        else:
            age = ''

        return age


    def stringify_grade(data_grade):
        if data_grade is not None:
            grade = "Grade " + str(data_grade)
        else:
            grade = ''

        return grade

    @task
    def make_csv(people_list :dict):

        field_names = ['name', 'primary_email', 'primary_phone_number', 'grade', 'age']

        # error handling would be good
        with open('/opt/airflow/CSVs/CSV_fmt.json', 'r') as json_file:
            csv_fmt = json.load(json_file)

        # aka... for list in people_list.lists 
        for key in people_list.keys():

            if key in csv_fmt:
                csv_name = csv_fmt[key]
                # print(csv_name, key)

                csv_path = f'./CSVs/{csv_name}.csv'

                with open(csv_path, 'w') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=field_names, extrasaction='ignore')
                    writer.writeheader()

                    writer.writerows(people_list[key])
                
                logging.info(f"Completed writing {csv_name} to CSV")

    
    @task
    def upload_to_s3():

        # S3 Connection
        s3 = boto3.client('s3')
 
        # iterate over files in CSVs
        path = "/opt/airflow/CSVs/*.csv"
        for file in glob.glob(path):

            file_no_ext = Path(f'{file}').stem

            #right now, just uploading to S3 for backup
            s3.upload_file(file, "planningcenter", f"CSVs/{file_no_ext}.csv")

            logging.info(f"Completed writing {file_no_ext}.csv to AWS S3 bucket")

    @task
    def make_google_sheet():

        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
                "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '/opt/airflow/Sheets_API_JSON/client_secret.json', scope)
        client = gspread.authorize(credentials)

        # Get CSV files list from a folder
        path = '/opt/airflow/CSVs'
        csv_files = glob.glob(path + "/*.csv")
        # print(csv_files)

        for file in csv_files:

            sheet_no_ext = Path(f'{file}').stem
            # print(sheet_no_ext)

            spreadsheet = client.open(sheet_no_ext)

            # worksheet = spreadsheet.worksheet(sheet_no_ext)
            # print(worksheet)

            with open(file, 'r') as file_obj:
                content = file_obj.read()
                client.import_csv(spreadsheet.id, data=content)

            logging.info(f"{sheet_no_ext} has completed uploading to Google Sheets")

        logging.info("\n\n"
                    "**************************************************************************************************\n"
                    f"Planning Center data pull completed on {datetime.now()} ....\n"
                    "**************************************************************************************************\n")

    @task
    def purge_temp():
        import os

        # Get CSV files list from this folder
        path = '/opt/airflow/CSVs'
        csv_files = glob.glob(path + "/*.csv")

        for file in csv_files:

            # If file exists, delete it.
            if os.path.isfile(file):
                os.remove(file)

                logging.info("\n\n"
                    "**************************************************************************************************\n"
                    f"Successfully cleared out temp file {file}"
                    "**************************************************************************************************\n")

            else:
                # If it fails, inform the user.
                logging.warning(f"Error: {file} not found")

    # init log / begin process
    log_res = log()
    # web scrape site info to validate data pull numbers
    validation_set = scrape_validate()
    # pull data from Planning Center and validate numbers (using previouly scraped numbers)
    people_list = pull_data(validation_set)
    # take data (dictionary), and convert to CSVs
    make_csv_res = make_csv(people_list)
    # upload to S3
    upload_to_s3_res = upload_to_s3() 
    # write CSVs out to Google Sheets model for further analysis
    google_sheet_res = make_google_sheet()
    # clear out the temp CSVs once have been successfully written out to Sheets / S3
    purge_temp_res = purge_temp()

    # Facilitate dependencies (using new TaskFlow syntax)
    log_res >> validation_set >> people_list >> make_csv_res >> upload_to_s3_res >> google_sheet_res >> purge_temp_res

pco_taskflow()
