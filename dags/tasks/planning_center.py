from airflow.decorators import task
import logging
from datetime import datetime, date
import os
import pypco
from classes.Person import Person
import tasks.webscraper as wsc

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
