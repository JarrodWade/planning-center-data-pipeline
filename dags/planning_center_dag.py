from airflow.decorators import dag, task
import pendulum
import logging
from datetime import datetime
import sys

# Import tasks
from tasks.planning_center import pull_data
from tasks.csv_operations import make_csv
from tasks.s3_operations import upload_to_s3
from tasks.google_sheets import process_google_sheets
import tasks.webscraper as wsc

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


    # Define task dependencies
    log_res = log_start()
    validation_set = scrape_validate()
    people_list = pull_data(validation_set)
    csv_data = make_csv(people_list)
    upload_to_s3_res = upload_to_s3(csv_data)
    google_sheets_res = process_google_sheets(csv_data)

    # Define the task dependencies
    log_res >> validation_set >> people_list >> csv_data >> upload_to_s3_res >> google_sheets_res

# Instantiate the DAG
pco_taskflow()
