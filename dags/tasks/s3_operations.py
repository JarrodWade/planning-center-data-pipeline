from airflow.decorators import task
import logging
import boto3

# Task to upload CSV files to S3 from memory
@task
def upload_to_s3(csv_data: dict):
    s3 = boto3.client("s3")
    for csv_name, csv_content in csv_data.items():
        s3.put_object(
            Bucket="planningcenter", Key=f"CSVs/{csv_name}.csv", Body=csv_content
        )
        logging.info(f"Completed writing {csv_name}.csv to AWS S3 bucket")
