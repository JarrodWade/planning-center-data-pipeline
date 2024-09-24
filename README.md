# Planning Center Data Pipeline

This project is an Apache Airflow-based data pipeline for scraping, validating, and processing data from Planning Center. The pipeline includes tasks for web scraping, API data extraction, data validation, and uploading results to AWS S3 and Google Sheets.

## Project Structure
```
├── dags
│ ├── classes
│ │ └── Person.py
│ ├── planning_center_dag.py
│ └── webscraper.py
├── config
├── logs
├── plugins
├── CSVs
│ └── CSV_fmt.json
├── Sheets_API_JSON
│ └── client_secret.json
└── docker-compose.yaml
```
## Prerequisites

- Docker
- Docker Compose
- Python 3.7+
- Apache Airflow
- Google Sheets and Google API Credentials
- AWS Account for AWS S3

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/yourusername/planning-center-data-pipeline.git
   cd planning-center-data-pipeline
   ```

2. **Create and configure the `.env` file:**

   Create a `.env` file in the root directory and add the following environment variables:

   ```env
   PCO_USER_ID=your_pco_user_id
   PCO_PASSWORD=your_pco_password
   PCO_ID=your_pco_id
   PCO_SECRET=your_pco_secret
   GOOGLE_AUTH_SECRET=your_google_auth_secret
   ```

3. **Build and start the Docker containers:**

   ```bash
   docker-compose up --build
   ```

4. **Access the Airflow web interface:**

   Open your web browser and go to `http://localhost:8080`. Use the default credentials (`airflow`/`airflow`) to log in.

## DAG Overview

The main DAG is defined in `dags/planning_center_dag.py` and includes the following tasks:

- **log_start**: Logs the start of the DAG run.
- **scrape_validate**: Scrapes and validates data from Planning Center.
- **pull_data**: Pulls data from the Planning Center API.
- **make_csv**: Creates CSV files from the data in-memory.
- **upload_to_s3**: Uploads the CSV files to AWS S3.
- **make_google_sheet**: Uploads the CSV files to Google Sheets.

## Custom Classes and Scripts

- **Person Class**: Defined in `dags/classes/Person.py`, used to represent individuals in the data.
- **Web Scraper**: Functions for scraping Planning Center are defined in `dags/webscraper.py`.

## Configuration

- **Docker Compose**: The `docker-compose.yaml` file defines the services required for the Airflow setup, including PostgreSQL, Redis, and Selenium for web scraping.
- **Airflow Configuration**: Environment variables for Airflow are set in the `docker-compose.yaml` file and the `.env` file.

## Contact

Jarrod Wade - jarrod.wadej@gmail.com
