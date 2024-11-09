# Planning Center Data Pipeline

This project is an Apache Airflow-based data pipeline for scraping, validating, and processing data from Planning Center. The pipeline includes tasks for web scraping, API data extraction, data validation, and uploading results to AWS S3 and Google Sheets to support analysis.

## Project Structure
```
├── dags
│ ├── classes
│ │ └── Person.py
│ ├── planning_center_dag.py
│ └── tasks
│   └── webscraper.py
│   └── csv_operations.py
│   └── planning_center.py
│   └── csv_operations.py
│   └── google_sheets.py
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
   GOOGLE_SHEET_MASTER=your_master_worksheet
   GOOGLE_SHEET_REF_TAB=your_source_tab
   GOOGLE_SHEET_DEST_TAB=your_destination_tab
   ```

3. **Create CSV format file in `CSVs` folder called `CSV_fmt.json`**

	Please update to match your Planning Center lists of interest and desired CSV file names. 
	 ```JSON
	{
	  "Example_List_1":  "List_1",
	  "Example_List_2":  "List_2",
	  "Example_List_3":  "List_3",
	  ...
	}
	 ```

	 This is used to name the CSV files (per list) that are created from the Planning Center data.
	
	 See below for how this functions in practice:

	 ```Python
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
	 ```

4. **Modify Google Sheets task as needed in `process_google_sheets()`**

   Modify the following actions to fit your needs:
   
	```Python
        # Refresh master sheet
        spreadsheet = client.open(os.getenv('GOOGLE_SHEET_MASTER'))
        body = {
                "requests": [
                    {
                        "findReplace": {
                            "find": "=",
                            "includeFormulas": True, #ensures our formulas are updated
                            "allSheets": True, #refresh all sheets
                            "replacement": "=" #replace with = (again, refreshes functions)
                        }
                    }
                ]
            }
        spreadsheet.batch_update(body)

        logging.info("Sheet refresh completed for all sheets")

        # Update master sheet
        sourceSheetName = os.getenv('GOOGLE_SHEET_REF_TAB')
        destinationSheetName = os.getenv('GOOGLE_SHEET_DEST_TAB')

        sourceSheetId = spreadsheet.worksheet(sourceSheetName)._properties['sheetId']
        destinationSheetId = spreadsheet.worksheet(destinationSheetName)._properties['sheetId']
        
        body = {
            "requests": [
                {
                    "copyPaste": {
                        "source": {
                            "sheetId": sourceSheetId,
                            "startRowIndex": 2, # Skip header row + description row
                            "endRowIndex": 500, # 500 rows
                            "startColumnIndex": 0, # column A
                            "endColumnIndex": 5 # column E
                        },
                        "destination": {
                            "sheetId": destinationSheetId,
                            "startRowIndex": 1, # Skip header row
                            "endRowIndex": 500,
                            "startColumnIndex": 0, # column A
                            "endColumnIndex": 5 # column E
                        },
                        "pasteType": "PASTE_VALUES"
                    }
                }
            ]
        }
        spreadsheet.batch_update(body)
	```

6. **Build and start the Docker containers:**

   ```bash
   docker-compose up --build
   ```

7. **Access the Airflow web interface:**

   Open your web browser and go to `http://localhost:8080`. Use the default credentials (`airflow`/`airflow`) to log in.

## DAG Overview

The main DAG is defined in `dags/planning_center_dag.py` and includes the following tasks:

- **log_start**: Logs the start of the DAG run.
- **scrape_validate**: Scrapes and validates data from Planning Center.
- **pull_data**: Pulls data from the Planning Center API.
- **make_csv**: Creates CSV files from the data in-memory.
- **upload_to_s3**: Uploads the CSV files to AWS S3.
- **process_google_sheets**: Uploads the CSV files to Google Sheets, refreshes formulas, automates data copy.

## Custom Classes and Scripts

- **Person Class**: Defined in `dags/classes/Person.py`, used to represent individuals in the data.
- **Web Scraper**: Functions for scraping Planning Center are defined in `dags/tasks/webscraper.py`.

## Configuration

- **Docker Compose**: The `docker-compose.yaml` file defines the services required for the Airflow setup, including PostgreSQL, Redis, and Selenium for web scraping.
- **Airflow Configuration**: Environment variables for Airflow are set in the `docker-compose.yaml` file and the `.env` file.

## Contact

Jarrod Wade - jarrod.wadej@gmail.com
