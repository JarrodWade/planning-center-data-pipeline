import logging
from datetime import datetime
import os
from oauth2client.service_account import ServiceAccountCredentials
import gspread
from airflow.decorators import task

@task
def process_google_sheets(csv_data: dict):
    """
    Combined task that handles all Google Sheets operations
    """
    try:
        # Initialize client
        scope = ["https://spreadsheets.google.com/feeds",
                'https://www.googleapis.com/auth/spreadsheets',
                "https://www.googleapis.com/auth/drive.file",
                "https://www.googleapis.com/auth/drive"]

        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            '/opt/airflow/Sheets_API_JSON/client_secret.json', scope)
        client = gspread.authorize(credentials)
        logging.info("Google Sheets client initialized successfully")
        
         # Check if environment variable exists
        sheet_name = os.getenv('GOOGLE_SHEET_MASTER')
        if not sheet_name:
            raise ValueError("GOOGLE_SHEET_MASTER environment variable is not set")
        else:
            logging.info(f"Master Sheet name we are working with: {sheet_name}")

        # Upload CSV data to sheets
        for csv_name, csv_content in csv_data.items():
            spreadsheet = client.open(csv_name)
            client.import_csv(spreadsheet.id, data=csv_content)
            logging.info(f"{csv_name} has completed uploading to Google Sheets")

        logging.info("CSV upload to Google Sheets completed")
        
        # List available spreadsheets to verify access
        available_sheets = client.list_spreadsheet_files()
        logging.info("Available spreadsheets:")
        for sheet in available_sheets:
            logging.info(f"- {sheet['name']}")

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
        
        logging.info("\n\n"
                    "**************************************************************************************************\n"
                    f"All Google Sheets operations completed successfully @ {datetime.now()} ....\n"
                    "**************************************************************************************************\n")
        
        return True

    except Exception as e:
        logging.error(f"Error in Google Sheets operations: {str(e)}")
        raise