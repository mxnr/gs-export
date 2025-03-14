import os
import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pickle
from tqdm import tqdm
import time
import random
from datetime import datetime
import logging
import stat
import sys
import warnings

# Try to import local config, fall back to template if not found
try:
    from config import *
except ImportError:
    logger.error("config.py not found. Please copy config.template.py to config.py and update the values.")
    sys.exit(1)

# Suppress the file_cache warning
warnings.filterwarnings('ignore', message='file_cache is only supported with oauth2client<4.0.0')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('import_log.txt')
    ]
)
logger = logging.getLogger(__name__)

# Disable unnecessary logging from google api client
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)

# If modifying these scopes, delete the file token.pickle.
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'  # Added drive scope for copying spreadsheets
]

# File constants
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'

# Fun emoji arrays for random selection
SUCCESS_EMOJIS = ['🎉', '✨', '🌟', '🚀', '💫', '🎯', '🌈']
WORKING_EMOJIS = ['🔨', '⚙️', '🛠️', '🔧', '💪', '🤖', '🔄']
ERROR_EMOJIS = ['😱', '🚨', '💥', '⚡', '🆘', '😅', '🤔']

def secure_file_permissions(filepath):
    """Set secure permissions for sensitive files."""
    try:
        # Set file permissions to owner read/write only (600)
        os.chmod(filepath, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
        logger.warning(f"Could not set permissions for {filepath}: {e}")

def get_random_emoji(emoji_list):
    return random.choice(emoji_list)

def validate_file_size(file_path):
    """Validate if file size is within acceptable limits."""
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(f"File size ({file_size_mb:.2f}MB) exceeds maximum allowed size ({MAX_FILE_SIZE_MB}MB)")
    return True

def get_user_input_name():
    """Prompts the user for a spreadsheet name."""
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Please enter a name for your spreadsheet:")
    while True:
        name = input().strip()
        if name:
            return name
        logger.error(f"{get_random_emoji(ERROR_EMOJIS)} Name cannot be empty. Please try again:")

def get_credentials():
    """Gets valid user credentials from storage or initiates OAuth2 flow."""
    creds = None
    
    # Check if credentials.json exists
    if not os.path.exists(CREDENTIALS_FILE):
        logger.error(f"Missing {CREDENTIALS_FILE}. Please obtain credentials from Google Cloud Console.")
        sys.exit(1)
    
    # The file token.pickle stores the user's access and refresh tokens
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE, 'rb') as token:
                creds = pickle.load(token)
        except Exception as e:
            logger.error(f"Error reading token file: {e}")
            os.remove(TOKEN_FILE)  # Remove corrupted token file
            return get_credentials()  # Retry authentication
    
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.error(f"Error refreshing credentials: {e}")
                os.remove(TOKEN_FILE)  # Remove invalid token file
                return get_credentials()  # Retry authentication
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Save the credentials for the next run
        try:
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(creds, token)
            secure_file_permissions(TOKEN_FILE)
        except Exception as e:
            logger.error(f"Error saving credentials: {e}")
    
    return creds

def create_spreadsheet(service):
    """Creates a new Google Spreadsheet by copying the template."""
    try:
        base_name = get_user_input_name()
        current_date = datetime.now().strftime("%d%m%Y")
        copy_title = f"{base_name} {current_date}"
        
        creds = service._http.credentials
        drive_service = build('drive', 'v3', credentials=creds)
        
        copied_file = drive_service.files().copy(
            fileId=TEMPLATE_SPREADSHEET_ID,
            body={'name': copy_title}
        ).execute()
        
        logger.info(f"\n{get_random_emoji(SUCCESS_EMOJIS)} Created new spreadsheet '{copy_title}' with ID: {copied_file['id']}")
        return copied_file['id']
    except HttpError as error:
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} Error creating spreadsheet: {error}")
        return None

def import_csv_to_sheet(service, spreadsheet_id, csv_file, retry_count=0):
    """Imports a CSV file into a new sheet in the spreadsheet."""
    try:
        # Validate file size before processing
        validate_file_size(csv_file)
        
        # Read CSV file with robust error handling
        try:
            df = pd.read_csv(csv_file, encoding='utf-8', on_bad_lines='skip')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(csv_file, encoding='cp949', on_bad_lines='skip')
            except UnicodeDecodeError:
                df = pd.read_csv(csv_file, encoding='latin-1', on_bad_lines='skip')
        
        if df.empty or (len(df) == 1 and df.iloc[0].str.contains('NO RECORDS').any()):
            df = pd.DataFrame([["NO RECORDS"]])
        
        df = df.fillna('')
        
        def clean_value(val):
            if pd.isna(val):
                return ''
            val_str = str(val).strip()
            val_str = val_str.replace('\x00', '').replace('\r', '').replace('\n', ' ')
            return val_str
        
        df = df.apply(lambda x: x.map(clean_value))
        
        sheet_name = os.path.splitext(os.path.basename(csv_file))[0]
        body = {
            'requests': [{
                'addSheet': {
                    'properties': {
                        'title': sheet_name
                    }
                }
            }]
        }
        
        time.sleep(REQUEST_DELAY)
        
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
        values = [df.columns.values.tolist()] + df.values.tolist()
        body = {'values': values}
        
        time.sleep(REQUEST_DELAY)
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!A1',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        return True
    except HttpError as error:
        if error.resp.status == 429 and retry_count < MAX_RETRIES:
            retry_delay = (2 ** retry_count) * 30
            logger.warning(f"\n{get_random_emoji(WORKING_EMOJIS)} Rate limit reached. Waiting {retry_delay} seconds before retry...")
            time.sleep(retry_delay)
            return import_csv_to_sheet(service, spreadsheet_id, csv_file, retry_count + 1)
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} Error processing {csv_file}: {error}")
        return False
    except Exception as e:
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} Error processing {csv_file}: {e}")
        return False

def update_summary_sheet(service, spreadsheet_id, csv_files, successful_imports):
    """Updates the first sheet with summary of imported files and row counts."""
    try:
        # Get the first sheet's title
        sheet_metadata = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        first_sheet_title = sheet_metadata['sheets'][0]['properties']['title']

        # Prepare the summary data
        summary_data = []
        for csv_file in csv_files:
            if csv_file in successful_imports:
                sheet_name = os.path.splitext(csv_file)[0]
                summary_data.append([
                    sheet_name,
                    f'=COUNTA(INDIRECT(A{len(summary_data) + 1}&"!A:A")) - 1'
                ])

        # Update the first sheet
        if summary_data:
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f'{first_sheet_title}!A1',
                valueInputOption='USER_ENTERED',  # Required for formulas
                body={'values': summary_data}
            ).execute()
            
            logger.info(f"{get_random_emoji(SUCCESS_EMOJIS)} Added summary to the first sheet")
    except Exception as e:
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} Error updating summary: {e}")

def main():
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Starting up the CSV to Google Sheets importer...")
    
    # Create .gitignore if it doesn't exist
    gitignore_file = '.gitignore'
    gitignore_entries = set(['token.pickle', 'credentials.json', 'import_log.txt'])
    
    try:
        if os.path.exists(gitignore_file):
            with open(gitignore_file, 'r') as f:
                existing_entries = set(f.read().splitlines())
        else:
            existing_entries = set()
        
        # Add missing entries to .gitignore
        missing_entries = gitignore_entries - existing_entries
        if missing_entries:
            with open(gitignore_file, 'a') as f:
                for entry in missing_entries:
                    f.write(f'\n{entry}')
    except Exception as e:
        logger.warning(f"Could not update .gitignore: {e}")
    
    # Secure sensitive files
    for file in [CREDENTIALS_FILE, TOKEN_FILE]:
        if os.path.exists(file):
            secure_file_permissions(file)
    
    if not os.path.exists('csv_files'):
        os.makedirs('csv_files')
        logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Created 'csv_files' directory. Please place your CSV files there.")
        return
    
    csv_files = [f for f in os.listdir('csv_files') if f.endswith('.csv')]
    total_files = len(csv_files)
    
    if not csv_files:
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} No CSV files found in 'csv_files' directory!")
        return
    
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Found {total_files} CSV files to process!")
    
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Authenticating with Google Sheets API...")
    creds = get_credentials()
    
    try:
        service = build('sheets', 'v4', credentials=creds)
        spreadsheet_id = create_spreadsheet(service)
        if not spreadsheet_id:
            return

        logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Starting to import CSV files...")
        
        successful_imports = []
        for index, csv_file in enumerate(csv_files, 1):
            working_emoji = get_random_emoji(WORKING_EMOJIS)
            logger.info(f"\n{working_emoji} Processing file {index} of {total_files}: {csv_file}")
            
            success = import_csv_to_sheet(
                service,
                spreadsheet_id,
                os.path.join('csv_files', csv_file)
            )
            
            if success:
                successful_imports.append(csv_file)
                logger.info(f"{get_random_emoji(SUCCESS_EMOJIS)} Successfully imported {csv_file} ({len(successful_imports)} of {total_files} done)")
            
            if index < len(csv_files):
                time.sleep(FILE_DELAY)
        
        # Update the first sheet with summary
        update_summary_sheet(service, spreadsheet_id, csv_files, successful_imports)
        
        logger.info(f"\n{get_random_emoji(SUCCESS_EMOJIS)} Import complete!")
        logger.info(f"✓ Successfully imported {len(successful_imports)} of {total_files} files")
        if len(successful_imports) < total_files:
            logger.warning(f"✗ Failed to import {total_files - len(successful_imports)} files")
        logger.info(f"📊 Your spreadsheet is ready at: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")
        
    except HttpError as error:
        logger.error(f"\n{get_random_emoji(ERROR_EMOJIS)} An error occurred: {error}")

if __name__ == '__main__':
    main() 