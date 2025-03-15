#!/usr/bin/env python3
"""
Google Sheets CSV Importer.
This script imports CSV files into Google Sheets using the Google Sheets API.
"""

# Standard library imports
import logging
import os
import pickle
import random
import stat
import sys
import time
import warnings
from datetime import datetime
from typing import Any, Optional

# Third-party imports
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Local imports
try:
    from config import *
except ImportError:
    print("config.py not found. Please copy config.template.py to config.py and update the values.")
    sys.exit(1)

# Constants
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.pickle'

SUCCESS_EMOJIS = ['🎉', '✨', '🌟', '🚀', '💫', '🎯', '🌈']
WORKING_EMOJIS = ['🔨', '⚙️', '🛠️', '🔧', '💪', '🤖', '🔄']
ERROR_EMOJIS = ['😱', '🚨', '💥', '⚡', '🆘', '😅', '🤔']

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

# Suppress the file_cache warning
warnings.filterwarnings('ignore', message='file_cache is only supported with oauth2client<4.0.0')

def secure_file_permissions(filepath: str) -> None:
    """Set secure permissions for sensitive files.
    
    Args:
        filepath: Path to the file to secure
    """
    try:
        # Set file permissions to owner read/write only (600)
        os.chmod(filepath, stat.S_IRUSR | stat.S_IWUSR)
    except Exception as e:
        logger.warning(f"Could not set permissions for {filepath}: {e}")

def get_random_emoji(emoji_list: list[str]) -> str:
    """Get a random emoji from the provided list.
    
    Args:
        emoji_list: List of emoji strings to choose from
        
    Returns:
        A randomly selected emoji
    """
    return random.choice(emoji_list)

def validate_file_size(file_path: str, max_size_mb: float = MAX_FILE_SIZE_MB) -> None:
    """Validate if file size is within acceptable limits.
    
    Args:
        file_path: Path to the file to check
        max_size_mb: Maximum allowed file size in MB
        
    Raises:
        ValueError: If file size exceeds maximum allowed size
    """
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    if file_size_mb > max_size_mb:
        raise ValueError(
            f"File size ({file_size_mb:.2f}MB) exceeds maximum allowed size ({max_size_mb}MB)"
        )

def get_user_input_name() -> str:
    """Prompt the user for a spreadsheet name.
    
    Returns:
        User provided spreadsheet name
    """
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Please enter a name for your spreadsheet:")
    while True:
        name = input().strip()
        if name:
            return name
        logger.error(f"{get_random_emoji(ERROR_EMOJIS)} Name cannot be empty. Please try again:")

def get_credentials() -> Credentials:
    """Get valid user credentials from storage or initiate OAuth2 flow.
    
    Returns:
        Valid Google OAuth2 credentials
        
    Raises:
        SystemExit: If credentials.json is missing
    """
    # Check if credentials.json exists
    if not os.path.exists(CREDENTIALS_FILE):
        logger.error(f"Missing {CREDENTIALS_FILE}. Please obtain credentials from Google Cloud Console.")
        sys.exit(1)
    
    while True:
        creds = None
        # The file token.pickle stores the user's access and refresh tokens
        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE, 'rb') as token:
                    creds = pickle.load(token)
            except Exception as e:
                logger.error(f"Error reading token file: {e}")
                os.remove(TOKEN_FILE)
                continue
        
        # If there are no (valid) credentials available, let the user log in
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    logger.error(f"Error refreshing credentials: {e}")
                    os.remove(TOKEN_FILE)
                    continue
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
                # Continue even if we couldn't save the token
        
        return creds

def create_spreadsheet(service: Any) -> Optional[str]:
    """Create a new Google Spreadsheet by copying the template.
    
    Args:
        service: Google Sheets API service instance
        
    Returns:
        The ID of the created spreadsheet, or None if creation failed
    """
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

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and prepare DataFrame for Google Sheets import.
    
    Args:
        df: Input DataFrame to clean
        
    Returns:
        Cleaned DataFrame
    """
    if df.empty or (len(df) == 1 and df.iloc[0].str.contains('NO RECORDS').any()):
        return pd.DataFrame([["NO RECORDS"]])
    
    df = df.fillna('')
    
    def clean_value(val: Any) -> str:
        if pd.isna(val):
            return ''
        val_str = str(val).strip()
        val_str = val_str.replace('\x00', '').replace('\r', '').replace('\n', ' ')
        return val_str
    
    return df.apply(lambda x: x.map(clean_value))

def import_csv_to_sheet(
    service: Any,
    spreadsheet_id: str,
    csv_file: str,
    retry_count: int = 0
) -> bool:
    """Import a CSV file into a new sheet in the spreadsheet.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the target spreadsheet
        csv_file: Path to the CSV file to import
        retry_count: Number of retries attempted so far
        
    Returns:
        True if import was successful, False otherwise
    """
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
        
        df = _clean_dataframe(df)
        
        sheet_name = os.path.splitext(os.path.basename(csv_file))[0]
        
        # Create new sheet
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
        
        # Update sheet with data
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

def update_summary_sheet(
    service: Any,
    spreadsheet_id: str,
    csv_files: list[str],
    successful_imports: list[str]
) -> None:
    """Update the first sheet with summary of imported files and row counts.
    
    Args:
        service: Google Sheets API service instance
        spreadsheet_id: ID of the target spreadsheet
        csv_files: List of CSV files that were processed
        successful_imports: List of CSV files that were successfully imported
    """
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

def setup_workspace() -> None:
    """Set up the workspace with necessary files and permissions."""
    # Create .gitignore if it doesn't exist
    gitignore_file = '.gitignore'
    gitignore_entries = {'token.pickle', 'credentials.json', 'import_log.txt'}
    
    try:
        existing_entries = set()
        if os.path.exists(gitignore_file):
            with open(gitignore_file, 'r') as f:
                existing_entries = set(f.read().splitlines())
        
        # Add missing entries to .gitignore
        missing_entries = gitignore_entries - existing_entries
        if missing_entries:
            with open(gitignore_file, 'a') as f:
                f.write('\n'.join(missing_entries) + '\n')
    except Exception as e:
        logger.warning(f"Could not update .gitignore: {e}")
    
    # Secure sensitive files
    for file in [CREDENTIALS_FILE, TOKEN_FILE]:
        if os.path.exists(file):
            secure_file_permissions(file)
    
    # Create csv_files directory if it doesn't exist
    os.makedirs('csv_files', exist_ok=True)
    if not any(f.endswith('.csv') for f in os.listdir('csv_files')):
        logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Created 'csv_files' directory. Please place your CSV files there.")

def main() -> None:
    """Main entry point for the CSV to Google Sheets importer."""
    logger.info(f"\n{get_random_emoji(WORKING_EMOJIS)} Starting up the CSV to Google Sheets importer...")
    
    setup_workspace()
    
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