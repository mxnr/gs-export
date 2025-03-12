# CSV to Google Sheets Importer

A Python script to bulk import CSV files into Google Sheets with support for Korean text and robust error handling.

## Features

- Bulk import of CSV files to Google Sheets
- Support for Korean text (UTF-8, CP949 encoding)
- Automatic rate limiting and retry mechanism
- Secure credential handling
- Progress tracking with emoji indicators
- Detailed logging

## Setup

1. Clone the repository:
```bash
git clone <your-repo-url>
cd <repo-directory>
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up Google Sheets API:
   - Go to [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project or select an existing one
   - Enable the Google Sheets API and Google Drive API
   - Create credentials (OAuth 2.0 Client ID)
   - Download the credentials and save as `credentials.json` in the project directory

4. Configure the script:
   - Copy `config.template.py` to `config.py`
   - Update `TEMPLATE_SPREADSHEET_ID` in `config.py` with your template spreadsheet ID
   - Adjust other settings in `config.py` if needed

## Usage

1. Place your CSV files in the `csv_files` directory

2. Run the script:
```bash
python sheets_importer.py
```

3. Follow the prompts to:
   - Authenticate with Google (first time only)
   - Enter a name for the new spreadsheet

The script will:
- Create a new spreadsheet
- Import each CSV file into a separate sheet
- Show progress with emoji indicators
- Log all operations to `import_log.txt`

## Configuration

Edit `config.py` to customize:
- `TEMPLATE_SPREADSHEET_ID`: ID of your template spreadsheet
- `MAX_FILE_SIZE_MB`: Maximum allowed CSV file size
- `REQUEST_DELAY`: Delay between API requests
- `FILE_DELAY`: Delay between processing files
- `MAX_RETRIES`: Maximum number of retry attempts

## Security

- Sensitive files (`credentials.json`, `token.pickle`, `config.py`) are automatically excluded from git
- Credentials are stored securely with appropriate file permissions
- OAuth 2.0 authentication is used for Google API access

## Logging

- Console output shows progress with emoji indicators
- Detailed logs are saved to `import_log.txt`
- Different log levels for different types of messages

## Error Handling

- Automatic retry for rate limit errors
- Multiple encoding support for CSV files
- Validation of file sizes and formats
- Detailed error messages and logging 