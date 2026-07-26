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

2. Create a virtual environment and install dependencies:
```bash
python3 -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
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

Run these commands from the repository root:

```bash
venv/bin/python sheets_importer.py
```

Before running:

1. Place your CSV files in the `csv_files` directory.
2. Make sure `config.py` and `credentials.json` exist (see Setup).
3. Follow the prompt to authenticate with Google, if needed, and enter a name for the new spreadsheet.

The saved login in `token.pickle` is reused on later runs.

The script will:

- Copy the configured template to a new spreadsheet
- Import each CSV file into a separate sheet
- Show progress with emoji indicators
- Log all operations to `import_log.txt`

Alternatively, activate the environment first:

```bash
source venv/bin/activate
python sheets_importer.py
```

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
