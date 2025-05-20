# Gmail CSV to MySQL Importer

This Python script automatically scans your Gmail account for CSV attachments matching the pattern "FitNotes_Export_[timestamp]" and imports them into a MySQL database.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up Gmail API:
   - Go to the [Google Cloud Console](https://console.cloud.google.com/)
   - Create a new project
   - Enable the Gmail API
   - Create OAuth 2.0 credentials
   - Download the credentials and save as `credentials.json` in the project root

3. Configure environment variables:
   - Copy `.env.example` to `.env`
   - Fill in your MySQL database credentials and Gmail settings

4. First run:
   - Run the script: `python main.py`
   - Follow the authentication flow in your browser
   - The token will be saved for future use

## Usage

Run the script:
```bash
python main.py
```

The script will:
1. Scan your Gmail for unread emails with CSV attachments
2. Download and process matching attachments
3. Import the data into your MySQL database
4. Mark processed emails as read

## Database Schema

The script expects a MySQL database with a table structure matching the CSV format from FitNotes exports. 