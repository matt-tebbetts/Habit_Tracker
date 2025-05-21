import os
import base64
import re
from datetime import datetime
import pandas as pd
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
import pytz
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import zipfile
import io
from config import credentials, sql_addr

# Load environment variables
load_dotenv()
print("Environment variables loaded")

def get_gmail_connection():
    """Connect to Gmail using IMAP."""
    print("Attempting to connect to Gmail...")
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(os.getenv('GMAIL_USER'), os.getenv('GMAIL_PASS'))
    print("Successfully connected to Gmail")
    return mail

def get_db_connection():
    """Create and return SQLAlchemy database connection."""
    print("Attempting to connect to database...")
    engine = create_engine(sql_addr)
    print("Successfully connected to database")
    return engine

def search_emails(mail):
    """Search for emails with FitNotes CSV or Loop Habits ZIP attachments."""
    print("Searching for relevant emails...")
    mail.select('inbox')
    
    # First, let's look at all emails
    _, messages = mail.search(None, 'ALL')
    all_email_ids = messages[0].split()
    print(f"\nFound {len(all_email_ids)} total emails in inbox")
    
    # Print details of each email
    for email_id in all_email_ids:
        _, msg_data = mail.fetch(email_id, '(RFC822)')
        email_body = msg_data[0][1]
        email_message = email.message_from_bytes(email_body)
        
        # Get email details
        subject = decode_header(email_message["subject"])[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode()
        sender = decode_header(email_message["from"])[0][0]
        if isinstance(sender, bytes):
            sender = sender.decode()
        date = email_message["date"]
        
        print(f"\nEmail ID: {email_id.decode()}")
        print(f"From: {sender}")
        print(f"Subject: {subject}")
        print(f"Date: {date}")
        
        # Check if it has attachments
        has_attachments = False
        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
            filename = part.get_filename()
            if filename:
                has_attachments = True
                print(f"Attachment: {filename}")
        
        if not has_attachments:
            print("No attachments found")
    
    # Now search for our specific emails - looking for any email with FitNotes or Loop Habits attachments
    print("\nSearching for FitNotes or Loop Habits emails...")
    _, messages = mail.search(None, 'ALL')  # Search all emails
    email_ids = messages[0].split()
    
    # Filter emails that have the attachments we want
    relevant_emails = []
    for email_id in email_ids:
        _, msg_data = mail.fetch(email_id, '(RFC822)')
        email_body = msg_data[0][1]
        email_message = email.message_from_bytes(email_body)
        
        for part in email_message.walk():
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get('Content-Disposition') is None:
                continue
                
            filename = part.get_filename()
            if filename:
                if ('FitNotes_Export_' in filename and filename.endswith('.csv')) or \
                   ('Loop Habits CSV' in filename and filename.endswith('.zip')):
                    relevant_emails.append(email_id)
                    break
    
    print(f"Found {len(relevant_emails)} relevant emails")
    return relevant_emails

def get_attachment(mail, email_id):
    """Download attachment from Gmail."""
    print(f"Processing email ID: {email_id}")
    _, msg_data = mail.fetch(email_id, '(RFC822)')
    email_body = msg_data[0][1]
    email_message = email.message_from_bytes(email_body)
    
    attachments = []
    for part in email_message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
            
        filename = part.get_filename()
        if filename:
            if ('FitNotes_Export_' in filename and filename.endswith('.csv')) or \
               ('Loop Habits CSV' in filename and filename.endswith('.zip')):
                print(f"Found attachment: {filename}")
                attachments.append({
                    'filename': filename,
                    'data': part.get_payload(decode=True)
                })
    return attachments

def save_attachment(attachment, download_dir='files/downloads'):
    """Save attachment to disk if it doesn't exist."""
    print(f"Attempting to save attachment: {attachment['filename']}")
    os.makedirs(download_dir, exist_ok=True)
    filepath = os.path.join(download_dir, attachment['filename'])
    
    if not os.path.exists(filepath):
        with open(filepath, 'wb') as f:
            f.write(attachment['data'])
        print(f"Successfully saved file to: {filepath}")
        return filepath
    print(f"File already exists: {filepath}")
    return None

def process_fitnotes_data(filepath):
    """Process FitNotes CSV data."""
    print(f"Processing FitNotes data from: {filepath}")
    df = pd.read_csv(filepath)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    print(f"Processed {len(df)} rows of FitNotes data")
    return df

def refactor_checkmarks_df(df):
    """
    Refactor a wide Checkmarks DataFrame to long format with columns: date, habit, value.
    SQL will handle the id column.
    """
    # Clean up column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    # Remove unnamed columns (from trailing commas in CSV)
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]
    # Melt the DataFrame
    df_long = df.melt(id_vars=['date'], var_name='habit', value_name='value')
    return df_long

def save_sample_csv(df, filename, download_dir='files/downloads'):
    """Save a sample of the DataFrame to CSV for inspection."""
    sample_path = os.path.join(download_dir, f'sample_{filename}')
    df.head(20).to_csv(sample_path, index=False)
    print(f"Saved sample data to: {sample_path}")

def process_loop_habits_data(filepath):
    """Process Loop Habits ZIP data."""
    print(f"Processing Loop Habits data from: {filepath}")
    with zipfile.ZipFile(filepath) as zip_ref:
        # List all files in the ZIP
        file_list = zip_ref.namelist()
        print(f"Found {len(file_list)} files in ZIP archive")
        
        # Look for the master Checkmarks.csv file
        if 'Checkmarks.csv' not in file_list:
            raise ValueError("Master Checkmarks.csv file not found in ZIP archive")
        
        print("Found master Checkmarks.csv file")
        csv_data = zip_ref.read('Checkmarks.csv')
        df = pd.read_csv(io.BytesIO(csv_data))
        
        # Print original data info
        print(f"Found {len(df.columns)} columns in original Checkmarks.csv")
        print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
        
        # Convert to long format
        df_long = refactor_checkmarks_df(df)
        
        print(f"Processed {len(df_long)} rows of Loop Habits data")
        print(f"New format has {len(df_long.columns)} columns: {', '.join(df_long.columns)}")
        return df_long

def save_to_database(df, engine, table_name, filename):
    """Save DataFrame to MySQL database using SQLAlchemy."""
    print(f"Saving data to table: {table_name}")
    # Add metadata columns
    df['csv_filename'] = filename
    df['upload_dttm'] = datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d %H:%M:%S')

    # If the DataFrame has an 'id' column, rename it to 'csv_id' to avoid conflict
    if 'id' in df.columns:
        print("Renaming 'id' column in CSV to 'csv_id' to avoid SQL conflict.")
        df.rename(columns={'id': 'csv_id'}, inplace=True)

    # Create table if it doesn't exist
    with engine.connect() as conn:
        # Get column definitions from DataFrame
        columns = []
        for col in df.columns:
            columns.append(f"`{col}` VARCHAR(255)")

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(columns)}
        )
        """
        print("Creating table if it doesn't exist...")
        conn.execute(text(create_table_query))
        conn.commit()

    # Insert data
    print(f"Inserting {len(df)} rows into database...")
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print("Data successfully saved to database")

def send_confirmation_email(mail, email_id, success, filename, error_message=None):
    """Send confirmation email by replying to the original email."""
    print(f"Sending confirmation email for file: {filename}")
    
    # Get the original email to reply to
    _, msg_data = mail.fetch(email_id, '(RFC822)')
    email_body = msg_data[0][1]
    original_email = email.message_from_bytes(email_body)
    
    # Create reply message
    msg = MIMEMultipart()
    msg['From'] = os.getenv('GMAIL_USER')
    msg['To'] = original_email['from']
    msg['Subject'] = f"Re: {original_email['subject']}"
    
    # Add In-Reply-To and References headers for proper threading
    msg['In-Reply-To'] = original_email['message-id']
    msg['References'] = original_email['message-id']
    
    body = f"""
    File: {filename}
    Status: {'Successfully processed' if success else 'Failed to process'}
    """
    if error_message:
        body += f"\nError: {error_message}"
    
    msg.attach(MIMEText(body, 'plain'))
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(os.getenv('GMAIL_USER'), os.getenv('GMAIL_PASS'))
        server.send_message(msg)
    print("Confirmation email sent")

def mark_as_read(mail, email_id):
    """Mark email as read."""
    print(f"Marking email {email_id} as read")
    mail.store(email_id, '+FLAGS', '\\Seen')

def main():
    try:
        print("\n=== Starting data processing ===")
        # Initialize connections
        mail = get_gmail_connection()
        engine = get_db_connection()
        
        # Search for relevant emails
        email_ids = search_emails(mail)
        
        if not email_ids:
            print("No new FitNotes or Loop Habits emails found.")
            return
        
        for email_id in email_ids:
            attachments = get_attachment(mail, email_id)
            
            for attachment in attachments:
                try:
                    # Save attachment if new
                    filepath = save_attachment(attachment)
                    if not filepath:
                        print(f"File {attachment['filename']} already exists, skipping.")
                        continue
                    
                    # Process data based on file type
                    if 'FitNotes_Export_' in attachment['filename']:
                        df = process_fitnotes_data(filepath)
                        table_name = 'fitnotes_workouts'
                    else:  # Loop Habits
                        df = process_loop_habits_data(filepath)
                        table_name = 'loop_habits'
                    
                    # Save to database
                    save_to_database(df, engine, table_name, attachment['filename'])
                    
                    # Send success email
                    send_confirmation_email(mail, email_id, True, attachment['filename'])
                    print(f"Successfully processed {attachment['filename']}")
                    
                except Exception as e:
                    error_message = str(e)
                    print(f"Error processing {attachment['filename']}: {error_message}")
                    send_confirmation_email(mail, email_id, False, attachment['filename'], error_message)
            
            # Mark email as read
            mark_as_read(mail, email_id)
        
        mail.logout()
        print("\n=== Processing complete! ===\n")
        
    except Exception as e:
        print(f"\n=== An error occurred: {str(e)} ===\n")

if __name__ == '__main__':
    main() 