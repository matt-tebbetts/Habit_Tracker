import os
import base64
import re
from datetime import datetime
import pandas as pd
import mysql.connector
import imaplib
import email
from email.header import decode_header
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_gmail_connection():
    """Connect to Gmail using IMAP."""
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(os.getenv('GMAIL_USER'), os.getenv('GMAIL_PASS'))
    return mail

def get_db_connection():
    """Create and return MySQL database connection."""
    return mysql.connector.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )

def search_emails(mail):
    """Search for emails with FitNotes CSV attachments."""
    mail.select('inbox')
    _, messages = mail.search(None, '(UNSEEN SUBJECT "FitNotes Export")')
    return messages[0].split()

def get_attachment(mail, email_id):
    """Download attachment from Gmail."""
    _, msg_data = mail.fetch(email_id, '(RFC822)')
    email_body = msg_data[0][1]
    email_message = email.message_from_bytes(email_body)
    
    for part in email_message.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
            
        filename = part.get_filename()
        if filename and 'FitNotes_Export_' in filename and filename.endswith('.csv'):
            return part.get_payload(decode=True)
    return None

def process_csv_data(csv_data):
    """Process CSV data and return as pandas DataFrame."""
    return pd.read_csv(pd.io.common.BytesIO(csv_data))

def save_to_database(df, connection):
    """Save DataFrame to MySQL database."""
    cursor = connection.cursor()
    
    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fitnotes_workouts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        date DATETIME,
        exercise VARCHAR(255),
        category VARCHAR(255),
        weight FLOAT,
        reps INT,
        sets INT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_query)
    
    # Insert data
    for _, row in df.iterrows():
        insert_query = """
        INSERT INTO fitnotes_workouts 
        (date, exercise, category, weight, reps, sets, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        values = (
            row['Date'],
            row['Exercise'],
            row['Category'],
            row['Weight'],
            row['Reps'],
            row['Sets'],
            row.get('Notes', '')
        )
        cursor.execute(insert_query, values)
    
    connection.commit()
    cursor.close()

def mark_as_read(mail, email_id):
    """Mark email as read."""
    mail.store(email_id, '+FLAGS', '\\Seen')

def main():
    try:
        # Initialize Gmail connection
        mail = get_gmail_connection()
        
        # Search for relevant emails
        email_ids = search_emails(mail)
        
        if not email_ids:
            print("No new FitNotes export emails found.")
            return
        
        # Initialize database connection
        db_connection = get_db_connection()
        
        for email_id in email_ids:
            # Get attachment
            csv_data = get_attachment(mail, email_id)
            
            if csv_data:
                # Process and save data
                df = process_csv_data(csv_data)
                save_to_database(df, db_connection)
                
                # Mark email as read
                mark_as_read(mail, email_id)
                print(f"Processed attachment from email {email_id}")
        
        db_connection.close()
        mail.logout()
        print("Processing complete!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == '__main__':
    main() 