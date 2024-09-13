"Python script that extracts email addresses and dates from a log file, transforms the data, and saves it into both MongoDB and MySQL databases"

import re
from datetime import datetime
from pymongo import MongoClient
import mysql.connector

# Task 1: Extract Email Addresses and Dates
def extract_emails_and_dates(file_path):
    email_date_pairs = []
    with open(file_path, 'r') as file:
        lines = file.readlines()

    email_regex = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b')
    date_regex = re.compile(r'^Date:\s*(.*)\s\(')  # Adjusted regex to capture date before the parenthesis

    current_date = None
    for line in lines:
        # Find date
        date_match = date_regex.search(line)
        if date_match:
            date_str = date_match.group(1).strip()
            try:
                current_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S %z')  # Updated format
            except ValueError:
                print(f"Error parsing date: {date_str}")
                continue

        # Find email
        email_match = email_regex.search(line)
        if email_match and current_date:
            email_date_pairs.append((email_match.group(), current_date.strftime('%Y-%m-%d %H:%M:%S')))

    return email_date_pairs

# Task 2: Data Transformation
def transform_data(email_date_pairs):
    return [{'email': email, 'date': date} for email, date in email_date_pairs]

# Task 3: Save Data to MongoDB
def save_to_mongodb(data):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['log_data']
    collection = db['user_history']
    collection.insert_many(data)
    print(f'{len(data)} records inserted into MongoDB.')

# Task 4: Save Data to MySQL
def save_to_mysql():
    # Set up MySQL connection
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="raju10ajay11",
        database="log_data",
        connection_timeout=600,  # Increased timeout to handle large data inserts
        autocommit=True
    )
    cursor = conn.cursor()

    # Create the user_history table if it doesn't exist, with unique constraints on email and date
    cursor.execute('''CREATE TABLE IF NOT EXISTS user_history (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        email VARCHAR(255) NOT NULL,
                        date DATETIME NOT NULL,
                        UNIQUE(email, date)
                      )''')

    # Fetch data from MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['log_data']
    collection = db['user_history']
    records = list(collection.find({}, {'_id': 0}))

    # Insert data in batches to avoid overwhelming MySQL
    batch_size = 1000  # Batch size to avoid large insert operations
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        data_to_insert = [(record['email'], record['date']) for record in batch]
        
        try:
            # Insert data, ignoring duplicates
            cursor.executemany("INSERT IGNORE INTO user_history (email, date) VALUES (%s, %s)", data_to_insert)
            conn.commit()
            print(f'{cursor.rowcount} records inserted in this batch (ignoring duplicates).')
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    # Close MySQL connection and MongoDB client
    conn.close()
    client.close()

if __name__ == "__main__":
    file_path = 'C:/Users/AJAY/Downloads/mbox.txt'
    
    # Task 1: Extract Email Addresses and Dates
    email_date_pairs = extract_emails_and_dates(file_path)

    # Task 2: Data Transformation
    transformed_data = transform_data(email_date_pairs)

    # Task 3: Save to MongoDB
    save_to_mongodb(transformed_data)

    # Task 4: Save to MySQL
    try:
        save_to_mysql()
        print("Data inserted successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")
