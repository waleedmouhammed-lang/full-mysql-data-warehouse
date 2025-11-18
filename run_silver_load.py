import mysql.connector
from mysql.connector import Error
import os
import sys

# Database Configuration
# SECURITY UPDATE: Credentials are now fetched from Environment Variables.
# This prevents passwords from being stored in plain text in the code.
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_DATABASESI'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

SQL_FILE_PATH = '04_load_silver_layer.sql'

def read_sql_file(file_path):
    """
    Reads the SQL file and cleans it for execution.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    with open(file_path, 'r') as file:
        sql_script = file.read()
    return sql_script

def execute_silver_load():
    """
    Connects to the database and executes the Silver Layer transformation script.
    """
    # Validation: Ensure credentials exist before trying to connect
    if not DB_CONFIG['user'] or not DB_CONFIG['password']:
        print("ERROR: Database credentials not found.")
        print("Please set the environment variables: MYSQL_USER and MYSQL_PASSWORD")
        sys.exit(1)

    connection = None
    cursor = None
    
    print("--- Starting Silver Layer Load ---")
    
    try:
        # 1. Establish Connection
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if connection.is_connected():
            db_info = connection.get_server_info()
            print(f"Connected to MySQL Server version {db_info}")
            cursor = connection.cursor()

            # 2. Read the SQL Script
            print(f"Reading SQL file: {SQL_FILE_PATH}")
            sql_script = read_sql_file(SQL_FILE_PATH)

            # 3. Execute the Script
            # The 'multi=True' option allows executing multiple statements in one call
            print("Executing transformation logic...")
            
            # We iterate through the results to ensure all statements execute
            for result in cursor.execute(sql_script, multi=True):
                if result.with_rows:
                    print(f"Executed query returning {result.rowcount} rows.")
                else:
                    print(f"Executed update/insert affecting {result.rowcount} rows.")
            
            # 4. Commit the transaction
            connection.commit()
            print("--- Silver Layer Load Completed Successfully ---")

    except Error as e:
        print(f"Error while connecting to MySQL or executing script: {e}")
        if connection and connection.is_connected():
            connection.rollback() # Rollback changes on error
            print("Transaction rolled back.")
            
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    execute_silver_load()