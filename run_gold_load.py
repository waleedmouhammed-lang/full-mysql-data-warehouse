import mysql.connector
from mysql.connector import Error
import os
import sys

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'database': os.getenv('DB_DATABASEGO', 'dw_gold'), # Default to dw_gold
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

SQL_FILE_PATH = '06_load_gold_layer.sql'

def read_sql_file(file_path):
    """Reads the SQL file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file {file_path} does not exist.")
    with open(file_path, 'r') as file:
        return file.read()

def execute_gold_load():
    """
    Executes the Gold Layer Load with Atomic Transaction handling.
    """
    # Validation
    if not DB_CONFIG['user'] or not DB_CONFIG['password']:
        print("ERROR: Database credentials not found.")
        sys.exit(1)

    connection = None
    cursor = None
    
    print("--- Starting Gold Layer Load (All-or-Nothing) ---")
    
    try:
        # 1. Connect
        connection = mysql.connector.connect(**DB_CONFIG)
        
        if connection.is_connected():
            # CRITICAL: Disable Autocommit to start a transaction
            connection.autocommit = False
            
            cursor = connection.cursor()
            print(f"Connected to MySQL. Transaction Started.")

            # 2. Read Script
            print(f"Reading SQL file: {SQL_FILE_PATH}")
            sql_script = read_sql_file(SQL_FILE_PATH)

            # 3. Execute Script
            # multi=True yields an iterator of cursors for each statement
            print("Executing Gold Layer Logic...")
            for result in cursor.execute(sql_script, multi=True):
                if result.with_rows:
                    print(f"Query executed (Rows returned: {result.rowcount})")
                else:
                    print(f"Update/Insert executed (Rows affected: {result.rowcount})")
            
            # 4. Commit
            # If we reach this line, no errors occurred. We save the changes.
            connection.commit()
            print("--- Gold Layer Load COMMITTED Successfully ---")

    except Error as e:
        # 5. Rollback
        # If ANY error occurs, we undo pending changes (Inserts).
        # Note: TRUNCATE cannot be rolled back in MySQL, but Inserts will be.
        print(f"CRITICAL ERROR: {e}")
        if connection and connection.is_connected():
            connection.rollback()
            print("--- Transaction ROLLED BACK ---")
        sys.exit(1) # Return error code so Scheduler knows it failed
            
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

if __name__ == "__main__":
    execute_gold_load()