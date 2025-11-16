import mysql.connector
import configparser
import logging
import time
import sys
from datetime import datetime

# --- Configuration & Logging Setup ---

def setup_logging():
    """Configures logging to print to console and save to a file."""
    # We create a logger named 'bronze_etl'.
    logger = logging.getLogger('bronze_etl')
    logger.setLevel(logging.INFO)
    
    # Stop logs from propagating to the root logger
    logger.propagate = False

    # Prevent duplicate handlers if script is re-run in same session
    if logger.hasHandlers():
        logger.handlers.clear()

    # Console Handler (for real-time feedback)
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File Handler (for persistent history)
    file_handler = logging.FileHandler('bronze_load.log')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger

def read_config(filename='config.ini'):
    """Reads database and file path configuration from config.ini."""
    logger = logging.getLogger('bronze_etl')
    logger.info(f"Reading configuration from {filename}...")
    config = configparser.ConfigParser()
    if not config.read(filename):
        logger.error(f"CRITICAL: Configuration file '{filename}' not found.")
        sys.exit(1)
    
    # Check for all required sections and keys
    if 'mysql' not in config or 'etl_paths' not in config:
        logger.error("CRITICAL: Config file must contain [mysql] and [etl_paths] sections.")
        sys.exit(1)
        
    return config

# --- Database Logging Functions ---

def log_etl_start(connection, process_name):
    """Inserts a new 'In Progress' record into etl_log and returns the log_id."""
    start_time = datetime.now()
    query = """
        INSERT INTO etl_log (process_name, start_time, status, log_message)
        VALUES (%s, %s, 'In Progress', 'Bronze load started.')
    """
    # Create a cursor, execute, commit, and close.
    with connection.cursor() as cursor:
        cursor.execute(query, (process_name, start_time))
        connection.commit()
        return cursor.lastrowid, start_time

def log_etl_end(connection, log_id, start_time, status, message):
    """Updates the etl_log record with the final status, end time, and duration."""
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    query = """
        UPDATE etl_log
        SET end_time = %s, duration_sec = %s, status = %s, log_message = %s
        WHERE log_id = %s
    """
    # Create a cursor, execute, commit, and close.
    with connection.cursor() as cursor:
        cursor.execute(query, (end_time, duration, status, message, log_id))
        connection.commit()

# --- Main ETL Logic ---

def main():
    """
    Main ETL function to truncate and load all Bronze tables.
    Implements try-catch logic and logs performance for each table.
    """
    logger = setup_logging()
    config = read_config()
    
    db_config = config['mysql']
    paths = config['etl_paths']
    
    # This list defines all the tables and their corresponding files
    tables_to_load = [
        ('crm_cust_info', paths.get('crm_cust_info')),
        ('crm_prd_info', paths.get('crm_prd_info')),
        ('crm_sales_details', paths.get('crm_sales_details')),
        ('erp_cust_az12', paths.get('erp_cust_az12')),
        ('erp_loc_a101', paths.get('erp_loc_a101')),
        ('erp_px_cat_g1v2', paths.get('erp_px_cat_g1v2'))
    ]

    # Check if all paths are defined in config
    if any(path is None for _, path in tables_to_load):
        logger.error("CRITICAL: One or more file paths are missing in config.ini [etl_paths] section.")
        sys.exit(1)

    connection = None
    log_id = None
    process_start_time = datetime.now() # Used even if DB log fails
    
    try:
        # --- Connect to Database ---
        logger.info(f"Connecting to database '{db_config['database']}' on {db_config['host']}...")
        connection = mysql.connector.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            database=db_config['database'],
            allow_local_infile=True  # CRITICAL: This enables LOAD DATA LOCAL INFILE
        )
        
        if not connection.is_connected():
            logger.error("CRITICAL: Database connection failed.")
            sys.exit(1)
            
        logger.info("Database connection successful.")
        
        # --- Log Start to DB Table ---
        # Pass the whole connection object to the logging function
        log_id, process_start_time = log_etl_start(connection, 'bronze_load_python')

        # --- Begin Main Load Process ---
        logger.info(f"Starting Bronze load process (Log ID: {log_id})...")
        total_start_time = time.time()

        # Create a single cursor for the main loop
        with connection.cursor() as cursor:
            for table_name, file_path in tables_to_load:
                table_start_time = time.time()
                logger.info(f"Processing table: {table_name}...")

                # 1. TRUNCATE table
                logger.debug(f"Truncating {table_name}...")
                cursor.execute(f"TRUNCATE TABLE {table_name}")
                
                # 2. LOAD DATA
                logger.debug(f"Loading data from {file_path} into {table_name}...")
                
                # Use os.path.normpath and replace backslashes for Windows compatibility
                import os
                safe_file_path = os.path.normpath(file_path).replace('\\', '\\\\')

                load_query = f"""
                    LOAD DATA LOCAL INFILE '{safe_file_path}'
                    INTO TABLE {table_name}
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\r\\n'
                    IGNORE 1 LINES
                """
                cursor.execute(load_query)
                
                # Commit after each table load
                connection.commit()
                
                table_duration = time.time() - table_start_time
                logger.info(f"Successfully loaded {cursor.rowcount} rows into {table_name} in {table_duration:.2f} seconds.")

        # --- Log Success ---
        total_duration = time.time() - total_start_time
        success_message = f"All {len(tables_to_load)} tables loaded successfully in {total_duration:.2f} seconds."
        logger.info(success_message)
        log_etl_end(connection, log_id, process_start_time, 'Success', success_message)

    except mysql.connector.Error as err:
        # --- "CATCH" Block ---
        # This catches any database-related error
        error_message = f"MySQL Error: {err.errno} - {err.msg}"
        logger.error(f"ETL FAILED. {error_message}")
        
        if log_id and connection and connection.is_connected():
            log_etl_end(connection, log_id, process_start_time, 'Error', error_message)
        sys.exit(1) # Exit with an error code

    except Exception as e:
        # --- General "CATCH" Block ---
        # This catches any other error (e.g., config file issue, Python error)
        error_message = f"Non-DB Error: {str(e)}"
        logger.error(f"ETL FAILED. {error_message}")
        
        if log_id and connection and connection.is_connected():
            log_etl_end(connection, log_id, process_start_time, 'Error', error_message)
        sys.exit(1) # Exit with an error code

    finally:
        # --- "FINALLY" Block (Cleanup) ---
        if connection and connection.is_connected():
            # No need to close cursor if using 'with' block, but doesn't hurt
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()