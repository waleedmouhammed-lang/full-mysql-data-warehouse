import mysql.connector
import logging
import time
import sys
import os
from datetime import datetime
from dotenv import load_dotenv

# --- Configuration & Logging Setup ---

def setup_logging():
    """Configures logging to print to console and save to a file."""
    logger = logging.getLogger('bronze_etl')
    logger.setLevel(logging.INFO)
    
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    # Console Handler
    console_handler = logging.StreamHandler()
    console_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File Handler
    file_handler = logging.FileHandler('bronze_load.log')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger

def read_config(env_file='.env'):
    """Reads database and file path configuration from .env file."""
    logger = logging.getLogger('bronze_etl')
    
    if not load_dotenv(env_file):
        logger.error(f"CRITICAL: Environment file '{env_file}' not found.")
        sys.exit(1)
        
    logger.info(f"Reading configuration from {env_file}...")

    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_DATABASE')
    }

    paths_config = {
        'crm_cust_info': os.getenv('PATH_CRM_CUST_INFO'),
        'crm_prd_info': os.getenv('PATH_CRM_PRD_INFO'),
        'crm_sales_details': os.getenv('PATH_CRM_SALES_DETAILS'),
        'erp_cust_az12': os.getenv('PATH_ERP_CUST_AZ12'),
        'erp_loc_a101': os.getenv('PATH_ERP_LOC_A101'),
        'erp_px_cat_g1v2': os.getenv('PATH_ERP_PX_CAT_G1V2')
    }

    if not all(db_config.values()):
        logger.error("CRITICAL: One or more DB_... variables are missing from .env file.")
        sys.exit(1)
        
    if not all(paths_config.values()):
        logger.error("CRITICAL: One or more PATH_... variables are missing from .env file.")
        sys.exit(1)
        
    return db_config, paths_config

# --- Database Logging Functions ---

def log_etl_start(connection, process_name):
    """Inserts a new 'In Progress' record into etl_log and returns the log_id."""
    start_time = datetime.now()
    query = """
        INSERT INTO etl_log (process_name, start_time, status, log_message)
        VALUES (%s, %s, 'In Progress', 'Bronze load started.')
    """
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
    with connection.cursor() as cursor:
        cursor.execute(query, (end_time, duration, status, message, log_id))
        connection.commit()

# --- Main ETL Logic ---

def main():
    """
    Main ETL function for INCREMENTAL load to Bronze tables.
    Uses a staging table workflow for deduplication.
    """
    logger = setup_logging()
    db_config, paths = read_config()
    
    # UPDATED: We are changing this from a simple list of tuples to a
    # list of dictionaries. This provides the metadata (column names)
    # needed to dynamically build the UPSERT query.
    tables_to_load = [
        {
            'name': 'crm_cust_info',
            'path': paths.get('crm_cust_info'),
            'columns': [
                'cst_id', 'cst_key', 'cst_firstname', 'cst_lastname', 
                'cst_marital_status', 'cst_gndr', 'cst_create_date'
            ]
        },
        {
            'name': 'crm_prd_info',
            'path': paths.get('crm_prd_info'),
            'columns': [
                'prd_id', 'prd_key', 'prd_nm', 'prd_cost', 'prd_line', 
                'prd_start_dt', 'prd_end_dt'
            ]
        },
        {
            'name': 'crm_sales_details',
            'path': paths.get('crm_sales_details'),
            'columns': [
                'sls_ord_num', 'sls_prd_key', 'sls_cust_id', 'sls_order_dt',
                'sls_ship_dt', 'sls_due_dt', 'sls_sales', 'sls_quantity', 'sls_price'
            ]
        },
        {
            'name': 'erp_cust_az12',
            'path': paths.get('erp_cust_az12'),
            'columns': ['CID', 'BDATE', 'GEN']
        },
        {
            'name': 'erp_loc_a101',
            'path': paths.get('erp_loc_a101'),
            'columns': ['CID', 'CNTRY']
        },
        {
            'name': 'erp_px_cat_g1v2',
            'path': paths.get('erp_px_cat_g1v2'),
            'columns': ['ID', 'CAT', 'SUBCAT', 'MAINTENANCE']
        }
    ]

    connection = None
    log_id = None
    process_start_time = datetime.now()
    
    try:
        # --- Connect to Database ---
        logger.info(f"Connecting to database '{db_config['database']}' on {db_config['host']}...")
        connection = mysql.connector.connect(
            **db_config,
            allow_local_infile=True
        )
        
        if not connection.is_connected():
            logger.error("CRITICAL: Database connection failed.")
            sys.exit(1)
            
        logger.info("Database connection successful.")
        log_id, process_start_time = log_etl_start(connection, 'bronze_incremental_load')

        # --- Begin Main Load Process ---
        logger.info(f"Starting Bronze incremental load process (Log ID: {log_id})...")
        total_start_time = time.time()
        # This metric is no longer as simple as "new rows"
        total_rows_affected = 0

        with connection.cursor() as cursor:
            # UPDATED: The loop now iterates over the list of dictionaries
            for table_config in tables_to_load:
                table_start_time = time.time()
                
                table_name = table_config['name']
                file_path = table_config['path']
                columns = table_config['columns']
                
                # Define the corresponding staging table
                stg_table_name = f"stg_{table_name}"
                
                logger.info(f"Processing table: {table_name}...")

                # 1. TRUNCATE the STAGING table
                logger.debug(f"Truncating staging table: {stg_table_name}...")
                cursor.execute(f"TRUNCATE TABLE {stg_table_name}")
                
                # 2. LOAD DATA into the STAGING table
                logger.debug(f"Loading data from {file_path} into {stg_table_name}...")
                
                safe_file_path = os.path.normpath(file_path).replace('\\', '\\\\')
                load_query = f"""
                    LOAD DATA LOCAL INFILE '{safe_file_path}'
                    INTO TABLE {stg_table_name}
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\r\\n'
                    IGNORE 1 LINES
                """
                cursor.execute(load_query)
                rows_loaded_to_stage = cursor.rowcount
                logger.info(f"Loaded {rows_loaded_to_stage} rows from file into {stg_table_name}.")

                # 3. MERGE data from Staging to Final table (UPSERT)
                # This logic is CHANGED from INSERT IGNORE to UPSERT
                logger.debug(f"Upserting data from {stg_table_name} into {table_name}...")
                
                # Dynamically build the "ON DUPLICATE KEY UPDATE" clause
                # e.g., "cst_id = VALUES(cst_id), cst_key = VALUES(cst_key), ..."
                update_clause = ",\n".join([f"{col} = VALUES({col})" for col in columns])
                
                merge_query = f"""
                    INSERT INTO {table_name}
                    SELECT * FROM {stg_table_name}
                    ON DUPLICATE KEY UPDATE
                    {update_clause}
                """
                
                cursor.execute(merge_query)
                # In MySQL, rowcount is 1 for a new insert, 2 for an update.
                rows_affected = cursor.rowcount
                total_rows_affected += rows_affected
                
                # Commit after each *full* table workflow (Stage + Merge)
                connection.commit()
                
                table_duration = time.time() - table_start_time
                
                # UPDATED: Changed the log message to be more accurate
                logger.info(f"Successfully processed {table_name} in {table_duration:.2f}s. Rows affected (1=new, 2=update): {rows_affected}")

        # --- Log Success ---
        total_duration = time.time() - total_start_time
        # UPDATED: Changed the success message
        success_message = f"All {len(tables_to_load)} tables processed successfully (upsert) in {total_duration:.2f} seconds. Total rows affected: {total_rows_affected}."
        logger.info(success_message)
        log_etl_end(connection, log_id, process_start_time, 'Success', success_message)

    except mysql.connector.Error as err:
        error_message = f"MySQL Error: {err.errno} - {err.msg}"
        logger.error(f"ETL FAILED. {error_message}")
        if log_id and connection and connection.is_connected():
            log_etl_end(connection, log_id, process_start_time, 'Error', error_message)
        sys.exit(1)

    except Exception as e:
        error_message = f"Non-DB Error: {str(e)}"
        logger.error(f"ETL FAILED. {error_message}")
        if log_id and connection and connection.is_connected():
            log_etl_end(connection, log_id, process_start_time, 'Error', error_message)
        sys.exit(1)

    finally:
        if connection and connection.is_connected():
            connection.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()