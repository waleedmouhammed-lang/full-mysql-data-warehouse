import schedule
import time
import subprocess
import logging
import sys
from datetime import datetime
import os

# --- Configuration ---
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scheduler.log')
    ]
)

# Scripts to run
DATA_GENERATOR_SCRIPT = 'data_generator.py'
BRONZE_LOAD_SCRIPT = 'run_bronze_load.py'   # Renamed for clarity
SILVER_LOAD_SCRIPT = 'run_silver_load.py'   # Added Silver script

# --- Job Functions ---

def run_script(script_name):
    """Helper function to run a Python script using subprocess."""
    logging.info(f"Attempting to run script: {script_name}...")
    try:
        # Use sys.executable to ensure we use the same Python environment
        # We pass os.environ to ensure the subprocess inherits environment variables (DB Creds)
        result = subprocess.run(
            [sys.executable, script_name], 
            check=True, 
            capture_output=True, 
            text=True,
            env=os.environ.copy() # Explicitly pass environment variables
        )
        
        logging.info(f"Successfully ran {script_name}.")
        logging.debug(f"--- {script_name} STDOUT ---\n{result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logging.error(f"Script {script_name} failed with exit code {e.returncode}.")
        logging.debug(f"--- {script_name} STDERR ---\n{e.stderr}")
        if e.stdout:
            logging.debug(f"--- {script_name} STDOUT (Partial) ---\n{e.stdout}")
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

def run_daily_pipeline():
    """
    Orchestrates the execution of the daily ETL pipeline.
    Sequence: Generate Data -> Load Bronze -> Load Silver
    """
    logging.info("=== STARTING DAILY ETL PIPELINE ===")
    
    # --- Step 1: Data Generation ---
    logging.info("--- Step 1: Running Data Generator ---")
    gen_success = run_script(DATA_GENERATOR_SCRIPT)
    
    if not gen_success:
        logging.error("Data generation failed. Aborting pipeline.")
        logging.info("=== DAILY ETL PIPELINE FAILED (at Step 1: Generation) ===")
        return

    # --- Step 2: Bronze Load ---
    logging.info("--- Step 2: Running Bronze Load ---")
    bronze_success = run_script(BRONZE_LOAD_SCRIPT)
    
    if not bronze_success:
        logging.error("Bronze load failed. Aborting pipeline.")
        logging.info("=== DAILY ETL PIPELINE FAILED (at Step 2: Bronze Load) ===")
        return

    # --- Step 3: Silver Load ---
    logging.info("--- Step 3: Running Silver Load ---")
    silver_success = run_script(SILVER_LOAD_SCRIPT)

    if silver_success:
        logging.info("=== DAILY ETL PIPELINE FINISHED SUCCESSFULLY ===")
    else:
        logging.error("Silver load failed.")
        logging.info("=== DAILY ETL PIPELINE FAILED (at Step 3: Silver Load) ===")

# --- Scheduler Setup ---

def main():
    logging.info("ETL Scheduler started.")
    
    # Validation: Check if DB credentials are present in environment
    if not os.getenv('DB_USER') or not os.getenv('DB_PASSWORD'):
        logging.warning("WARNING: MYSQL_USER or MYSQL_PASSWORD not found in environment variables.")
        logging.warning("The Silver Load script may fail if credentials are not set.")

    # --- FOR TESTING: Run the pipeline every 1 minute ---
    # Comment this out for production
    # schedule.every(1).minutes.do(run_daily_pipeline)

    # --- Schedule Configuration ---
    # Run daily at 06:00 AM
    schedule.every().day.at("06:00").do(run_daily_pipeline)
    
    # --- FOR TESTING: Run immediately on startup ---
    logging.info("Running initial pipeline test on startup...")
    run_daily_pipeline()
    
    logging.info("Scheduler is active. Waiting for next scheduled run...")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()