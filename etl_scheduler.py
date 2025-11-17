import schedule
import time
import subprocess
import logging
import sys  # <-- Import sys
from datetime import datetime

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

# Scripts to run (make sure they are in the same directory or provide full paths)
DATA_GENERATOR_SCRIPT = 'data_generator.py'
ETL_LOAD_SCRIPT = 'run_bronze_load.py'

# --- Job Functions ---

def run_script(script_name):
    """Helper function to run a Python script using subprocess."""
    logging.info(f"Attempting to run script: {script_name}...")
    try:
        # We use 'python3'. Change to 'python' if that's your command.
        # We use check=True to raise an error if the script fails (non-zero exit code)
        
        # --- CHANGE: Use sys.executable instead of 'python3' ---
        # This ensures we use the Python from our virtual environment
        result = subprocess.run(
            [sys.executable, script_name],  # <-- [sys.executable, script_name]
            check=True, 
            capture_output=True, 
            text=True
        )
        # --- END CHANGE ---
        
        logging.info(f"Successfully ran {script_name}.")
        logging.debug(f"--- {script_name} STDOUT ---\n{result.stdout}")
        logging.debug(f"--- {script_name} STDERR ---\n{result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"FAILED to run {script_name}. Exit Code: {e.returncode}")
        logging.error(f"--- {script_name} STDOUT ---\n{e.stdout}")
        logging.error(f"--- {script_name} STDERR ---\n{e.stderr}")
        return False
    except FileNotFoundError:
        # --- CHANGE: Updated error message ---
        logging.error(f"FAILED to run {script_name}. Executable not found: '{sys.executable}'")
        # --- END CHANGE ---
        return False
    except Exception as e:
        logging.error(f"An unexpected error occurred while running {script_name}: {e}")
        return False

def run_daily_pipeline():
    """
    The main scheduled job.
    1. Runs the data generator.
    2. If successful, runs the ETL load.
    """
    logging.info("=== STARTING DAILY ETL PIPELINE RUN ===")
    
    # --- Step 1: Generate Data ---
    generation_success = run_script(DATA_GENERATOR_SCRIPT)
    
    if not generation_success:
        logging.error("Data generation failed. Aborting ETL load.")
        logging.info("=== DAILY ETL PIPELINE RUN FAILED (at generation) ===")
        return # Stop the pipeline

    # --- Step 2: Run ETL Load ---
    logging.info("Data generation successful. Proceeding with ETL load.")
    etl_success = run_script(ETL_LOAD_SCRIPT)
    
    if etl_success:
        logging.info("=== DAILY ETL PIPELINE RUN FINISHED SUCCESSFULLY ===")
    else:
        logging.info("=== DAILY ETL PIPELINE RUN FAILED (at load) ===")

# --- Scheduler Setup ---

def main():
    logging.info("ETL Scheduler started.")
    logging.info("Waiting for the next scheduled run.")

    # --- FOR TESTING: Run the pipeline every 1 minute ---
    # Comment this out for production
    # schedule.every(1).minutes.do(run_daily_pipeline)
    
    # --- FOR PRODUCTION: Run the pipeline every 24 hours ---
    # You can set a specific time, e.g., at 3 AM
    schedule.every().day.at("06:00").do(run_daily_pipeline)
    
    # --- FOR SIMPLICITY: Run every 24 hours from now ---
    # Instead we went to running the scheduler at 6:00 AM daily
    #schedule.every(30).seconds.do(run_daily_pipeline)
    
    # --- Run immediately on start, then schedule ---
    # This is great for testing
    logging.info("Running one initial pipeline on startup...")
    run_daily_pipeline()
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main()