import logging
import os
import subprocess
import sys
import time

import schedule


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scheduler.log"),
    ],
)

DATA_GENERATOR_SCRIPT = "data_generator.py"
BRONZE_LOAD_SCRIPT = "run_bronze_load.py"
SILVER_LOAD_SCRIPT = "run_silver_load.py"
GOLD_LOAD_SCRIPT = "run_gold_load.py"


def run_command(command, label):
    logging.info("Running %s: %s", label, " ".join(command))
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            env=os.environ.copy(),
        )
        if result.stdout:
            logging.info("%s stdout:\n%s", label, result.stdout)
        return True
    except subprocess.CalledProcessError as exc:
        logging.error("%s failed with exit code %s.", label, exc.returncode)
        if exc.stdout:
            logging.error("%s stdout:\n%s", label, exc.stdout)
        if exc.stderr:
            logging.error("%s stderr:\n%s", label, exc.stderr)
        return False


def run_python_script(script_name):
    return run_command([sys.executable, script_name], script_name)


def run_daily_pipeline():
    logging.info("=== STARTING DAILY SQL SERVER ETL PIPELINE ===")

    steps = [
        ("Generate source data", lambda: run_python_script(DATA_GENERATOR_SCRIPT)),
        ("Load bronze", lambda: run_python_script(BRONZE_LOAD_SCRIPT)),
        ("Load silver", lambda: run_python_script(SILVER_LOAD_SCRIPT)),
        ("Load gold", lambda: run_python_script(GOLD_LOAD_SCRIPT)),
    ]

    for step_name, step_callable in steps:
        logging.info("--- %s ---", step_name)
        if not step_callable():
            logging.error("=== PIPELINE FAILED at step: %s ===", step_name)
            return False

    logging.info("=== DAILY SQL SERVER ETL PIPELINE FINISHED SUCCESSFULLY ===")
    return True


def main():
    logging.info("ETL scheduler started.")

    if not os.getenv("SQLSERVER_PASSWORD"):
        logging.warning("SQLSERVER_PASSWORD is not set. Database steps will fail until it is configured.")

    run_on_startup = os.getenv("RUN_PIPELINE_ON_STARTUP", "true").lower() == "true"
    schedule_time = os.getenv("PIPELINE_SCHEDULE_TIME", "06:00")

    schedule.every().day.at(schedule_time).do(run_daily_pipeline)

    if run_on_startup:
        logging.info("Running initial pipeline on startup.")
        run_daily_pipeline()

    logging.info("Scheduler is active. Waiting for the next run at %s.", schedule_time)
    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
