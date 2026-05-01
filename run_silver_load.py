import os
import sys

from run_sql_script import execute_sql_file


SQL_FILE_PATH = "04_load_silver_layer.sql"


def execute_silver_load():
    print("--- Starting Silver Layer Load ---")
    try:
        execute_sql_file(SQL_FILE_PATH, database=os.getenv("SQLSERVER_DATABASE", "DataWarehouse"))
        print("--- Silver Layer Load Completed Successfully ---")
    except Exception as exc:
        print(f"Silver layer load failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    execute_silver_load()
