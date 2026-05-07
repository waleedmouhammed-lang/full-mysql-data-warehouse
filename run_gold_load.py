import os
import sys

from run_sql_script import execute_sql_file


SQL_FILE_PATH = "06_load_gold_layer.sql"


def execute_gold_load():
    print("--- Starting Gold Layer Load ---")
    try:
        execute_sql_file(SQL_FILE_PATH, database=os.getenv("SQLSERVER_DATABASE", "CustomerSales"))
        print("--- Gold Layer Load Completed Successfully ---")
    except Exception as exc:
        print(f"Gold layer load failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    execute_gold_load()
