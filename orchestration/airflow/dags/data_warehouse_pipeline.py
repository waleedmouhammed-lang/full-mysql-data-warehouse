from datetime import datetime
import os

from airflow import DAG
from airflow.utils.helpers import chain
from airflow.providers.standard.operators.bash import BashOperator
import pyodbc


PROJECT_DIR = "/opt/airflow/project"


default_args = {
    "owner": "data-engineering",
    "retries": 0,
}


def record_failure_alert(context):
    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    host = os.getenv("SQLSERVER_HOST", "sqlserver")
    port = os.getenv("SQLSERVER_PORT", "1433")
    database = os.getenv("SQLSERVER_DATABASE", "CustomerSales")
    user = os.getenv("SQLSERVER_USER", "sa")
    password = os.getenv("SQLSERVER_PASSWORD")

    if not password:
        return

    conn_str = (
        f"DRIVER={{{driver}}};SERVER={host},{port};DATABASE={database};"
        f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=yes;"
    )

    task_instance = context.get("task_instance")
    subject = f"Airflow failure: {context.get('dag').dag_id if context.get('dag') else 'unknown_dag'}"
    message = f"Task failed: {task_instance.task_id if task_instance else 'unknown_task'}"

    try:
        connection = pyodbc.connect(conn_str, autocommit=True)
        cursor = connection.cursor()
        cursor.execute(
            """
            IF OBJECT_ID(N'ops.alerts', N'U') IS NOT NULL
            BEGIN
                INSERT INTO ops.alerts (severity, channel, alert_subject, alert_message)
                VALUES ('Critical', 'airflow', ?, ?)
            END
            """,
            subject,
            message,
        )
        connection.close()
    except Exception as exc:
        print(f"Could not record failure alert: {exc}")


with DAG(
    dag_id="sqlserver_data_warehouse_pipeline",
    description="Load original CSV sources through staging, bronze, silver, and gold.",
    default_args=default_args,
    on_failure_callback=record_failure_alert,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["sqlserver", "warehouse"],
) as dag:
    setup_database = BashOperator(
        task_id="setup_database",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python run_sql_script.py 00_create_warehouse_schema.sql --database master --autocommit && "
            "python run_sql_script.py 00a_create_logging_utility.sql && "
            "python run_sql_script.py 01_create_bronze_tables.sql && "
            "python run_sql_script.py 02_create_staging_tables.sql && "
            "python run_sql_script.py 03_create_silver_tables.sql && "
            "python run_sql_script.py 05_create_gold_tables.sql"
        ),
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {PROJECT_DIR} && python run_bronze_load.py",
    )

    load_silver = BashOperator(
        task_id="load_silver",
        bash_command=f"cd {PROJECT_DIR} && python run_silver_load.py",
    )

    load_gold = BashOperator(
        task_id="load_gold",
        bash_command=f"cd {PROJECT_DIR} && python run_gold_load.py",
    )

    publish_metrics = BashOperator(
        task_id="publish_metrics",
        bash_command=f"cd {PROJECT_DIR} && python publish_table_counts.py",
    )

    chain(setup_database, load_bronze, load_silver, load_gold, publish_metrics)
