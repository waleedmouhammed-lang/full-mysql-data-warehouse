from datetime import datetime
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
import pyodbc


PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt_project"
PROFILES_DIR = DBT_DIR


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
                INSERT INTO ops.alerts (severity, channel, subject, message)
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
    description="Generate sources, load bronze, run dbt silver/gold, and validate the warehouse.",
    default_args=default_args,
    on_failure_callback=record_failure_alert,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["sqlserver", "warehouse", "dbt"],
) as dag:
    setup_database = BashOperator(
        task_id="setup_database",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python run_sql_script.py 00_create_warehouse_schema.sql --database master --autocommit && "
            "python run_sql_script.py 00a_create_logging_utility.sql && "
            "python run_sql_script.py 01_create_bronze_tables.sql && "
            "python run_sql_script.py 02_create_staging_tables.sql"
        ),
    )

    generate_source_data = BashOperator(
        task_id="generate_source_data",
        bash_command=f"cd {PROJECT_DIR} && python data_generator.py",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"cd {PROJECT_DIR} && python run_bronze_load.py",
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=f"cd {DBT_DIR} && dbt run --select path:models/silver --profiles-dir {PROFILES_DIR}",
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"cd {DBT_DIR} && dbt test --select path:models/silver --profiles-dir {PROFILES_DIR}",
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=f"cd {DBT_DIR} && dbt run --select path:models/gold --profiles-dir {PROFILES_DIR}",
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"cd {DBT_DIR} && dbt test --select path:models/gold --profiles-dir {PROFILES_DIR}",
    )

    publish_metrics = BashOperator(
        task_id="publish_metrics",
        bash_command=f"cd {PROJECT_DIR} && python publish_table_counts.py",
    )

    setup_database >> generate_source_data >> load_bronze
    load_bronze >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold >> publish_metrics
