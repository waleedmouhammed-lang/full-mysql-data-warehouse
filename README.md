# SQL Server Data Warehouse Project

This project is an end-to-end, portfolio-ready data warehouse pipeline built around SQL Server on Docker. It follows a medallion-style architecture with raw bronze ingestion, typed silver transformations, gold star-schema marts, operational metadata, Airflow orchestration, and dbt models/tests.

## Architecture

- **Runtime:** SQL Server in Docker.
- **Warehouse:** one database, `CustomerSales`.
- **Schemas:** `staging`, `bronze`, `silver`, `gold`, and `ops`.
- **Ingestion:** Python reads CSV files and batch-loads SQL Server staging tables with `pyodbc`.
- **Transformations:** dbt models build silver and gold layers.
- **Operations:** `ops` tables track job runs, task runs, source files, quality results, row counts, and alerts.
- **Orchestration:** Airflow DAG coordinates generation, bronze load, dbt run, and dbt tests.

## Key Files

- `00_create_warehouse_schema.sql`: creates `CustomerSales`, schemas, and the ETL database role.
- `00a_create_logging_utility.sql`: creates `ops` operational metadata tables.
- `01_create_bronze_tables.sql`: creates raw bronze tables with batch/source metadata.
- `02_create_staging_tables.sql`: creates transient staging tables for CSV batch loading.
- `03_create_silver_tables.sql`: optional SQL Server DDL for manually managed silver tables.
- `04_load_silver_layer.sql`: optional SQL Server fallback load for silver.
- `05_create_gold_tables.sql`: optional SQL Server DDL for manually managed gold tables.
- `06_load_gold_layer.sql`: optional SQL Server fallback load for gold.
- `run_bronze_load.py`: loads CSV files into `staging` and upserts into `bronze`.
- `dbt_project/`: dbt SQL Server models and tests for silver and gold.
- `orchestration/airflow/dags/data_warehouse_pipeline.py`: Airflow DAG.
- `docker-compose.yml`: SQL Server and Airflow services.

## Local Setup

1. Copy `.env.example` to `.env`.
2. Set `SQLSERVER_PASSWORD` to the same strong password used by SQL Server.
3. Start the stack:

```bash
docker compose up --build
```

4. Airflow is available at `http://localhost:8080` with `admin` / `admin`.

## Manual Setup Without Airflow

Run SQL setup scripts:

```bash
python run_sql_script.py 00_create_warehouse_schema.sql --database master --autocommit
python run_sql_script.py 00a_create_logging_utility.sql
python run_sql_script.py 01_create_bronze_tables.sql
python run_sql_script.py 02_create_staging_tables.sql
```

Run the pipeline:

```bash
python data_generator.py
python run_bronze_load.py
cd dbt_project
dbt run --select path:models/silver --profiles-dir .
dbt test --select path:models/silver --profiles-dir .
dbt run --select path:models/gold --profiles-dir .
dbt test --select path:models/gold --profiles-dir .
```

The legacy SQL Server fallback scripts can also be run with:

```bash
python run_silver_load.py
python run_gold_load.py
```

## Environment Variables

The project standardizes on SQL Server variables:

- `SQLSERVER_HOST`
- `SQLSERVER_PORT`
- `SQLSERVER_DATABASE`
- `SQLSERVER_USER`
- `SQLSERVER_PASSWORD`
- `SQLSERVER_DRIVER`
- `SQLSERVER_ENCRYPT`
- `SQLSERVER_TRUST_CERTIFICATE`

CSV paths are configured with:

- `PATH_CRM_CUST_INFO`
- `PATH_CRM_SALES_DETAILS`
- `PATH_CRM_PRD_INFO`
- `PATH_ERP_CUST_AZ12`
- `PATH_ERP_LOC_A101`
- `PATH_ERP_PX_CAT_G1V2`

## Production Notes

- Do not hardcode SQL Server passwords in SQL scripts.
- Prefer dbt models/tests for silver and gold in the Airflow path.
- Keep `03` through `06` as manual fallback scripts or migration references.
- Use `ops.job_runs` and `ops.task_runs` as the first monitoring surface.
- Add SMTP or Telegram delivery later by writing alert events from Airflow failures.
