# Running The SQL Server Pipeline

Airflow is the preferred orchestrator for this version of the project.

## Start The Docker Stack

```bash
cp .env.example .env
docker compose up --build
```

Airflow runs at:

```text
http://localhost:8080
```

Default local credentials:

```text
admin / admin
```

The DAG is named:

```text
sqlserver_data_warehouse_pipeline
```

## Manual Scheduler Fallback

The lightweight Python scheduler still exists for local experiments:

```bash
python etl_scheduler.py
```

It runs:

1. `data_generator.py`
2. `run_bronze_load.py`
3. `run_silver_load.py`
4. `run_gold_load.py`

For production-style orchestration, use Airflow and dbt instead of the fallback scheduler.
