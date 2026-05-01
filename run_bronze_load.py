import csv
import hashlib
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pyodbc
from dotenv import load_dotenv

from sqlserver_connection import get_connection


METADATA_COLUMNS = ["batch_id", "source_file", "source_row_number", "loaded_at", "row_hash"]

TABLES_TO_LOAD = [
    {
        "name": "crm_cust_info",
        "env_path": "PATH_CRM_CUST_INFO",
        "columns": [
            "cst_id", "cst_key", "cst_firstname", "cst_lastname",
            "cst_marital_status", "cst_gndr", "cst_create_date",
        ],
        "keys": ["cst_id"],
    },
    {
        "name": "crm_prd_info",
        "env_path": "PATH_CRM_PRD_INFO",
        "columns": [
            "prd_id", "prd_key", "prd_nm", "prd_cost", "prd_line",
            "prd_start_dt", "prd_end_dt",
        ],
        "keys": ["prd_id"],
    },
    {
        "name": "crm_sales_details",
        "env_path": "PATH_CRM_SALES_DETAILS",
        "columns": [
            "sls_ord_num", "sls_prd_key", "sls_cust_id", "sls_order_dt",
            "sls_ship_dt", "sls_due_dt", "sls_sales", "sls_quantity", "sls_price",
        ],
        "keys": ["sls_ord_num", "sls_prd_key"],
    },
    {
        "name": "erp_cust_az12",
        "env_path": "PATH_ERP_CUST_AZ12",
        "columns": ["CID", "BDATE", "GEN"],
        "keys": ["CID"],
    },
    {
        "name": "erp_loc_a101",
        "env_path": "PATH_ERP_LOC_A101",
        "columns": ["CID", "CNTRY"],
        "keys": ["CID"],
    },
    {
        "name": "erp_px_cat_g1v2",
        "env_path": "PATH_ERP_PX_CAT_G1V2",
        "columns": ["ID", "CAT", "SUBCAT", "MAINTENANCE"],
        "keys": ["ID"],
    },
]


def setup_logging():
    logger = logging.getLogger("bronze_etl")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler("bronze_load.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def require_config(logger):
    load_dotenv()

    paths = {}
    missing = []
    for table in TABLES_TO_LOAD:
        value = os.getenv(table["env_path"])
        if value:
            paths[table["name"]] = value
        else:
            missing.append(table["env_path"])

    if missing:
        logger.error("Missing required source path variables: %s", ", ".join(missing))
        sys.exit(1)

    return paths


def quote_name(name):
    return f"[{name}]"


def hash_row(row, columns):
    payload = "\x1f".join((row.get(column) or "").strip() for column in columns)
    return hashlib.sha256(payload.encode("utf-8")).digest()


def read_csv_rows(file_path, table_config, batch_id):
    source_file = str(Path(file_path).resolve())
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    columns = table_config["columns"]
    rows = []

    with open(file_path, "r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        for row_number, row in enumerate(reader, start=1):
            values = [row.get(column, "") for column in columns]
            values.extend([
                str(batch_id),
                source_file,
                row_number,
                loaded_at,
                pyodbc.Binary(hash_row(row, columns)),
            ])
            rows.append(values)

    return rows


def start_job(cursor, pipeline_name):
    job_id = str(uuid.uuid4())
    cursor.execute(
        """
        INSERT INTO ops.job_runs (job_run_id, pipeline_name, status, started_at, message)
        VALUES (?, ?, 'Running', SYSUTCDATETIME(), ?)
        """,
        job_id,
        pipeline_name,
        f"{pipeline_name} started.",
    )
    return job_id


def finish_job(cursor, job_id, status, message):
    cursor.execute(
        """
        UPDATE ops.job_runs
        SET
            status = ?,
            ended_at = SYSUTCDATETIME(),
            duration_sec = DATEDIFF_BIG(MILLISECOND, started_at, SYSUTCDATETIME()) / 1000.0,
            message = ?
        WHERE job_run_id = ?
        """,
        status,
        message,
        job_id,
    )


def start_task(cursor, job_id, task_name):
    cursor.execute(
        """
        INSERT INTO ops.task_runs (job_run_id, task_name, status, started_at)
        OUTPUT inserted.task_run_id
        VALUES (?, ?, 'Running', SYSUTCDATETIME())
        """,
        job_id,
        task_name,
    )
    return cursor.fetchone()[0]


def finish_task(cursor, task_run_id, status, rows_read, rows_inserted, rows_updated, rows_rejected, message):
    cursor.execute(
        """
        UPDATE ops.task_runs
        SET
            status = ?,
            ended_at = SYSUTCDATETIME(),
            duration_sec = DATEDIFF_BIG(MILLISECOND, started_at, SYSUTCDATETIME()) / 1000.0,
            rows_read = ?,
            rows_inserted = ?,
            rows_updated = ?,
            rows_rejected = ?,
            message = ?
        WHERE task_run_id = ?
        """,
        status,
        rows_read,
        rows_inserted,
        rows_updated,
        rows_rejected,
        message,
        task_run_id,
    )


def audit_source_file(cursor, job_id, table_name, file_path, row_count):
    path = Path(file_path)
    stat = path.stat()
    modified_at = datetime.fromtimestamp(stat.st_mtime, timezone.utc).replace(tzinfo=None)
    cursor.execute(
        """
        INSERT INTO ops.source_file_audit (
            job_run_id, source_name, source_file, file_size_bytes, row_count, file_modified_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        job_id,
        table_name,
        str(path.resolve()),
        stat.st_size,
        row_count,
        modified_at,
    )


def load_stage(cursor, table_config, rows):
    table_name = table_config["name"]
    columns = table_config["columns"] + METADATA_COLUMNS
    placeholders = ", ".join(["?"] * len(columns))
    column_list = ", ".join(quote_name(column) for column in columns)

    cursor.execute(f"TRUNCATE TABLE staging.{quote_name(table_name)}")
    cursor.fast_executemany = True
    cursor.executemany(
        f"INSERT INTO staging.{quote_name(table_name)} ({column_list}) VALUES ({placeholders})",
        rows,
    )


def merge_stage_to_bronze(cursor, table_config):
    table_name = table_config["name"]
    business_columns = table_config["columns"]
    all_columns = business_columns + METADATA_COLUMNS
    key_columns = table_config["keys"]

    partition_clause = ", ".join(quote_name(column) for column in key_columns)
    key_not_null = " AND ".join(f"{quote_name(column)} IS NOT NULL" for column in key_columns)
    join_clause = " AND ".join(f"tgt.{quote_name(column)} = src.{quote_name(column)}" for column in key_columns)
    update_clause = ", ".join(
        f"tgt.{quote_name(column)} = src.{quote_name(column)}"
        for column in all_columns
        if column not in key_columns
    )
    insert_columns = ", ".join(quote_name(column) for column in all_columns)
    select_columns = ", ".join(f"src.{quote_name(column)}" for column in all_columns)

    source_cte = f"""
        WITH src AS (
            SELECT
                {", ".join(quote_name(column) for column in all_columns)},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_clause}
                    ORDER BY source_row_number DESC
                ) AS rn
            FROM staging.{quote_name(table_name)}
            WHERE {key_not_null}
        )
    """

    update_sql = f"""
        {source_cte}
        UPDATE tgt
        SET {update_clause}
        FROM bronze.{quote_name(table_name)} AS tgt
        INNER JOIN src
            ON {join_clause}
        WHERE src.rn = 1;
    """
    cursor.execute(update_sql)
    rows_updated = cursor.rowcount if cursor.rowcount != -1 else 0

    insert_sql = f"""
        {source_cte}
        INSERT INTO bronze.{quote_name(table_name)} ({insert_columns})
        SELECT {select_columns}
        FROM src
        WHERE src.rn = 1
          AND NOT EXISTS (
              SELECT 1
              FROM bronze.{quote_name(table_name)} AS tgt
              WHERE {join_clause}
          );
    """
    cursor.execute(insert_sql)
    rows_inserted = cursor.rowcount if cursor.rowcount != -1 else 0

    rejected_sql = f"""
        SELECT COUNT(*)
        FROM staging.{quote_name(table_name)}
        WHERE NOT ({key_not_null})
    """
    cursor.execute(rejected_sql)
    rows_rejected = cursor.fetchone()[0]

    return rows_inserted, rows_updated, rows_rejected


def main():
    logger = setup_logging()
    paths = require_config(logger)
    batch_id = uuid.uuid4()
    job_id = None

    logger.info("Starting SQL Server bronze load. Batch ID: %s", batch_id)
    start_time = time.time()

    connection = None
    try:
        connection = get_connection()
        connection.autocommit = False
        cursor = connection.cursor()
        job_id = start_job(cursor, "bronze_load")
        connection.commit()

        for table_config in TABLES_TO_LOAD:
            table_name = table_config["name"]
            task_id = start_task(cursor, job_id, f"bronze.{table_name}")
            connection.commit()

            try:
                file_path = paths[table_name]
                rows = read_csv_rows(file_path, table_config, batch_id)
                logger.info("Read %s rows from %s.", len(rows), file_path)

                load_stage(cursor, table_config, rows)
                rows_inserted, rows_updated, rows_rejected = merge_stage_to_bronze(cursor, table_config)
                audit_source_file(cursor, job_id, table_name, file_path, len(rows))
                finish_task(
                    cursor,
                    task_id,
                    "Success",
                    len(rows),
                    rows_inserted,
                    rows_updated,
                    rows_rejected,
                    f"{table_name} loaded successfully.",
                )
                connection.commit()

                logger.info(
                    "%s complete. Inserted=%s Updated=%s Rejected=%s",
                    table_name,
                    rows_inserted,
                    rows_updated,
                    rows_rejected,
                )
            except Exception as exc:
                connection.rollback()
                cursor = connection.cursor()
                finish_task(cursor, task_id, "Failed", None, None, None, None, str(exc))
                finish_job(cursor, job_id, "Failed", f"Bronze load failed at {table_name}: {exc}")
                connection.commit()
                raise

        duration = time.time() - start_time
        finish_job(cursor, job_id, "Success", f"Bronze load completed in {duration:.2f} seconds.")
        connection.commit()
        logger.info("Bronze load completed in %.2f seconds.", duration)

    except Exception as exc:
        logger.error("Bronze load failed: %s", exc)
        sys.exit(1)
    finally:
        if connection:
            connection.close()


if __name__ == "__main__":
    main()
