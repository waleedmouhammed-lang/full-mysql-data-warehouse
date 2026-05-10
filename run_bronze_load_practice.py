import csv
import os
from pathlib import Path

import pyodbc
from dotenv import load_dotenv


TABLES = [
    {
        "name": "crm_cust_info",
        "file": "datasets/source_crm/cust_info.csv",
        "columns": [
            "cst_id",
            "cst_key",
            "cst_firstname",
            "cst_lastname",
            "cst_marital_status",
            "cst_gndr",
            "cst_create_date",
        ],
        "keys": ["cst_id"],
        "order_by": "cst_create_date DESC",
    },
    {
        "name": "crm_prd_info",
        "file": "datasets/source_crm/prd_info.csv",
        "columns": [
            "prd_id",
            "prd_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt",
        ],
        "keys": ["prd_id"],
    },
    {
        "name": "crm_sales_details",
        "file": "datasets/source_crm/sales_details.csv",
        "columns": [
            "sls_ord_num",
            "sls_prd_key",
            "sls_cust_id",
            "sls_order_dt",
            "sls_ship_dt",
            "sls_due_dt",
            "sls_sales",
            "sls_quantity",
            "sls_price",
        ],
        "keys": ["sls_ord_num", "sls_prd_key"],
    },
    {
        "name": "erp_cust_az12",
        "file": "datasets/source_erp/CUST_AZ12.csv",
        "columns": ["CID", "BDATE", "GEN"],
        "keys": ["CID"],
    },
    {
        "name": "erp_loc_a101",
        "file": "datasets/source_erp/LOC_A101.csv",
        "columns": ["CID", "CNTRY"],
        "keys": ["CID"],
    },
    {
        "name": "erp_px_cat_g1v2",
        "file": "datasets/source_erp/PX_CAT_G1V2.csv",
        "columns": ["ID", "CAT", "SUBCAT", "MAINTENANCE"],
        "keys": ["ID"],
    },
]


def quote_name(name):
    return f"[{name}]"


def get_connection():
    load_dotenv()

    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    host = os.getenv("SQLSERVER_HOST", "localhost")
    port = os.getenv("SQLSERVER_PORT", "1433")
    database = os.getenv("SQLSERVER_DATABASE", "CustomerSales")
    user = os.getenv("SQLSERVER_USER", "sa")
    password = os.getenv("SQLSERVER_PASSWORD")
    encrypt = os.getenv("SQLSERVER_ENCRYPT", "yes")
    trust_certificate = os.getenv("SQLSERVER_TRUST_CERTIFICATE", "yes")

    if not password:
        raise RuntimeError("SQLSERVER_PASSWORD is missing from your .env file.")

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_certificate};"
        "Connection Timeout=30;"
    )

    return pyodbc.connect(connection_string)


def read_csv_file(file_path, columns):
    rows = []

    with open(file_path, "r", newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            rows.append([row.get(column, "") for column in columns])

    return rows


def load_staging_table(cursor, table):
    columns = table["columns"]
    table_name = table["name"]
    file_path = Path(__file__).resolve().parent / table["file"]
    rows = read_csv_file(file_path, columns)

    column_list = ", ".join(quote_name(column) for column in columns)
    placeholders = ", ".join("?" for _ in columns)

    cursor.execute(f"TRUNCATE TABLE staging.{quote_name(table_name)}")

    cursor.fast_executemany = True
    cursor.executemany(
        f"""
        INSERT INTO staging.{quote_name(table_name)} ({column_list})
        VALUES ({placeholders})
        """,
        rows,
    )

    return len(rows)


def upsert_bronze_table(cursor, table):
    table_name = table["name"]
    columns = table["columns"]
    keys = table["keys"]

    key_filter = " AND ".join(
        f"{quote_name(key)} IS NOT NULL AND LTRIM(RTRIM({quote_name(key)})) <> ''"
        for key in keys
    )
    join_condition = " AND ".join(
        f"target.{quote_name(key)} = source.{quote_name(key)}"
        for key in keys
    )
    update_columns = [column for column in columns if column not in keys]
    update_values = ", ".join(
        f"target.{quote_name(column)} = source.{quote_name(column)}"
        for column in update_columns
    )
    insert_columns = ", ".join(quote_name(column) for column in columns)
    select_columns = ", ".join(f"source.{quote_name(column)}" for column in columns)
    partition_by = ", ".join(quote_name(key) for key in keys)
    order_by = table.get("order_by") or ", ".join(quote_name(key) for key in keys)

    source_cte = f"""
        WITH source AS (
            SELECT
                {insert_columns},
                ROW_NUMBER() OVER (
                    PARTITION BY {partition_by}
                    ORDER BY {order_by}
                ) AS row_number
            FROM staging.{quote_name(table_name)}
            WHERE {key_filter}
        )
    """

    cursor.execute(
        f"""
        {source_cte}
        UPDATE target
        SET {update_values}
        FROM bronze.{quote_name(table_name)} AS target
        INNER JOIN source
            ON {join_condition}
        WHERE source.row_number = 1
        """
    )

    cursor.execute(
        f"""
        {source_cte}
        INSERT INTO bronze.{quote_name(table_name)} ({insert_columns})
        SELECT {select_columns}
        FROM source
        WHERE source.row_number = 1
          AND NOT EXISTS (
              SELECT 1
              FROM bronze.{quote_name(table_name)} AS target
              WHERE {join_condition}
          )
        """
    )


def main():
    connection = get_connection()

    try:
        cursor = connection.cursor()

        for table in TABLES:
            print(f"Loading {table['name']}...")
            rows_loaded = load_staging_table(cursor, table)
            upsert_bronze_table(cursor, table)
            print(f"Loaded {rows_loaded} rows into staging.{table['name']}.")

        connection.commit()
        print("Bronze practice load completed successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    main()
