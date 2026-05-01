from sqlserver_connection import get_connection


TABLES = [
    ("bronze", "crm_cust_info"),
    ("bronze", "crm_prd_info"),
    ("bronze", "crm_sales_details"),
    ("bronze", "erp_cust_az12"),
    ("bronze", "erp_loc_a101"),
    ("bronze", "erp_px_cat_g1v2"),
    ("silver", "crm_cust_info"),
    ("silver", "crm_prd_info"),
    ("silver", "crm_sales_details"),
    ("silver", "erp_cust_az12"),
    ("silver", "erp_loc_a101"),
    ("silver", "erp_px_cat_g1v2"),
    ("gold", "dim_customers"),
    ("gold", "dim_products"),
    ("gold", "fact_sales"),
]


def publish_table_counts():
    connection = get_connection()
    try:
        cursor = connection.cursor()
        for schema_name, table_name in TABLES:
            cursor.execute(f"SELECT COUNT_BIG(*) FROM [{schema_name}].[{table_name}]")
            row_count = cursor.fetchone()[0]
            cursor.execute(
                """
                INSERT INTO ops.table_row_counts (schema_name, table_name, row_count)
                VALUES (?, ?, ?)
                """,
                schema_name,
                table_name,
                row_count,
            )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


if __name__ == "__main__":
    publish_table_counts()
