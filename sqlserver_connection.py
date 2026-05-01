import os
from contextlib import contextmanager

import pyodbc
from dotenv import load_dotenv


def build_connection_string(database=None):
    load_dotenv()

    driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 18 for SQL Server")
    host = os.getenv("SQLSERVER_HOST", "localhost")
    port = os.getenv("SQLSERVER_PORT", "1433")
    db_name = database or os.getenv("SQLSERVER_DATABASE", "DataWarehouse")
    user = os.getenv("SQLSERVER_USER", "sa")
    password = os.getenv("SQLSERVER_PASSWORD")
    encrypt = os.getenv("SQLSERVER_ENCRYPT", "yes")
    trust_cert = os.getenv("SQLSERVER_TRUST_CERTIFICATE", "yes")

    if not password:
        raise RuntimeError("SQLSERVER_PASSWORD is required.")

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={host},{port};"
        f"DATABASE={db_name};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt={encrypt};"
        f"TrustServerCertificate={trust_cert};"
        "Connection Timeout=30;"
    )


def get_connection(database=None, autocommit=False):
    return pyodbc.connect(build_connection_string(database=database), autocommit=autocommit)


@contextmanager
def sqlserver_connection(database=None, autocommit=False):
    connection = get_connection(database=database, autocommit=autocommit)
    try:
        yield connection
    finally:
        connection.close()
