import argparse
import re
from pathlib import Path

from sqlserver_connection import get_connection


GO_PATTERN = re.compile(r"^\s*GO(?:\s+\d+)?\s*(?:--.*)?$", re.IGNORECASE)


def split_sql_batches(sql_text):
    batches = []
    current = []

    for line in sql_text.splitlines():
        if GO_PATTERN.match(line):
            batch = "\n".join(current).strip()
            if batch:
                batches.append(batch)
            current = []
        else:
            current.append(line)

    batch = "\n".join(current).strip()
    if batch:
        batches.append(batch)

    return batches


def execute_sql_file(file_path, database=None, autocommit=False):
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    batches = split_sql_batches(path.read_text(encoding="utf-8"))

    connection = get_connection(database=database, autocommit=autocommit)
    try:
        cursor = connection.cursor()
        for batch in batches:
            cursor.execute(batch)
            while cursor.nextset():
                pass
        if not autocommit:
            connection.commit()
    except Exception:
        if not autocommit:
            connection.rollback()
        raise
    finally:
        connection.close()


def main():
    parser = argparse.ArgumentParser(description="Execute a SQL Server script with GO batch support.")
    parser.add_argument("script", help="Path to a .sql script")
    parser.add_argument("--database", default=None, help="Database name override")
    parser.add_argument("--autocommit", action="store_true", help="Use autocommit mode")
    args = parser.parse_args()

    execute_sql_file(args.script, database=args.database, autocommit=args.autocommit)
    print(f"Executed {args.script}")


if __name__ == "__main__":
    main()
