"""
Run a SQL file against the project's DuckDB database.

Usage:
    python run_sql.py sql/02_warehouse.sql
"""

import sys
from pathlib import Path

import duckdb


DB_PATH = "data/warehouse.duckdb"


def run_sql_file(sql_path):
    """Execute every statement in the SQL file and print any result rows."""
    sql_text = Path(sql_path).read_text()

    # DuckDB's execute runs one statement at a time. Split on semicolons,
    # strip empty chunks, then run each statement in order.
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]

    con = duckdb.connect(DB_PATH)

    print(f"Running {sql_path} ({len(statements)} statements)\n")

    for i, stmt in enumerate(statements, start=1):
        # Show the first line of each statement so output is readable
        first_line = stmt.splitlines()[0][:80]
        print(f"[{i}/{len(statements)}] {first_line}")

        result = con.execute(stmt)

        # If the statement returns rows (SELECT, etc.), print them
        try:
            rows = result.fetchall()
            columns = [desc[0] for desc in result.description] if result.description else []
            if rows:
                print(f"    Columns: {columns}")
                for row in rows:
                    print(f"    {row}")
        except duckdb.Error:
            # Not all statements produce rows (CREATE, DROP, etc.). That's fine.
            pass

        print()

    con.close()
    print("Done.")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_sql.py <path-to-sql-file>")
        sys.exit(1)

    run_sql_file(sys.argv[1])