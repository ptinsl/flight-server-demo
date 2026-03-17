"""DuckDB Airport client demo.

Connects to the Airport Flight server via DuckDB's ATTACH command,
which gives full SQL access to the remote Delta tables.

Usage:
    # Run queries against a local Airport server
    python scripts/airport_client_demo.py

    # Run against a remote server
    python scripts/airport_client_demo.py --server grpc://your-server:8815

    # Run a custom query
    python scripts/airport_client_demo.py --query "SELECT Symbol, AVG(Close) FROM db.default.prices GROUP BY Symbol"
"""

import argparse

import duckdb
import polars as pl


def main():
    parser = argparse.ArgumentParser(description="DuckDB Airport client demo")
    parser.add_argument("--server", default="grpc://localhost:8815", help="Airport Flight server URI")
    parser.add_argument("--query", help="Custom SQL query to run (optional)")
    args = parser.parse_args()

    conn = duckdb.connect()
    conn.execute("INSTALL airport; LOAD airport;")
    conn.execute(f"ATTACH '{args.server}' AS db (TYPE AIRPORT);")

    print(f"Connected to {args.server}\n")

    # List all tables
    print("Tables in catalog:")
    tables = conn.execute("SHOW ALL TABLES;").fetchall()
    for row in tables:
        print(f"  {row[0]}.{row[1]}.{row[2]}  ({len(row[3])} columns)")
    print(f"\nTotal: {len(tables)} table(s)\n")

    if not tables:
        print("No tables found — is the server running with Azure credentials set?")
        return

    # Run custom query or demo queries against the first discovered table
    if args.query:
        print(f"Query: {args.query}")
        print(pl.from_arrow(conn.execute(args.query).arrow()))
    else:
        # Pick the first table for demo queries
        catalog, schema, table = tables[0][0], tables[0][1], tables[0][2]
        full_name = f"{catalog}.{schema}.{table}"

        print(f"Demo queries on {full_name}")
        print("-" * 60)

        # Row count
        count = conn.execute(f"SELECT COUNT(*) FROM {full_name};").fetchone()[0]
        print(f"Row count:  {count:,}")

        # Schema
        print("\nSchema:")
        schema_rows = conn.execute(f"DESCRIBE {full_name};").fetchall()
        for col_name, col_type, *_ in schema_rows:
            print(f"  {col_name}: {col_type}")

        # Sample rows
        print("\nFirst 5 rows:")
        print(pl.from_arrow(conn.execute(f"SELECT * FROM {full_name} LIMIT 5;").arrow()))

        # Filtered query (predicate pushdown)
        print("\nFiltered query (predicate pushdown):")
        sample_col = schema_rows[0][0]
        sample_val = conn.execute(f"SELECT {sample_col} FROM {full_name} LIMIT 1;").fetchone()[0]
        filtered = conn.execute(
            f"SELECT COUNT(*) FROM {full_name} WHERE {sample_col} = ?;", [sample_val]
        ).fetchone()[0]
        print(f"  WHERE {sample_col} = {sample_val!r} → {filtered:,} rows")


if __name__ == "__main__":
    main()
