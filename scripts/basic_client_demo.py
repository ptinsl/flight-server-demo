"""Arrow Flight client demo.

Usage:
    # List all tables
    python scripts/client_demo.py

    # Fetch a specific table
    python scripts/client_demo.py --table prices
    python scripts/client_demo.py --server grpc://your-server:8081 --table prices
"""

import argparse

import polars as pl

from flight_server.client import FlightClientHelper


def main():
    parser = argparse.ArgumentParser(description="Arrow Flight client demo")
    parser.add_argument("--server", default="grpc://localhost:8081", help="Flight server URI")
    parser.add_argument("--table", help="Table name to fetch (if not provided, lists tables)")
    args = parser.parse_args()

    client = FlightClientHelper(args.server)

    # Always list tables first
    print(f"Available tables on {args.server}:")
    tables = client.list_tables()
    for table in tables:
        print(f"  - {table}")
    print(f"\nTotal: {len(tables)} table(s)")

    # If --table specified, fetch and display it
    if args.table:
        print(f"\nFetching table '{args.table}'...")
        table = client.get_table(args.table)
        print(f"Received {table.num_rows} rows, {table.num_columns} columns\n")

        # Display schema
        print("Schema:")
        for field in table.schema:
            print(f"  {field.name}: {field.type}")
        print()

        # Display data
        df = pl.from_arrow(table)
        print(f"All {len(df)} rows:")
        print(df)


if __name__ == "__main__":
    main()