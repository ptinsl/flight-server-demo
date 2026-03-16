"""Vanilla Flight Server client demo.

Usage:
    # List all tables
    python scripts/vanilla_client_demo.py

    # Fetch a specific table
    python scripts/vanilla_client_demo.py --table prices
    python scripts/vanilla_client_demo.py --server grpc://your-server:8815 --table prices
"""

import argparse

import polars as pl
import pyarrow.flight as flight


def main():
    parser = argparse.ArgumentParser(description="Arrow Flight client demo")
    parser.add_argument("--server", default="grpc://localhost:8815", help="Flight server URI")
    parser.add_argument("--table", help="Table name to fetch (if not provided, lists tables)")
    args = parser.parse_args()

    # 1 - connect to server
    client = flight.connect(args.server)

    # 2 - list available datasets
    print(f"Available tables on {args.server}:")
    tables = [fl.descriptor.path[0].decode() for fl in client.list_flights()]
    for name in tables:
        print(f"  - {name}")
    print(f"\nTotal: {len(tables)} table(s)")

    # If --table specified, fetch and display it
    if args.table:
        print(f"\nFetching table '{args.table}'...")

        # 3 - stream dataset to client
        descriptor = flight.FlightDescriptor.for_path(args.table)
        info = client.get_flight_info(descriptor)
        ticket = info.endpoints[0].ticket
        table = client.do_get(ticket).read_all()

        print(f"Received {table.num_rows} rows, {table.num_columns} columns\n")

        # Display schema
        print("Schema:")
        for field in table.schema:
            print(f"  {field.name}: {field.type}")
        print()

        # 4 - load a Polars dataframe from the Arrow table
        df = pl.from_arrow(table)
        print(f"All {len(df)} rows:")
        print(df)


if __name__ == "__main__":
    main()