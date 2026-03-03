# Apache Flight Server Demo

Read-only Apache Arrow Flight server for streaming Delta Lake tables and CSV files from Azure Blob Storage. Provides high-performance columnar data access via the standard Flight protocol.

## Architecture
![alt text](docs/images/architecture.png)
## Prerequisites

- Python 3.12+
- Azure Storage Account with connection string
- Delta tables without deletion vectors
- Paths using `abfss://` protocol

## Installation

```bash
pip install .
```

## Configuration

```bash
cp .env.example .env
# Edit .env with your Azure credentials
source .env
```

Required variables:
- `AZURE_CONNECTION_STRING` - Azure storage connection string
- `AZURE_CONTAINER` - Container name
- `AZURE_PREFIX` - Optional path prefix

## Usage

### Start the server

```bash
basic-flight-server
```

Server runs on `grpc://0.0.0.0:8081` (configurable via env vars `FLIGHT_HOST` and `FLIGHT_PORT`) and auto-discovers Delta tables (via `_delta_log` directories) and CSV files. Table names are derived from paths (e.g., `data/prices/` → `data_prices`).

### Query from the demo client

The client can run from anywhere with network access to the server:

```bash
# List all tables
python scripts/client_demo.py --server grpc://your-server:8081

# Fetch a specific table
python scripts/client_demo.py --server grpc://your-server:8081 --table your_table_name --limit 10
```

**Using the client helper in your own code:**

```python
from flight_server.client import FlightClientHelper

client = FlightClientHelper("grpc://localhost:8081")

# List tables
tables = client.list_tables()
print(tables)  # ['prices', 'trades', 'metadata']

# Fetch a table
table = client.get_table("prices")

# Use with Polars or Pandas
import polars as pl
df = pl.from_arrow(table)  # zero-copy
df = table.to_pandas()      # one conversion
```

## Troubleshooting

**Deletion vectors error**: Run `OPTIMIZE` and `VACUUM` on your Delta tables to remove deletion vectors (not supported by delta-rs).

**No tables found**: Check `AZURE_CONNECTION_STRING`, `AZURE_CONTAINER`, and that your container has Delta tables or CSV files.