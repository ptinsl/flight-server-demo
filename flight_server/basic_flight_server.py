"""Basic read only Flight Server for delta tables.

Uses delta-rs and PyArrow datasets for streaming reads to client.

    client = flight.connect("grpc://localhost:8081")
    info = client.get_flight_info(flight.FlightDescriptor.for_path("table_name"))
    reader = client.do_get(info.endpoints[0].ticket)
"""

import os

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.flight as flight
from pyarrow.fs import FileSystem
import structlog
from azure.storage.blob import ContainerClient
from deltalake import DeltaTable

from flight_server.azure import AzureConfig

log = structlog.get_logger()

# --- Table type: (schema, path, format) ---
TableEntry = tuple[pa.Schema, str, str]


def _clean_name(raw: str, prefix: str, suffix: str = "") -> str:
    # used for cleaning table directory name
    return raw.removeprefix(prefix).removesuffix(suffix).replace("/", "_").replace("-", "_")


def discover_tables(
    azure_config: AzureConfig, storage_options: dict[str, str]
) -> dict[str, TableEntry]:
    """Scan Azure blob container for Delta tables and CSV files."""
    tables: dict[str, TableEntry] = {}
    container = azure_config.container
    prefix = azure_config.prefix.strip("/") + "/" if azure_config.prefix else ""
    client = ContainerClient.from_connection_string(azure_config.connection_string, container)

    delta_paths, csv_blobs = set(), []
    for blob in client.list_blobs(name_starts_with=prefix):
        if "/_delta_log/" in blob.name:
            delta_paths.add(blob.name.split("/_delta_log/")[0])
        elif blob.name.endswith(".csv"):
            csv_blobs.append(blob.name)

    for table_dir in sorted(delta_paths):
        name = _clean_name(table_dir, prefix)
        path = f"abfss://{container}@{storage_options['account_name']}.dfs.core.windows.net/{table_dir}"
        try:
            schema = pa.schema(DeltaTable(path, storage_options=storage_options).schema().to_arrow())
            tables[name] = (schema, path, "delta")
            log.info("registered", name=name, type="delta")
        except Exception as e:
            log.warning("registration_failed", name=name, error=str(e))

    for blob_name in sorted(csv_blobs):
        name = _clean_name(blob_name, prefix, ".csv")
        path = f"abfss://{container}@{storage_options['account_name']}.dfs.core.windows.net/{blob_name}"
        try:
            fs, fs_path = FileSystem.from_uri(path)
            schema = ds.dataset(fs_path, format="csv", filesystem=fs).schema
            tables[name] = (schema, path, "csv")
            log.info("registered", name=name, type="csv")
        except Exception as e:
            log.warning("registration_failed", name=name, error=str(e))

    return tables


def scan_table(
    path: str, table_type: str, storage_options: dict[str, str]
) -> pa.RecordBatchReader:
    """Open a streaming reader for a delta or CSV table."""
    if table_type == "delta":
        return DeltaTable(path, storage_options=storage_options).to_pyarrow_dataset().scanner().to_reader()
    fs, fs_path = pa.fs.FileSystem.from_uri(path)  # type: ignore
    return ds.dataset(fs_path, format="csv", filesystem=fs).scanner().to_reader()



# --- The Flight Server ---
class BasicDeltaFlightServer(flight.FlightServerBase):

    def __init__(self, location: str, azure_config: AzureConfig | None = None, **kwargs):
        super().__init__(location, **kwargs)
        self._storage_options = azure_config.storage_options if azure_config else {}
        self._tables: dict[str, TableEntry] = (
            discover_tables(azure_config, self._storage_options) if azure_config else {}
        )
        log.info("initialized", tables=list(self._tables))

    def _info(self, name: str, descriptor: flight.FlightDescriptor) -> flight.FlightInfo:
        schema, _, _ = self._tables[name]
        return flight.FlightInfo(schema, descriptor, [flight.FlightEndpoint(name.encode(), [])], -1, -1)

    def list_flights(self, context, criteria):
        for name in self._tables:
            yield self._info(name, flight.FlightDescriptor.for_path(name))

    def get_flight_info(self, context, descriptor):
        name = descriptor.path[0].decode() if isinstance(descriptor.path[0], bytes) else descriptor.path[0]
        if name not in self._tables:
            raise flight.FlightServerError(f"Unknown table: {name}")
        return self._info(name, descriptor)

    def do_get(self, context, ticket):
        name = ticket.ticket.decode()
        if name not in self._tables:
            raise flight.FlightServerError(f"Unknown table: {name}")
        _, path, table_type = self._tables[name]
        log.info("do_get", table=name)
        return flight.RecordBatchStream(scan_table(path, table_type, self._storage_options))


def main() -> None:
    structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.dev.ConsoleRenderer()])
    host = os.getenv("FLIGHT_HOST", "0.0.0.0")
    port = int(os.getenv("FLIGHT_PORT", "8081"))
    location = f"grpc://{host}:{port}"

    server = BasicDeltaFlightServer(location, azure_config=AzureConfig.from_env())
    print(f"Starting Basic Delta Flight Server on {location}")
    server.serve()


if __name__ == "__main__":
    main()
