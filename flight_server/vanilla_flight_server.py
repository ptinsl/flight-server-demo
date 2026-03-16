"""Vanilla read-only Arrow Flight server for Delta Lake tables.

Extends PyArrow's FlightServerBase directly — no additional dependencies.
Any Arrow Flight client can connect and stream tables over gRPC.

    client = flight.connect("grpc://localhost:8815")

    for fl in client.list_flights():
        print(fl.descriptor.path[0].decode(), fl.schema)

    info = client.get_flight_info(flight.FlightDescriptor.for_path("table_name"))
    table = client.do_get(info.endpoints[0].ticket).read_all()
"""

import os

import pyarrow as pa
import pyarrow.flight as flight
import structlog
from azure.storage.blob import ContainerClient
from deltalake import DeltaTable

from flight_server.azure import AzureConfig

log = structlog.get_logger()

# --- Table registry: name → (schema, abfss_path) ---
TableEntry = tuple[pa.Schema, str]


def discover_tables(
    azure_config: AzureConfig, storage_options: dict[str, str]
) -> dict[str, TableEntry]:
    """Scan Azure blob container for Delta tables."""
    tables: dict[str, TableEntry] = {}
    container = azure_config.container
    prefix = azure_config.prefix.strip("/") + "/" if azure_config.prefix else ""
    client = ContainerClient.from_connection_string(azure_config.connection_string, container)

    delta_roots: set[str] = set()
    for blob in client.list_blobs(name_starts_with=prefix):
        if "/_delta_log/" in blob.name:
            delta_roots.add(blob.name.split("/_delta_log/")[0])

    for table_dir in sorted(delta_roots):
        name = table_dir.removeprefix(prefix).replace("/", "_").replace("-", "_")
        path = f"abfss://{container}@{storage_options['account_name']}.dfs.core.windows.net/{table_dir}"
        try:
            schema = pa.schema(DeltaTable(path, storage_options=storage_options).schema().to_arrow())
            tables[name] = (schema, path)
            log.info("registered", name=name)
        except Exception as e:
            log.warning("registration_failed", name=name, error=str(e))

    return tables


# --- The Flight Server ---
class VanillaFlightServer(flight.FlightServerBase):

    def __init__(self, location: str, azure_config: AzureConfig | None = None, **kwargs):
        super().__init__(location, **kwargs)
        self._storage_options = azure_config.storage_options if azure_config else {}
        self._tables: dict[str, TableEntry] = (
            discover_tables(azure_config, self._storage_options) if azure_config else {}
        )
        log.info("initialized", tables=list(self._tables))

    def _info(self, name: str, descriptor: flight.FlightDescriptor) -> flight.FlightInfo:
        schema, _ = self._tables[name]
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
        _, path = self._tables[name]
        log.info("do_get", table=name)
        reader = DeltaTable(path, storage_options=self._storage_options).to_pyarrow_dataset().scanner().to_reader()
        return flight.RecordBatchStream(reader)


def main() -> None:
    structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.dev.ConsoleRenderer()])
    host = os.getenv("FLIGHT_HOST", "0.0.0.0")
    port = int(os.getenv("FLIGHT_PORT", "8815"))
    location = f"grpc://{host}:{port}"

    server = VanillaFlightServer(location, azure_config=AzureConfig.from_env())
    print(f"Starting Vanilla Flight Server on {location}")
    server.serve()


if __name__ == "__main__":
    main()
