"""Client helper for Apache Arrow Flight connections."""

import pyarrow as pa
import pyarrow.flight as flight


class FlightClientHelper:
    """Wrapper around pyarrow.flight.FlightClient with convenience methods."""

    def __init__(self, server_uri: str):
        """Connect to Flight server.

        Args:
            server_uri: Server address like "grpc://localhost:8081"
        """
        self.client = flight.connect(server_uri)
        self.server_uri = server_uri

    def list_tables(self) -> list[str]:
        """List all available table names.

        Returns:
            List of table names
        """
        tables = []
        for flight_info in self.client.list_flights():
            name = flight_info.descriptor.path[0]
            if isinstance(name, bytes):
                name = name.decode()
            tables.append(name)
        return tables

    def get_table(self, table_name: str) -> pa.Table:
        """Fetch a table by name.

        Args:
            table_name: Name of the table to fetch

        Returns:
            PyArrow Table with all data
        """
        descriptor = flight.FlightDescriptor.for_path(table_name)
        info = self.client.get_flight_info(descriptor)
        reader = self.client.do_get(info.endpoints[0].ticket)
        return reader.read_all()
