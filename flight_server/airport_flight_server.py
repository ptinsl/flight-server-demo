"""Airport-compatible Flight server for Delta Lake tables.

Uses DeltaTable + PyArrow scanner for data access with predicate pushdown
and column pruning driven by the Airport/DuckDB client.

    ATTACH 'grpc://localhost:8815' AS db (TYPE AIRPORT);
    SHOW ALL TABLES;
    SELECT col FROM db.default.my_table WHERE col2 = 'x';
"""

import os
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.flight as flight
import structlog
from azure.storage.blob import ContainerClient
from deltalake import DeltaTable
from pydantic import BaseModel

import query_farm_flight_server.flight_handling as flight_handling
import query_farm_flight_server.flight_inventory as flight_inventory
import query_farm_flight_server.parameter_types as parameter_types
from query_farm_flight_server.auth import Account, AccountToken
from query_farm_flight_server.auth_manager_naive import AuthManagerNaive
from query_farm_flight_server.flight_inventory import FlightSchemaMetadata, SchemaInfo, UploadParameters
from query_farm_flight_server.middleware import AuthManagerMiddlewareFactory, SaveHeadersMiddlewareFactory
from query_farm_flight_server.server import (
    AirportSerializedCatalogRoot,
    BasicFlightServer,
    CallContext,
    CreateTransactionResult,
    GetCatalogVersionResult,
)

from flight_server.azure import AzureConfig

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Ticket model
# ---------------------------------------------------------------------------

class TicketData(BaseModel):
    table_name: str
    json_filters: str | None = None  # raw JSON string, re-parsed at scan time
    column_names: list[str] | None = None


# ---------------------------------------------------------------------------
# Filter conversion: DuckDB bound-expression JSON → PyArrow compute expressions
# Airport sends DuckDB's WHERE clause as a bound expression tree (JSON).
# We walk it and convert to PyArrow expressions for Delta scanner pushdown.
# ---------------------------------------------------------------------------

def _get_column_name(expr: dict, column_names: list[str]) -> str | None:
    if expr.get("expression_class") != "BOUND_COLUMN_REF":
        return None
    alias = expr.get("alias")
    if alias:
        return alias
    col_idx = expr.get("binding", {}).get("column_index")
    if col_idx is not None and col_idx < len(column_names):
        return column_names[col_idx]
    return None


def _get_constant_value(expr: dict) -> Any:
    if expr.get("expression_class") != "BOUND_CONSTANT":
        return None
    value_obj = expr.get("value", {})
    if value_obj.get("is_null"):
        return None
    return value_obj.get("value")


def _convert_comparison(expr: dict, column_names: list[str]) -> pc.Expression | None:
    col_name = _get_column_name(expr.get("left", {}), column_names)
    if not col_name:
        return None

    value = _get_constant_value(expr.get("right", {}))
    field = pc.field(col_name)

    op = expr.get("type", "")
    if op == "COMPARE_EQUAL":
        return field.is_null() if value is None else field == value
    if op == "COMPARE_NOTEQUAL":
        return field.is_valid() if value is None else field != value
    if op == "COMPARE_LESSTHAN":
        return field < value if value is not None else None
    if op == "COMPARE_LESSTHANOREQUALTO":
        return field <= value if value is not None else None
    if op == "COMPARE_GREATERTHAN":
        return field > value if value is not None else None
    if op == "COMPARE_GREATERTHANOREQUALTO":
        return field >= value if value is not None else None
    return None


def _convert_expr(expr: dict, column_names: list[str]) -> pc.Expression | None:
    cls = expr.get("expression_class")
    if cls == "BOUND_COMPARISON":
        return _convert_comparison(expr, column_names)
    if cls == "BOUND_CONJUNCTION":
        children = [_convert_expr(c, column_names) for c in expr.get("children", [])]
        children = [c for c in children if c is not None]
        if not children:
            return None
        result = children[0]
        joiner = "__and__" if expr.get("type") == "CONJUNCTION_AND" else "__or__"
        for c in children[1:]:
            result = getattr(result, joiner)(c)
        return result
    return None


def _build_pyarrow_filter(filters: list[Any], column_names: list[str]) -> pc.Expression | None:
    expressions = [_convert_expr(f, column_names) for f in filters]
    expressions = [e for e in expressions if e is not None]
    if not expressions:
        return None
    result = expressions[0]
    for e in expressions[1:]:
        result = result.__and__(e)
    return result


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

class AirportFlightServer(BasicFlightServer[Account, AccountToken]):
    """Airport-compatible Flight server for Azure Delta Lake tables."""

    def __init__(
        self,
        *,
        location: str,
        azure_config: AzureConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._storage_options: dict[str, str] = azure_config.storage_options if azure_config else {}
        self._tables: dict[str, pa.Schema] = {}
        self._paths: dict[str, str] = {}

        if azure_config:
            self._discover_tables(azure_config)

        super().__init__(location=location, **kwargs)
        log.info("server_initialized", tables=list(self._tables))

    def _discover_tables(self, azure_config: AzureConfig) -> None:
        container = azure_config.container
        prefix = azure_config.prefix.strip("/") + "/" if azure_config.prefix else ""
        account = self._storage_options["account_name"]
        client = ContainerClient.from_connection_string(azure_config.connection_string, container)

        delta_roots: set[str] = set()
        for blob in client.list_blobs(name_starts_with=prefix):
            if "/_delta_log/" in blob.name:
                delta_roots.add(blob.name.split("/_delta_log/")[0])

        for table_dir in sorted(delta_roots):
            name = table_dir.removeprefix(prefix).replace("/", "_").replace("-", "_")
            path = f"abfss://{container}@{account}.dfs.core.windows.net/{table_dir}"
            try:
                schema = pa.schema(DeltaTable(path, storage_options=self._storage_options).schema().to_arrow())
                self._tables[name] = schema
                self._paths[name] = path
                log.info("registered", table=name)
            except Exception as e:
                log.warning("registration_failed", table=name, error=str(e))

    # --- Required abstract actions ---

    def action_catalog_version(
        self,
        *,
        context: CallContext[Account, AccountToken],
        parameters: parameter_types.CatalogVersion,
    ) -> GetCatalogVersionResult:
        return GetCatalogVersionResult(catalog_version=1, is_fixed=False)

    def action_create_transaction(
        self,
        *,
        context: CallContext[Account, AccountToken],
        parameters: parameter_types.CreateTransaction,
    ) -> CreateTransactionResult:
        return CreateTransactionResult(identifier=None)

    # --- Airport catalog actions ---

    def action_list_schemas(
        self,
        *,
        context: CallContext[Account, AccountToken],
        parameters: parameter_types.ListSchemas,
    ) -> AirportSerializedCatalogRoot:
        catalog = parameters.catalog_name
        inventory: dict[str, dict[str, list]] = {catalog: {"default": []}}

        for name, schema in self._tables.items():
            descriptor = flight.FlightDescriptor.for_path(name)
            metadata = FlightSchemaMetadata(
                type="table", catalog=catalog, schema="default", name=name, comment=None
            )
            info = flight.FlightInfo(schema, descriptor, [], -1, -1, app_metadata=metadata.serialize())
            inventory[catalog]["default"].append((info, metadata))

        return flight_inventory.upload_and_generate_schema_list(
            upload_parameters=UploadParameters(s3_client=None, base_url="", bucket_name=""),
            flight_service_name="airport-delta",
            flight_inventory=inventory,
            schema_details={
                "default": SchemaInfo(description="Delta Lake tables", tags={}, is_default=True)
            },
            skip_upload=True,
            serialize_inline=True,
            catalog_version=1,
            catalog_version_fixed=False,
        )

    def action_flight_info(
        self,
        *,
        context: CallContext[Account, AccountToken],
        parameters: parameter_types.FlightInfo,
    ) -> flight.FlightInfo:
        name = parameters.descriptor.path[0].decode()
        if name not in self._tables:
            raise flight.FlightServerError(f"Unknown table: {name}")
        return flight.FlightInfo(
            self._tables[name], flight.FlightDescriptor.for_path(name), [], -1, -1
        )

    def action_endpoints(
        self,
        *,
        context: CallContext[Account, AccountToken],
        parameters: parameter_types.Endpoints,
    ) -> list[flight.FlightEndpoint]:
        name = parameters.descriptor.path[0].decode()
        if name not in self._tables:
            raise flight.FlightServerError(f"Unknown table: {name}")

        json_filters = (
            parameters.parameters.json_filters.model_dump_json()
            if parameters.parameters.json_filters
            else None
        )

        column_ids = parameters.parameters.column_ids
        column_names = (
            [self._tables[name].field(i).name for i in column_ids if i < len(self._tables[name])]
            if column_ids
            else None
        ) or None

        ticket = TicketData(table_name=name, json_filters=json_filters, column_names=column_names)
        return [flight_handling.endpoint(ticket_data=ticket, locations=None)]

    # --- Data ---

    def impl_do_get(
        self,
        *,
        context: CallContext[Account, AccountToken],
        ticket: flight.Ticket,
    ) -> flight.RecordBatchStream:
        data = flight_handling.decode_ticket_model(ticket, TicketData)

        if data.table_name not in self._paths:
            raise flight.FlightServerError(f"Unknown table: {data.table_name}")

        pa_filter = None
        if data.json_filters:
            fd = parameter_types.FilterData.model_validate_json(data.json_filters)
            pa_filter = _build_pyarrow_filter(fd.filters, fd.column_binding_names_by_index)
            if pa_filter is not None:
                log.info("predicate_pushdown", table=data.table_name)

        full_schema = self._tables[data.table_name]
        ipc_opts = pa.ipc.IpcWriteOptions(compression="lz4")

        scanner = (
            DeltaTable(self._paths[data.table_name], storage_options=self._storage_options)
            .to_pyarrow_dataset()
            .scanner(filter=pa_filter, columns=data.column_names)
        )
        reader = scanner.to_reader()

        # DuckDB Airport expects batches matching the full FlightInfo schema.
        # If columns were pruned, pad missing ones with nulls using the library helper.
        if set(reader.schema.names) != set(full_schema.names):
            log.info("column_pruning", table=data.table_name, columns=data.column_names)
            reader = pa.RecordBatchReader.from_batches(
                schema = full_schema,
                batches = flight_handling.generate_record_batches_for_used_fields(
                            reader=reader,
                            used_field_names=set(reader.schema.names),
                            schema=full_schema,
                ),
            )

        return flight.RecordBatchStream(reader, options=ipc_opts)


def main() -> None:
    structlog.configure(processors=[structlog.stdlib.add_log_level, structlog.dev.ConsoleRenderer()])
    host = os.getenv("FLIGHT_HOST", "0.0.0.0")
    port = int(os.getenv("FLIGHT_PORT", "8815"))
    location = f"grpc://{host}:{port}"

    auth_manager = AuthManagerNaive(
        account_type=Account,
        token_type=AccountToken,
        allow_anonymous_access=True,
    )

    server = AirportFlightServer(
        location=location,
        azure_config=AzureConfig.from_env(),
        middleware={
            "auth": AuthManagerMiddlewareFactory(auth_manager=auth_manager),
            "headers": SaveHeadersMiddlewareFactory(),
        },
    )

    print(f"Starting Airport Flight Server on {location}")
    server.serve()


if __name__ == "__main__":
    main()
