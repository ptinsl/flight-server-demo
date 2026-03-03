"""Azure configuration for Delta Lake access."""

import os
from dataclasses import dataclass


@dataclass
class AzureConfig:
    """Azure Blob Storage configuration."""

    connection_string: str
    container: str
    prefix: str | None = None

    @property
    def storage_options(self) -> dict[str, str]:
        """Storage options for delta-rs / object_store. Split conn str"""
        parts = dict(p.split("=", 1) for p in self.connection_string.split(";") if "=" in p)
        return {"account_name": parts["AccountName"], "account_key": parts["AccountKey"]}

    @classmethod
    def from_env(cls) -> "AzureConfig | None":
        """Load config from environment variables. Returns None if not configured."""
        conn_str = os.getenv("AZURE_CONNECTION_STRING")
        container = os.getenv("AZURE_CONTAINER")
        if not conn_str or not container:
            return None

        return cls(
            connection_string=conn_str,
            container=container,
            prefix=os.getenv("AZURE_PREFIX"),
        )
