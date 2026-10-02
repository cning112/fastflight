"""Demo services for FastFlight framework."""

from .duckdb_demo import DuckDBDataService, DuckDBParams
from .echo_demo import EchoDataService, EchoParams

__all__ = ["DuckDBDataService", "DuckDBParams", "EchoDataService", "EchoParams"]
