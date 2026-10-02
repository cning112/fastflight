"""DuckDB demo service re-exported for the multi-protocol demo.

The canonical implementation lives in ``fastflight.demo_services.duckdb_demo``.
This module simply re-exports it so the demo scripts keep a stable import path.
"""

from fastflight.demo_services.duckdb_demo import DuckDBDataService, DuckDBParams

__all__ = ["DuckDBDataService", "DuckDBParams"]
