#!/usr/bin/env python3
"""
Comprehensive Multi-Protocol Comparison for All Demo Services

This script demonstrates how to use each demo service type via both gRPC and REST APIs,
allowing direct performance and usability comparison.
"""

import asyncio
import os
import site
import tempfile
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TypeVar

import httpx
import pandas as pd
from rich.console import Console
from rich.table import Table

# Add examples folder
site.addsitedir(str(Path(__file__).parent.parent))

from fastflight import BaseParams
from fastflight.client import FastFlightBouncer
from fastflight.utils.stream_utils import read_dataframe_from_arrow_stream
from multi_protocol_demo.demo_services.csv_demo import CsvFileParams
from multi_protocol_demo.demo_services.duckdb_demo import DuckDBParams
from multi_protocol_demo.demo_services.sqllite_demo import SQLParams

T = TypeVar("T")
console = Console()


def request_for_data(url: str, json_data: dict, handle_stream: Callable[[Iterable[bytes]], T]) -> T:
    """Helper function to make HTTP requests and process streaming response"""
    with httpx.stream("POST", url, json=json_data, timeout=30) as response:
        response.raise_for_status()
        return handle_stream(response.iter_bytes())


class ServiceComparison:
    """Class to handle comparison between gRPC and REST for each service type"""

    def __init__(self):
        # Get ports from environment variables with defaults
        self.flight_port = os.getenv("FLIGHT_PORT", "8815")
        self.rest_port = os.getenv("REST_PORT", "8000")

        self.grpc_client = None
        self.grpc_url = f"grpc://localhost:{self.flight_port}"
        self.rest_base_url = f"http://127.0.0.1:{self.rest_port}/fastflight"

    def setup_grpc_client(self):
        """Setup gRPC client"""
        try:
            self.grpc_client = FastFlightBouncer(self.grpc_url)
            return True
        except Exception as e:
            console.print(f"❌ Failed to connect to gRPC server: {e}")
        return False

    def test_rest_connection(self):
        """Test REST API connection"""
        try:
            response = httpx.get(f"{self.rest_base_url}/registered_data_types", timeout=5)
            response.raise_for_status()
            return True
        except Exception as e:
            console.print(f"❌ Failed to connect to REST server: {e}")
            return False

    def compare_service(
        self, service_name: str, params: BaseParams, description: str
    ) -> tuple[dict[str, pd.DataFrame | None], dict[str, float]]:
        """Compare gRPC vs REST for a specific service"""
        console.print(f"\n📊 Testing {service_name}: {description}")
        console.print("=" * 60)

        results: dict[str, pd.DataFrame | None] = {"grpc_sync": None, "grpc_async": None, "rest": None}
        timings = {"grpc_sync": 0.0, "grpc_async": 0.0, "rest": 0.0}

        # Test gRPC Sync
        try:
            start_time = time.time()
            df = self.grpc_client.get_pd_dataframe(params)
            timings["grpc_sync"] = time.time() - start_time
            results["grpc_sync"] = df
            console.print(f"✅ gRPC Sync: {len(df)} rows in {timings['grpc_sync']:.3f}s")
        except Exception as e:
            console.print(f"❌ gRPC Sync failed: {e}")
            raise

        # Test gRPC Async
        try:

            async def grpc_async_test():
                start_time = time.time()
                df = await self.grpc_client.aget_pd_dataframe(params)
                timings["grpc_async"] = time.time() - start_time
                return df

            df = asyncio.run(grpc_async_test())
            results["grpc_async"] = df
            console.print(f"✅ gRPC Async: {len(df)} rows in {timings['grpc_async']:.3f}s")
        except Exception as e:
            console.print(f"❌ gRPC Async failed: {e}")
            raise

        # Test REST
        try:
            start_time = time.time()
            df = request_for_data(f"{self.rest_base_url}/stream", params.to_json(), read_dataframe_from_arrow_stream)
            timings["rest"] = time.time() - start_time
            results["rest"] = df
            console.print(f"✅ REST API: {len(df)} rows in {timings['rest']:.3f}s")
        except Exception as e:
            console.print(f"❌ REST API failed: {e}")
            raise

        # Display comparison
        self._display_comparison_table(service_name, results, timings)

        return results, timings

    def _display_comparison_table(self, service_name: str, results: dict, timings: dict):
        """Display comparison table for the service"""
        table = Table(title=f"{service_name} Performance Comparison")
        table.add_column("Method", style="cyan")
        table.add_column("Status", style="green")
        table.add_column("Rows", justify="right")
        table.add_column("Time (s)", justify="right")
        table.add_column("Throughput (rows/s)", justify="right")

        for method in ["grpc_sync", "grpc_async", "rest"]:
            if results[method] is not None:
                rows = len(results[method])
                timing = timings[method]
                throughput = rows / timing if timing > 0 else 0
                table.add_row(
                    method.replace("_", " ").title(), "✅ Success", str(rows), f"{timing:.3f}", f"{throughput:.0f}"
                )
            else:
                table.add_row(method.replace("_", " ").title(), "❌ Failed", "-", "-", "-")

        console.print(table)


def main():
    """Main comparison function"""
    console.print("🚀 FastFlight Multi-Protocol Service Comparison")
    console.print("=" * 60)
    console.print("Testing all demo services via gRPC and REST APIs")
    console.print()

    # Check server connections
    console.print("🔍 Checking server connections...")

    comparison = ServiceComparison()

    grpc_ok = comparison.setup_grpc_client()
    rest_ok = comparison.test_rest_connection()

    if not grpc_ok:
        console.print("Please start the FastFlight server: python start_flight_server.py")
    if not rest_ok:
        console.print("Please start the REST server: python start_rest_server.py")

    if not (grpc_ok and rest_ok):
        console.print("\n❌ Cannot proceed without both servers running")
        return

    console.print("✅ Both servers are running")

    # Test 1: SQLite Example
    console.print("\n" + "=" * 80)
    sql_params = SQLParams(
        conn_str="sqlite:///example.db",
        query="SELECT 1 as id, 'Hello from SQLite' as message, datetime('now') as timestamp",
    )
    comparison.compare_service("SQLite", sql_params, "Basic SQLite query")

    # Test 2: CSV File (if we can create a temp file)
    console.print("\n" + "=" * 80)
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample CSV for direct CSV reading
        csv_data = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=200, freq="1h"),
                "temperature": [20 + (i % 30) + (i * 0.1 % 10) for i in range(200)],
                "humidity": [50 + (i % 40) + (i * 0.05 % 15) for i in range(200)],
            }
        )
        csv_file_path = Path(tmpdir) / "sensor_data.csv"
        csv_data.to_csv(csv_file_path, index=False)

        csv_params = CsvFileParams(path=csv_file_path)
        comparison.compare_service("CSV File", csv_params, "Direct CSV file reading (200 rows)")

    # Test 3: DuckDB Example
    console.print("\n" + "=" * 80)
    duckdb_params = DuckDBParams(query="SELECT range as id, 'Row ' || range as message FROM range(1, 101)")
    comparison.compare_service("DuckDB", duckdb_params, "In-memory DuckDB with 100 rows")

    # Test 4: DuckDB with CSV
    console.print("\n" + "=" * 80)
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create sample CSV
        sample_data = pd.DataFrame(
            {
                "id": range(1, 501),
                "value": [i * 1.5 for i in range(1, 501)],
                "category": ["A" if i % 3 == 0 else "B" if i % 3 == 1 else "C" for i in range(1, 501)],
            }
        )
        csv_path = Path(tmpdir) / "sample_data.csv"
        sample_data.to_csv(csv_path, index=False)

        duckdb_csv_params = DuckDBParams(
            database_path=":memory:",
            query=f"SELECT * FROM read_csv_auto('{csv_path}') WHERE value > ? ORDER BY id LIMIT 50",
            parameters=[100.0],
        )
        comparison.compare_service("DuckDB+CSV", duckdb_csv_params, "DuckDB querying CSV file (500 rows)")

    # Summary
    console.print("\n" + "=" * 80)
    console.print("🎉 Comparison completed!")
    console.print("\n📋 Summary:")
    console.print("• gRPC typically offers lower latency for direct connections")
    console.print("• REST provides better compatibility with web applications")
    console.print("• Both use the same underlying FastFlight server")
    console.print("• Choice depends on your integration requirements")

    console.print("\n💡 Next steps:")
    console.print("• Try modifying the queries to test with your data")
    console.print("• Check the benchmark tools for detailed performance analysis")
    console.print("• Explore custom data services for your specific use cases")


if __name__ == "__main__":
    main()
