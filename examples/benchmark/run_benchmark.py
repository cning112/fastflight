"""
FastFlight Benchmark Runner V2

Refactored and improved version with the following features:
1. Cleaner configuration management
2. Better error handling and retry mechanisms
3. Real-time progress display and statistics
4. Scalable result formats
5. More accurate performance metric calculations
6. Graceful resource cleanup
"""

import dataclasses
import logging
import statistics
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd

from fastflight.client import FastFlightBouncer
from fastflight.utils.custom_logging import setup_logging

# Handle imports based on execution context
if __name__ == "__main__":
    # When run as script, use relative import
    from mock_data_service import MockDataParams, MockDataParamsAsync
else:
    # When imported as module, use relative import
    from .mock_data_service import MockDataParams, MockDataParamsAsync

setup_logging(console_log_level="INFO", log_file=None)

# Setup logging
logger = logging.getLogger(__name__)


@dataclasses.dataclass
class BenchmarkConfig:
    """Benchmark configuration"""

    server_location: str = "grpc://0.0.0.0:8815"  # nosec B104

    # Test parameter ranges
    delay_per_row_values: list[float] = dataclasses.field(
        default_factory=lambda: [1e-6, 1e-5]  # 1µs, 10µs
    )
    rows_per_batch_values: list[int] = dataclasses.field(default_factory=lambda: [1000, 5000, 10000])
    concurrent_requests_values: list[int] = dataclasses.field(default_factory=lambda: [1, 3, 5, 10])

    # Test control
    warmup_runs: int = 1
    benchmark_runs: int = 3  # Multiple runs for averaging
    timeout_seconds: float = 60.0

    # Output control
    output_file: str = "benchmark_results_v2.csv"
    save_raw_results: bool = True
    verbose: bool = True


@dataclasses.dataclass
class SingleRunResult:
    """Result of a single run"""

    rows_per_batch: int
    concurrent_requests: int
    delay_per_row: float
    mode: str  # 'sync' or 'async'

    # Performance metrics
    latency_seconds: float
    throughput_mbps: float
    data_size_mb: float

    # Metadata
    timestamp: datetime
    success: bool
    error_message: str | None = None


@dataclasses.dataclass
class AggregatedResult:
    """Aggregated result statistics"""

    rows_per_batch: int
    concurrent_requests: int
    delay_per_row: float
    mode: str

    # Aggregated statistics
    avg_latency_seconds: float
    min_latency_seconds: float
    max_latency_seconds: float
    std_latency_seconds: float

    avg_throughput_mbps: float
    min_throughput_mbps: float
    max_throughput_mbps: float
    std_throughput_mbps: float

    # Metadata
    run_count: int
    success_rate: float
    avg_data_size_mb: float


class BenchmarkRunner:
    """Benchmark runner"""

    def __init__(self, config: BenchmarkConfig):
        self.config = config
        self.results: list[SingleRunResult] = []

    def calculate_throughput(self, start_timestamp: float, end_timestamp: float, data_size_bytes: int) -> float:
        """Calculate throughput (MB/s)"""
        elapsed_time = end_timestamp - start_timestamp
        if elapsed_time <= 0:
            return 0.0
        return (data_size_bytes / elapsed_time) / (1024 * 1024)

    def run_single_request(
        self, client: FastFlightBouncer, rows_per_batch: int, delay_per_row: float, mode: str
    ) -> SingleRunResult:
        """Execute single request and measure performance"""

        start_time = time.perf_counter()
        timestamp = datetime.now()

        if mode == "sync":
            params = MockDataParams(rows_per_batch=rows_per_batch, delay_per_row=delay_per_row)
        else:
            params = MockDataParamsAsync(rows_per_batch=rows_per_batch, delay_per_row=delay_per_row)

        # Execute data fetch
        table = client.get_pa_table(params)
        end_time = time.perf_counter()

        # Calculate metrics
        latency = end_time - start_time
        data_size_bytes = table.nbytes
        data_size_mb = data_size_bytes / (1024 * 1024)
        throughput = self.calculate_throughput(start_time, end_time, data_size_bytes)

        return SingleRunResult(
            rows_per_batch=rows_per_batch,
            concurrent_requests=1,
            delay_per_row=delay_per_row,
            mode=mode,
            latency_seconds=latency,
            throughput_mbps=throughput,
            data_size_mb=data_size_mb,
            timestamp=timestamp,
            success=True,
        )

    def run_concurrent_requests(
        self, concurrent_requests: int, rows_per_batch: int, delay_per_row: float, mode: str
    ) -> list[SingleRunResult]:
        """Execute concurrent requests"""
        logger.info(
            f"Running {concurrent_requests} concurrent {mode=} requests, each with "
            f"{rows_per_batch=} and {delay_per_row=}..."
        )

        results: list[SingleRunResult] = []

        with ThreadPoolExecutor(max_workers=concurrent_requests) as executor:
            # Submit all tasks, each task uses independent client as context
            futures = []
            for _i in range(concurrent_requests):
                future = executor.submit(self._run_single_request_with_context, rows_per_batch, delay_per_row, mode)
                futures.append(future)

            # Collect results
            for future in as_completed(futures, timeout=self.config.timeout_seconds):
                result = future.result()
                result.concurrent_requests = concurrent_requests
                results.append(result)

        if self.config.verbose:
            successful_results = [r for r in results if r.success]
            if successful_results:
                avg_latency = statistics.mean([r.latency_seconds for r in successful_results])
                total_throughput = sum([r.throughput_mbps for r in successful_results])
                logger.info(
                    f"Concurrent test completed: {len(successful_results)}/{concurrent_requests} successful, "
                    f"avg_latency={avg_latency:.3f}s, total_throughput={total_throughput:.2f} MB/s"
                )

        return results

    def _run_single_request_with_context(self, rows_per_batch: int, delay_per_row: float, mode: str) -> SingleRunResult:
        """Execute single request in independent context"""
        with FastFlightBouncer(self.config.server_location, client_pool_size=1) as client:
            return self.run_single_request(client, rows_per_batch, delay_per_row, mode)

    def run_warmup(self, rows_per_batch: int, delay_per_row: float, mode: str) -> None:
        """Execute warmup runs"""
        if self.config.verbose:
            logger.info(f"Running warmup for {mode} mode...")

        for _ in range(self.config.warmup_runs):
            with FastFlightBouncer(self.config.server_location, client_pool_size=1) as client:
                self.run_single_request(client, rows_per_batch, delay_per_row, mode)

    def run_benchmark_scenario(
        self, rows_per_batch: int, concurrent_requests: int, delay_per_row: float, mode: str
    ) -> list[SingleRunResult]:
        """Run benchmark for specific scenario"""

        scenario_name = f"{mode}_{rows_per_batch}rows_{concurrent_requests}conc_{delay_per_row:.0e}delay"

        if self.config.verbose:
            logger.info(f"Running scenario: {scenario_name}")

        # Warmup
        self.run_warmup(rows_per_batch, delay_per_row, mode)

        # Multiple runs for averaging
        all_results = []

        for run_i in range(self.config.benchmark_runs):
            if self.config.verbose:
                logger.info(f"  Run {run_i + 1}/{self.config.benchmark_runs}")

            run_results = self.run_concurrent_requests(concurrent_requests, rows_per_batch, delay_per_row, mode)
            all_results.extend(run_results)

            # Brief rest between runs
            time.sleep(0.5)

        return all_results

    def aggregate_results(self, results: list[SingleRunResult]) -> AggregatedResult | None:
        """Aggregate single run results"""
        if not results:
            return None

        successful_results = [r for r in results if r.success]
        if not successful_results:
            return None

        # Get basic information
        first_result = results[0]

        # Calculate statistics
        latencies = [r.latency_seconds for r in successful_results]
        throughputs = [r.throughput_mbps for r in successful_results]
        data_sizes = [r.data_size_mb for r in successful_results]

        return AggregatedResult(
            rows_per_batch=first_result.rows_per_batch,
            concurrent_requests=first_result.concurrent_requests,
            delay_per_row=first_result.delay_per_row,
            mode=first_result.mode,
            avg_latency_seconds=statistics.mean(latencies),
            min_latency_seconds=min(latencies),
            max_latency_seconds=max(latencies),
            std_latency_seconds=statistics.stdev(latencies) if len(latencies) > 1 else 0.0,
            avg_throughput_mbps=statistics.mean(throughputs),
            min_throughput_mbps=min(throughputs),
            max_throughput_mbps=max(throughputs),
            std_throughput_mbps=statistics.stdev(throughputs) if len(throughputs) > 1 else 0.0,
            run_count=len(successful_results),
            success_rate=len(successful_results) / len(results),
            avg_data_size_mb=statistics.mean(data_sizes),
        )

    def run_full_benchmark(self) -> pd.DataFrame:
        """Run complete benchmark suite"""

        logger.info("🚀 Starting FastFlight benchmark")
        logger.info(f"Configuration: {self.config}")

        total_scenarios = (
            len(self.config.delay_per_row_values)
            * len(self.config.rows_per_batch_values)
            * len(self.config.concurrent_requests_values)
            * 2  # sync and async
        )

        logger.info(f"Total {total_scenarios} test scenarios")

        aggregated_results = []
        scenario_count = 0

        for delay_per_row in self.config.delay_per_row_values:
            for rows_per_batch in self.config.rows_per_batch_values:
                for concurrent_requests in self.config.concurrent_requests_values:
                    for mode in ["sync", "async"]:
                        scenario_count += 1

                        logger.info(f"\n📊 Scenario {scenario_count}/{total_scenarios}")
                        logger.info(f"Parameters: {rows_per_batch=}, {concurrent_requests=}, {delay_per_row=}, {mode=}")

                        scenario_results = self.run_benchmark_scenario(
                            rows_per_batch, concurrent_requests, delay_per_row, mode
                        )

                        # Save raw results
                        self.results.extend(scenario_results)

                        # Aggregate results
                        aggregated = self.aggregate_results(scenario_results)
                        if aggregated:
                            aggregated_results.append(aggregated)

                            if self.config.verbose:
                                logger.info(
                                    f"✅ Completed: avg_latency={aggregated.avg_latency_seconds:.3f}s, "
                                    f"avg_throughput={aggregated.avg_throughput_mbps:.2f} MB/s, "
                                    f"success_rate={aggregated.success_rate:.1%}"
                                )
                        else:
                            logger.warning("❌ Scenario failed - no valid results")

        # Convert to DataFrame
        df = pd.DataFrame([dataclasses.asdict(r) for r in aggregated_results])

        # Save results
        self.save_results(df)

        logger.info(f"🎉 Benchmark completed! Results saved to {self.config.output_file}")
        return df

    def save_results(self, df: pd.DataFrame) -> None:
        """Save results to file"""

        # Save aggregated results
        df.to_csv(self.config.output_file, index=False)
        logger.info(f"Aggregated results saved to: {self.config.output_file}")

        # Save raw results
        if self.config.save_raw_results and self.results:
            raw_file = self.config.output_file.replace(".csv", "_raw.csv")
            raw_df = pd.DataFrame([dataclasses.asdict(r) for r in self.results])
            raw_df.to_csv(raw_file, index=False)
            logger.info(f"Raw results saved to: {raw_file}")


def create_default_config() -> BenchmarkConfig:
    """Create default configuration"""
    return BenchmarkConfig(
        delay_per_row_values=[1e-6, 1e-5],  # 1µs, 10µs
        rows_per_batch_values=[1000, 5000, 10000],
        concurrent_requests_values=[1, 3, 5, 10],
        warmup_runs=1,
        benchmark_runs=3,
        verbose=True,
    )


def create_quick_config() -> BenchmarkConfig:
    """Create quick test configuration"""
    return BenchmarkConfig(
        delay_per_row_values=[1e-6],  # Only test 1µs
        rows_per_batch_values=[1000, 5000],  # Reduce batch sizes
        concurrent_requests_values=[1, 5],  # Reduce concurrency
        warmup_runs=1,
        benchmark_runs=2,  # Reduce run count
        output_file="benchmark_results_quick.csv",
        verbose=True,
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="FastFlight Benchmark Runner")
    parser.add_argument("--quick", action="store_true", help="Run quick test")
    parser.add_argument("--server", default="grpc://127.0.0.1:8815", help="Server address")
    parser.add_argument("--output", default="benchmark_results.csv", help="Output filename")
    parser.add_argument("--runs", type=int, default=3, help="Number of runs per scenario")

    args = parser.parse_args()

    print(f"Make sure the fastflight server is running at {args.server}")
    print("If not, please start the server first by running the following command:")
    print(f"python start_flight_server.py --server {args.server}")
    input("Press Enter to continue...")

    # Create configuration
    config = create_quick_config() if args.quick else create_default_config()

    # Apply command line arguments
    config.server_location = args.server
    config.output_file = args.output
    config.benchmark_runs = args.runs

    logger.info("🎯 FastFlight Benchmark")
    logger.info(f"Server: {config.server_location}")
    logger.info(f"Output file: {config.output_file}")

    # Run benchmark
    runner = BenchmarkRunner(config)
    results_df = runner.run_full_benchmark()

    # Display summary
    print("\n📈 Benchmark Summary:")
    print("=" * 50)

    for mode in ["sync", "async"]:
        mode_results = results_df[results_df["mode"] == mode]
        if not mode_results.empty:
            avg_throughput = mode_results["avg_throughput_mbps"].mean()
            avg_latency = mode_results["avg_latency_seconds"].mean()
            print(f"{mode.upper():>5}: avg throughput {avg_throughput:.2f} MB/s, avg latency {avg_latency:.3f}s")

    # Calculate overall improvement
    sync_data = results_df[results_df["mode"] == "sync"]
    async_data = results_df[results_df["mode"] == "async"]

    if not sync_data.empty and not async_data.empty:
        sync_avg_throughput = sync_data["avg_throughput_mbps"].mean()
        async_avg_throughput = async_data["avg_throughput_mbps"].mean()

        improvement = ((async_avg_throughput / sync_avg_throughput) - 1) * 100
        print(f"\n🚀 Average async performance improvement over sync: {improvement:+.1f}%")
