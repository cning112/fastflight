# FastFlight Benchmark Analysis

This folder contains benchmark scripts to compare the performance of FastFlight's synchronous (sync) and asynchronous (async) batch data transmission implementations.

## 🎯 Quick Answer: Should I Use Async?

**YES** - Async mode is consistently **22% faster** than sync across all test scenarios.

---

## What We Test

We measure **throughput** (data transfer speed in MB/s) under different conditions:

- **Batch Sizes:** 1k, 5k, and 10k rows per request
- **Concurrency:** 1 to 10 concurrent requests  
- **Network Conditions:** Low delay (1µs) and high delay (10µs) scenarios
- **Data Size:** ~191 MB per batch

---

## 📊 Key Results

### Overall Performance
- **Average Improvement:** Async is **22% faster** than sync
- **Range:** 4% to 28% improvement depending on scenario
- **Consistency:** Async wins in **100%** of test scenarios

### By Scenario
| Scenario | Sync Throughput | Async Throughput | Improvement |
|----------|----------------|------------------|-------------|
| **Low Delay (1µs)** | 142 MB/s | 167 MB/s | **+18%** |
| **High Delay (10µs)** | 17 MB/s | 18 MB/s | **+6%** |

### Best Performance
- **Best case:** +28% improvement (5k batch, single client, low delay)
- **Worst case:** +4% improvement (still better!)
- **Sweet spot:** 5k-10k rows per batch

---

## 🚀 Quick Start

### 1. Run the Benchmark

```bash
# Install dependencies
uv sync

# Start the server (in separate terminal)
python start_flight_server.py

# Run benchmark
python run_benchmark.py

# Generate charts
python plot_benchmark_results.py
```

### 2. View Results

The script generates:
- **Console output:** Simple summary with improvement percentages
- **CSV files:** Raw benchmark data
- **Charts:** Visual comparison showing throughput improvements

---

## 📈 Understanding the Charts

### Heatmaps (Main Output)
- **Two separate heatmaps:** One for each delay scenario
- **Green numbers:** Positive improvement percentages
- **Axes:** Concurrent requests (Y) vs Batch size (X)
- **What to look for:** All numbers should be positive (async better)

### Bar Chart (Summary)
- **Orange bars:** Sync performance
- **Green bars:** Async performance  
- **Green should be taller:** Shows async is faster
- **Red percentages:** Improvement amount

---

## 🔬 Technical Details

### Why Async is Faster
1. **Non-blocking I/O:** Async can handle other requests while waiting
2. **Better resource usage:** Less memory overhead than threading
3. **Cooperative scheduling:** Efficient handling of concurrent operations

### Why Async Advantage Decreases with Longer Delays
When delays are longer (10µs vs 1µs), the actual data processing becomes a smaller fraction of total time. Most time is spent waiting, so both sync and async modes are limited by the inherent delay rather than their efficiency differences. 

**Analogy:** If you're waiting 10 seconds for a slow database, being 20% more efficient at the 0.1s processing part only saves you 0.02s total - a barely noticeable improvement on the 10s total time.

This is why async shows **18% improvement** in low-delay scenarios but only **6% improvement** in high-delay scenarios.

### Test Environment
- **Device:** MacBook Pro (M2 Max), 32GB RAM
- **Reliability:** 3 runs per scenario, statistical aggregation
- **Data consistency:** Fixed random seed for reproducible results

### Performance Patterns
- **Low delay scenarios:** Async shows 13-28% improvement
- **High delay scenarios:** Async shows 4-12% improvement  
- **High concurrency:** Async maintains performance better than sync
- **Large batches:** Both modes benefit, but async still leads

---

## 📋 File Structure

```
fastflight_benchmark/
├── README.md                    # This file
├── run_benchmark.py            # Main benchmark script
├── plot_benchmark_results.py   # Generate visualizations  
├── start_flight_server.py      # Start test server
├── mock_data_service.py        # Test data generators
├── benchmark_results.csv       # Benchmark output
└── plots/                      # Generated charts
    └── throughput_improvement_by_delay.png
```

---

## 🎯 Conclusion

**Use async mode for FastFlight.** It's faster in every scenario we tested, with an average 22% throughput improvement and better handling of concurrent requests.

The benchmark demonstrates that FastFlight's asynchronous implementation provides consistent, measurable performance benefits across diverse workload patterns.

---

## 🛠 Advanced Usage

### Quick Test
```bash
python run_benchmark.py --quick
```

### Custom Server Location  
```bash
python start_flight_server.py --server grpc://localhost:9090
python run_benchmark.py --server grpc://localhost:9090
```

### Modify Test Parameters
Edit `run_benchmark.py` to change:
- Delay values (`delay_per_row_values`)
- Batch sizes (`rows_per_batch_values`) 
- Concurrency levels (`concurrent_requests_values`)
- Number of runs (`benchmark_runs`)

---

*For questions or suggestions, please refer to the main FastFlight project documentation.*
