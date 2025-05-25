# Time Series Distributed Processing

FastFlight now supports intelligent partitioning and distributed processing of time series data using Ray.

## Overview

The time series extension provides:

1. **Smart Partitioning** - Automatically split large time series queries into optimal partitions
2. **Query Optimization** - Different strategies for real-time vs analytics workloads  
3. **Distributed Processing** - Scale horizontally using Ray clusters
4. **Flexible Configuration** - Easy-to-use optimization hints

## Quick Start

### 1. Define Your Time Series Parameters

```python
from fastflight.core.timeseries import TimeSeriesParams

class StockDataParams(TimeSeriesParams):
    symbol: str
    interval: str = "1min"
    
    def estimate_data_points(self) -> int:
        """Provide data point estimation for better partitioning."""
        duration = self.time_range_duration()
        if self.interval == "1min":
            return int(duration.total_seconds() / 60)
        return int(duration.total_seconds() / 3600)
```

### 2. Implement Your Data Service

```python
from fastflight.core.base import BaseDataService

class StockDataService(BaseDataService[StockDataParams]):
    def get_batches(self, params: StockDataParams, batch_size: int = None):
        # Your data fetching logic
        for batch in fetch_stock_data(params):
            yield batch
```

### 3. Use Smart Partitioning

```python
from datetime import datetime, timedelta

params = StockDataParams(
    symbol="AAPL",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 2, 1)  # 1 month of data
)

# Auto-partition based on data size
partitions = params.get_optimal_partitions(max_workers=8)
print(f"Split into {len(partitions)} partitions")

# Fixed window partitioning
hourly_partitions = params.split_by_window_size(timedelta(hours=1))
```

### 4. Enable Distributed Processing

```python
# Install Ray first: pip install ray
from fastflight.core.distributed import DistributedTimeSeriesService

base_service = StockDataService()
distributed_service = DistributedTimeSeriesService(base_service)

# Process data across Ray cluster
async for batch in distributed_service.aget_batches(params):
    process_batch(batch)
```

## Query Optimization

Different query patterns need different optimization strategies:

### Real-time Queries
```python
from fastflight.core.optimization import OptimizationHint, optimize_time_series_query

hint = OptimizationHint.for_real_time()
partitions = optimize_time_series_query(params, hint)
```

**Characteristics:**
- Minimal partitioning for low latency
- Queries ≤ 1 hour: no partitioning
- Longer queries: 15-minute windows

### Analytics Queries
```python
hint = OptimizationHint.for_analytics()
partitions = optimize_time_series_query(params, hint)
```

**Characteristics:**
- Aggressive partitioning for high throughput
- Large batch sizes (50,000 points)
- Utilizes maximum available workers

### Custom Optimization
```python
hint = OptimizationHint(
    pattern=QueryPattern.HISTORICAL,
    max_workers=12,
    target_batch_size=25000,
    prefer_recent_data=False
)
```

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   FastFlight    │    │  TimeSeriesParams │    │ OptimizationHint │
│     Server      │────│    Partitioner    │────│    Strategy     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│ DistributedTime │    │  Ray Remote      │    │ Query Pattern   │
│ SeriesService   │────│    Workers       │────│  Recognition    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

## Performance Benefits

### Automatic Scaling
- **Small queries**: Processed directly for minimal overhead
- **Medium queries**: Smart partitioning based on data density  
- **Large queries**: Maximum parallelization across Ray cluster

### Intelligent Partitioning
- **Time-based**: Equal time windows
- **Data-aware**: Based on estimated data points
- **Resource-aware**: Considers available workers

### Pattern Recognition
- **Real-time**: Optimized for low latency
- **Analytics**: Optimized for high throughput
- **Historical**: Balanced approach

## Configuration Examples

### High-Frequency Trading Data
```python
params = TickDataParams(
    symbol="EURUSD",
    start_time=datetime.now() - timedelta(hours=1),
    end_time=datetime.now()
)

hint = OptimizationHint.for_real_time()
# Result: No partitioning, direct processing
```

### Historical Analysis
```python
params = DailyDataParams(
    symbol="SPY", 
    start_time=datetime(2020, 1, 1),
    end_time=datetime(2024, 1, 1)  # 4 years
)

hint = OptimizationHint.for_analytics()
# Result: Multiple partitions across Ray cluster
```

### Custom Workload
```python
params = CustomParams(...)

# Fine-tune for your specific use case
hint = OptimizationHint(
    pattern=QueryPattern.BACKFILL,
    max_workers=6,
    target_batch_size=15000,
    enable_caching=True
)
```

## Ray Cluster Setup

### Local Development
```python
import ray
ray.init()  # Automatically uses local machine
```

### Production Cluster
```python
ray.init(address="ray://head-node:10001")
```

### Docker/Kubernetes
```yaml
# ray-cluster.yaml
apiVersion: ray.io/v1alpha1
kind: RayCluster
metadata:
  name: fastflight-cluster
spec:
  rayVersion: '2.8.0'
  headGroupSpec:
    # Head node configuration
  workerGroupSpecs:
    # Worker node configuration
```

## Error Handling

The distributed service includes automatic retry and error handling:

```python
# Failed partitions are logged and skipped
# Circuit breaker patterns prevent cascade failures
# Resilience configuration still applies
```

## Monitoring

Monitor distributed processing performance:

```python
import logging
logging.basicConfig(level=logging.INFO)

# Logs include:
# - Partition count and distribution
# - Worker utilization
# - Processing times per partition
# - Error rates and retry attempts
```

## Migration Guide

### From Regular to Distributed

**Before:**
```python
service = StockDataService()
for batch in service.get_batches(params):
    process(batch)
```

**After:**
```python
distributed_service = DistributedTimeSeriesService(service)
async for batch in distributed_service.aget_batches(params):
    process(batch)
```

### Gradual Adoption

You can use time series features without distributed processing:

```python
# Just partitioning
partitions = params.get_optimal_partitions(max_workers=4)
for partition in partitions:
    for batch in service.get_batches(partition):
        process(batch)

# Just optimization
hint = OptimizationHint.for_real_time()
optimized_partitions = optimize_time_series_query(params, hint)
```

## Best Practices

1. **Implement `estimate_data_points()`** in your params class for better partitioning
2. **Choose appropriate optimization hints** based on your query patterns
3. **Monitor Ray cluster utilization** to optimize worker count
4. **Test with small datasets first** before scaling to production
5. **Use connection pooling** in your data services for better performance

## Limitations

- Requires Ray for distributed processing
- Partition coordination adds some overhead for very small queries
- Data services must be stateless for distributed processing
- Clock synchronization important for time-based partitioning
