# DistributedTimeSeriesService Configuration Control

## 🎯 New Feature: Configurable Distribution

The `DistributedTimeSeriesService` now supports fine-grained control over distributed processing through configuration parameters.

## 🔧 Configuration Parameters

### `enable_distributed: bool = True`
Controls whether distributed processing is enabled at all.

- **`True`** (default): Uses Ray or AsyncIO for parallel processing
- **`False`**: Forces single-threaded sequential processing

### `max_workers: Optional[int] = None`
Controls the maximum number of parallel workers.

- **`None`** (default): Auto-detects optimal worker count
- **`int`**: Explicitly limits the number of workers

## 🚀 Usage Examples

### Default Configuration (Recommended)
```python
# Auto-selects best backend (Ray -> AsyncIO)
service = DistributedTimeSeriesService(base_service)
```

### Development/Debugging Mode
```python
# Single-threaded for easy debugging
service = DistributedTimeSeriesService(
    base_service, 
    enable_distributed=False
)
```

### Resource-Constrained Environment
```python
# Limit to 2 workers
service = DistributedTimeSeriesService(
    base_service, 
    max_workers=2
)
```

## 📊 Backend Selection Logic

```
1. Is enable_distributed=False?
   └─ Yes → Use single_threaded backend
   └─ No → Continue to step 2

2. Is Ray available and can initialize?
   └─ Yes → Use ray backend
   └─ No → Use asyncio backend
```

## 🎉 Summary

✅ **Ray -> AsyncIO -> Single-threaded fallback**
✅ **Configurable distribution via `enable_distributed` parameter**  
✅ **Resource control via `max_workers` parameter**
✅ **Same API works across all configurations**
✅ **Zero breaking changes to existing code**

Your `DistributedTimeSeriesService` now provides complete control over distributed processing!