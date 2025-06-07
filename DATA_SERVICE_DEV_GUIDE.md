# FastFlight Data Service Developer Guide

## Quick Start

Data services require **two components**:

```python
# 1. Parameter class - defines request structure
class DatabaseParams(BaseParams):
    connection_string: str
    query: str
    timeout: int = 30

# 2. Service class - implements data fetching
class DatabaseService(BaseDataService[DatabaseParams]):
    def get_batches(self, params: DatabaseParams, batch_size=None):
        # For CPU operations
        yield pa.RecordBatch.from_pandas(df)
    
    # OR for I/O operations:
    async def aget_batches(self, params: DatabaseParams, batch_size=None):
        data = await fetch_from_database(params.connection_string)
        yield pa.RecordBatch.from_pandas(data)
```

**Key Notes:**
- Parameter class defines typed fields for request validation
- Service class is generic over the parameter type: `BaseDataService[DatabaseParams]`
- Both classes are linked through the generic type parameter
- The `param_type` field is automatically added when being serialized for service routing

## When to Use Which Method?

**Use sync `get_batches()` for:**
- In-memory data
- CPU-bound calculations  
- Simple file reads
- No external dependencies

**Use async `aget_batches()` for:**
- Database queries
- HTTP API calls
- Remote storage access
- Any I/O-bound operations

## Performance Notes

- Sync methods: Zero overhead
- Async methods: 3-6% adapter overhead
- Server always tries sync first, falls back to async

## FAQ

**Q: Can I implement both methods?**
A: Yes, but sync will always be used (server tries sync first).

**Q: Why does the server prefer sync?**
A: PyArrow Flight's `do_get()` is synchronous by protocol. Async services require conversion.

**Q: Should I always use async for modern code?**
A: Not necessarily. For simple data operations, sync is simpler with no performance cost.

**Q: How does the server choose which method to call?**
A: Always tries `get_batches()` first. If it raises `NotImplementedError`, falls back to `aget_batches()`.
