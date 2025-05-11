<file name=1 path=TECHNICAL_DETAILS_chn.md># **FastFlight 技术文档**

## **📌 概述**

FastFlight 是 **Apache Arrow Flight** 的扩展，旨在 **简化高性能数据传输**，同时提供更好的 **可用性、集成性和开发者友好特性
**。

本文档详细介绍 FastFlight 的 **核心架构、数据流、性能优化策略、部署建议、扩展方式**，帮助开发者更深入地理解其设计原理。

---

## **🚀 FastFlight 架构设计**

FastFlight 主要包含以下核心组件：

1. **Flight Server**：基于 Apache Arrow Flight，提供高吞吐、低延迟的数据传输服务。
2. **Ticket 机制**：使用参数化 Ticket (`kind` 机制)，支持结构化数据请求，提高可读性。
3. **异步流式传输**：基于 Python `asyncio`，实现高效的 `async for` 数据流消费。
4. **FastAPI 适配层**：提供 REST API 兼容接口，支持 HTTP 客户端访问。
5. **CLI 工具**：提供快速启动和管理 Flight Server 的命令行工具。

### **🔹 FastFlight 架构图**

```text
+---------------------------+
|       Client (Python)     |
|    Flight Client (PyArrow)|
+---------------------------+
           ▲
           |  gRPC
           ▼
+----------------------------+
|      FastFlight Server     |
| - Handles Flight Requests  |
| - Uses Parameterized Ticket|
| - Streams Data Efficiently |
+---------------------------+
           ▲
           | JDBC / ODBC
           ▼
+---------------------------+
|      SQL / NoSQL DB       |
|  (Columnar or Row-Based)  |
+---------------------------+
```

---

## **🔀 数据流设计**

FastFlight 采用 **结构化 Ticket 机制**，避免原生 Arrow Flight 仅支持字节 (`bytes`) 传输的不透明问题。其数据流如下：

1️⃣ **客户端发送参数化 Ticket 请求**

```json
{
  "param_type": "duckdb.query",
  "query": "SELECT * FROM flights WHERE year = 2023",
  "limit": 1000,
  "timeout": 30
}
```

2️⃣ **Flight Server 解析 `param_type`，匹配合适的 `DataService` 处理请求**

3️⃣ **Flight Server 通过 JDBC/ODBC 查询数据库，并转换数据为 Apache Arrow 格式**

4️⃣ **使用 Arrow Flight gRPC 流式返回数据**

5️⃣ **客户端使用 `async for` 方式消费数据**

```python
async for batch in fast_flight_client.aget_stream_reader(ticket=DuckDBParams(...)):
    print(batch.to_pandas())
```

---

## **⚡ 性能优化分析**

FastFlight 相比传统 REST API / JDBC / ODBC 方案，具备显著性能优势：

| 方式                            | 传输协议 | 数据格式                | 是否支持流式传输 | 适用场景          |
|-------------------------------|------|---------------------|----------|---------------|
| **REST API**                  | HTTP | JSON / CSV          | ❌ 否      | 适用于轻量级 API 调用 |
| **JDBC / ODBC**               | TCP  | 行式数据                | ❌ 否      | 适用于数据库查询      |
| **Arrow Flight (FastFlight)** | gRPC | 列式数据 (Apache Arrow) | ✅ 是      | 适用于大规模数据流     |

FastFlight 采用 **列式存储格式（Apache Arrow）**，避免传统 JSON / CSV 的解析开销，并支持 **零拷贝传输**，极大提升数据吞吐量。

---

## **🏗️ 最佳部署策略**

为了充分利用 FastFlight 的高吞吐特性，建议采用 **数据库亲和性部署**：

✅ **在数据库附近部署 Flight Server**，减少 JDBC / ODBC 远程调用的延迟。  
✅ **使用 Arrow Flight 作为 API 层**，避免传统 REST API JSON 解析带来的开销。  
✅ **开启流式传输**，让客户端按需获取数据，而非一次性加载。

---

## **🛠 扩展 FastFlight**

### **自定义数据源 (BaseDataService)**

开发者可以通过继承 `BaseDataService` 扩展数据源，例如支持 Kafka、MongoDB、Elasticsearch：

```python
from fastflight.service import BaseDataService


class CustomService(BaseDataService):
    def fetch_data(self, request):
        return get_custom_data(request)  # 替换为实际数据获取逻辑
```

注册数据服务：

```python
server.register_service(CustomService(), "custom_dataset")
```

---

## **📖 相关文档**

- **[CLI 指南](./CLI_USAGE.md)** – FastFlight 命令行工具使用说明。
- **[FastAPI 集成指南](./fastapi/README.md)** – 如何将 Arrow Flight 作为 REST API 暴露。
- **[性能基准测试](./docs/BENCHMARK.md)** – FastFlight 与传统 API 方案的性能对比。

---

## **📌 总结**

- **FastFlight 提供比 REST API / JDBC 更高效的数据传输方案**，适用于大规模数据查询。
- **采用 `kind` 机制优化 Ticket，使请求结构化、可读、可扩展**。
- **支持异步流式数据消费，提高吞吐量，减少内存占用**。
- **适用于金融、数据分析、日志处理等高并发数据场景**。

🚀 **立即开始使用 FastFlight，优化你的数据传输效率！**
</file>
