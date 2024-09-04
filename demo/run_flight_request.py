import httpx
import pyarrow as pa

if __name__ == "__main__":
    # make sure the main fastapi app has started

    resp = httpx.post("http://127.0.0.1:8000/fastflight/", json={"kind": "sql", "query": "select 1 as a"})
    content = resp.content

    reader = pa.ipc.open_stream(content)
    arrow_table = reader.read_all()

    # 转换为 Pandas DataFrame
    df = arrow_table.to_pandas()
    print(df)
