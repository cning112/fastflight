import pandas as pd
from fastapi.testclient import TestClient

from my_fastapi.main import app

client = TestClient(app)
client.base_url = client.base_url.join('/api')


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello World"}


def test_hello():
    response = client.get("/hello/Loong")
    assert response.status_code == 200
    assert response.json() == {"message": "Hello Loong"}


def test_settings():
    response = client.get("/settings")
    assert response.status_code == 200
    assert response.json() == {"env": "local"}


def test_csv_df():
    df = pd.DataFrame(data={"a": [1, 2, 3], "b": [2, 3, 4], "c": [5, 6, 7]})
    response = client.post(
        "/pd_data/csv/abc.csv",
        # headers={'Content-type': 'application/octet-stream'},
        headers={"Content-type": "text/csv"},
        content=df.to_csv(),
        params={"index_col": "a", "y_column": "b", "x_columns": ["c"]},
    )
    # assert response.status_code == 200
    assert response.json() == {
        "filename": "abc.csv",
        "y_column": "b",
        "x_columns": ["c"],
        "df.index": "a",
        "len(df)": 3,
    }
