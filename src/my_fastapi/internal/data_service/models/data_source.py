from enum import Enum


class DataSourceKind(str, Enum):
    SQL = "sql"
    NoSQL = "nosql"
