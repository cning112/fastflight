from enum import Enum


class DataSource(str, Enum):
    SQL = "sql"
    NoSQL = "nosql"
