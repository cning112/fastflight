from enum import Enum


class DataKind(str, Enum):
    SQL = ("sql",)
    NO_SQL = ("nosql",)
    CSV = ("csv",)
