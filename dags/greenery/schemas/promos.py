"""Promos schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "promos"

PROMO_COLUMNS = [
    ("promo_id", pa.string(), "String", False),
    ("discount", pa.int64(),  "Int64",  False),
    ("status",   pa.string(), "String", False),
]


def pyarrow_schema():
    return make_pyarrow_schema(PROMO_COLUMNS)


PROMO_COLUMN_NAMES = [c[0] for c in PROMO_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, PROMO_COLUMNS, "(promo_id)")
