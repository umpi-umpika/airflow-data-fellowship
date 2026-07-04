"""Products schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "products"

PRODUCT_COLUMNS = [
    ("product_id", pa.string(),  "String",  False),
    ("name",       pa.string(),  "String",  False),
    ("price",      pa.float64(), "Float64", False),
    ("inventory",  pa.int64(),   "Int64",   False),
]


def pyarrow_schema():
    return make_pyarrow_schema(PRODUCT_COLUMNS)


PRODUCT_COLUMN_NAMES = [c[0] for c in PRODUCT_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, PRODUCT_COLUMNS, "(product_id)")
