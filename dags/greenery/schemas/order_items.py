"""Order items schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "order_items"

ORDER_ITEM_COLUMNS = [
    ("order_id",   pa.string(), "String", False),
    ("product_id", pa.string(), "String", False),
    ("quantity",   pa.int64(),  "Int64",  False),
]


def pyarrow_schema():
    return make_pyarrow_schema(ORDER_ITEM_COLUMNS)


ORDER_ITEM_COLUMN_NAMES = [c[0] for c in ORDER_ITEM_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, ORDER_ITEM_COLUMNS, "(order_id, product_id)")
