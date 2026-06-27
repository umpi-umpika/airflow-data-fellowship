"""Orders schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "orders"

ORDER_COLUMNS = [
    ("order_id",              pa.string(),   "String",            False),
    ("user_id",               pa.string(),   "String",            False),
    ("promo_id",              pa.string(),   "Nullable(String)",  True),
    ("address_id",            pa.string(),   "String",            False),
    ("created_at",            pa.string(),   "Nullable(String)",  True),
    ("order_cost",            pa.float64(),  "Float64",           False),
    ("shipping_cost",         pa.float64(),  "Float64",           False),
    ("order_total",           pa.float64(),  "Float64",           False),
    ("tracking_id",           pa.string(),   "Nullable(String)",  True),
    ("shipping_service",      pa.string(),   "Nullable(String)",  True),
    ("estimated_delivery_at", pa.string(),   "Nullable(String)",  True),
    ("delivered_at",          pa.string(),   "Nullable(String)",  True),
    ("status",                pa.string(),   "String",            False),
]


def pyarrow_schema():
    return make_pyarrow_schema(ORDER_COLUMNS)


ORDER_COLUMN_NAMES = [c[0] for c in ORDER_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, ORDER_COLUMNS, "(order_id)")
