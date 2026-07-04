"""Events schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "events"

EVENT_COLUMNS = [
    ("event_id",    pa.string(), "String",            False),
    ("session_id",  pa.string(), "String",            False),
    ("user_id",     pa.string(), "String",            False),
    ("page_url",    pa.string(), "String",            False),
    ("created_at",  pa.string(), "Nullable(String)",  True),
    ("event_type",  pa.string(), "String",            False),
    ("order_id",    pa.string(), "Nullable(String)",  True),
    ("product_id",  pa.string(), "Nullable(String)",  True),
]


def pyarrow_schema():
    return make_pyarrow_schema(EVENT_COLUMNS)


EVENT_COLUMN_NAMES = [c[0] for c in EVENT_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, EVENT_COLUMNS, "(event_id)")
