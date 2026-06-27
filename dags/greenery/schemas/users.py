"""Users schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "users"

USER_COLUMNS = [
    ("user_id",      pa.string(), "String",            False),
    ("first_name",   pa.string(), "String",            False),
    ("last_name",    pa.string(), "String",            False),
    ("email",        pa.string(), "String",            False),
    ("phone_number", pa.string(), "String",            False),
    ("created_at",   pa.string(), "Nullable(String)",  True),
    ("updated_at",   pa.string(), "Nullable(String)",  True),
    ("address_id",   pa.string(), "String",            False),
]


def pyarrow_schema():
    return make_pyarrow_schema(USER_COLUMNS)


USER_COLUMN_NAMES = [c[0] for c in USER_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, USER_COLUMNS, "(user_id)")
