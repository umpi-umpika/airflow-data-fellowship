"""Addresses schema."""

import pyarrow as pa
from .base import make_pyarrow_schema, make_clickhouse_ddl

TABLE = "addresses"

ADDRESS_COLUMNS = [
    ("address_id", pa.string(), "String",   False),
    ("address",    pa.string(), "String",   False),
    ("zipcode",    pa.string(), "String",   False),
    ("state",      pa.string(), "String",   False),
    ("country",    pa.string(), "String",   False),
]


def pyarrow_schema():
    return make_pyarrow_schema(ADDRESS_COLUMNS)


ADDRESS_COLUMN_NAMES = [c[0] for c in ADDRESS_COLUMNS]


def clickhouse_ddl(database: str, table: str = TABLE) -> str:
    return make_clickhouse_ddl(database, table, ADDRESS_COLUMNS, "(address_id)")
