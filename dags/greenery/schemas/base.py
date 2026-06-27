"""Helper functions for schemas."""

import pyarrow as pa


def make_pyarrow_schema(columns: list) -> pa.Schema:
    """Create a PyArrow schema from column definitions."""
    return pa.schema([pa.field(name, typ) for name, typ, *_ in columns])


def make_clickhouse_ddl(
    database: str,
    table: str,
    columns: list,
    order_by: str,
) -> str:
    """Create a ClickHouse CREATE TABLE DDL string."""
    cols = ",\n    ".join(
        f"{name} {ch_type}" for name, _, ch_type, *_ in columns
    )
    return f"""CREATE TABLE IF NOT EXISTS {database}.{table} (
    {cols}
) ENGINE = MergeTree()
ORDER BY {order_by}"""
