"""
Greenery Orders Ingest Pipeline
================================
CSV (local) → MinIO raw/ (CSV) → MinIO processed/ (Parquet) → ClickHouse greenery.orders
"""

from datetime import datetime
from io import BytesIO

import pyarrow as pa
import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

MINIO_CONN_ID = "minio_default"
MINIO_BUCKET = "greenery"

CSV_PATH = "/opt/airflow/docs/greenery/orders.csv"

CLICKHOUSE_CONN_ID = "clickhouse_conn"
CLICKHOUSE_DB = "greenery"
CLICKHOUSE_TABLE = "orders"


@dag(
    schedule=None,
    dag_id="greenery_ingest_orders",
    start_date=datetime(2026, 1, 1),
    tags=["greenery", "ingest", "orders"],
    catchup=False,
    doc_md=__doc__,
)
def greenery_ingest_orders():
    """Load orders.csv → MinIO raw → Parquet → MinIO processed → ClickHouse."""

    @task
    def upload_csv_to_minio_raw(ds=None):
        """Read orders.csv from local filesystem and upload to MinIO raw layer."""
        raw_key = f"raw/orders/{ds}/orders.csv"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        # Create bucket if it doesn't exist
        if not s3_hook.check_for_bucket(bucket_name=MINIO_BUCKET):
            s3_hook.create_bucket(bucket_name=MINIO_BUCKET)
            print(f"📁 Created bucket '{MINIO_BUCKET}'")

        s3_hook.load_file(
            filename=CSV_PATH,
            key=raw_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        print(f"✅ Uploaded CSV to s3://{MINIO_BUCKET}/{raw_key}")
        return raw_key

    @task
    def csv_to_parquet(raw_key: str, ds=None):
        """Download raw CSV from MinIO, convert to Parquet, upload to processed layer."""
        processed_key = f"processed/orders/{ds}/orders.parquet"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        # Download CSV from MinIO
        csv_obj = s3_hook.get_key(key=raw_key, bucket_name=MINIO_BUCKET)
        csv_bytes = csv_obj.get()["Body"].read()

        # Parse CSV to PyArrow Table with explicit schema
        schema = pa.schema([
            ("order_id", pa.string()),
            ("user_id", pa.string()),
            ("promo_id", pa.string()),
            ("address_id", pa.string()),
            ("created_at", pa.string()),
            ("order_cost", pa.float64()),
            ("shipping_cost", pa.float64()),
            ("order_total", pa.float64()),
            ("tracking_id", pa.string()),
            ("shipping_service", pa.string()),
            ("estimated_delivery_at", pa.string()),
            ("delivered_at", pa.string()),
            ("status", pa.string()),
        ])

        convert_options = pcsv.ConvertOptions(column_types=schema)
        table = pcsv.read_csv(BytesIO(csv_bytes), convert_options=convert_options)

        print(f"📊 Parsed {table.num_rows} rows, {table.num_columns} columns")

        # Write Parquet to buffer
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        # Upload Parquet to MinIO processed layer
        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=processed_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        print(f"✅ Uploaded Parquet to s3://{MINIO_BUCKET}/{processed_key}")
        return processed_key

    @task
    def create_clickhouse_table():
        """Create greenery database and orders table in ClickHouse if not exists."""
        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()
        client.query(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} (
            order_id          String,
            user_id           String,
            promo_id          Nullable(String),
            address_id        String,
            created_at        Nullable(String),
            order_cost        Float64,
            shipping_cost     Float64,
            order_total       Float64,
            tracking_id       Nullable(String),
            shipping_service  Nullable(String),
            estimated_delivery_at  Nullable(String),
            delivered_at      Nullable(String),
            status            String
        ) ENGINE = MergeTree()
        ORDER BY (order_id)
        """
        client.query(create_table_sql)
        print(f"✅ Table {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} ready")

    @task
    def load_parquet_to_clickhouse(processed_key: str):
        """Download Parquet from MinIO processed layer and insert into ClickHouse."""
        # Download Parquet from MinIO
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        parquet_obj = s3_hook.get_key(key=processed_key, bucket_name=MINIO_BUCKET)
        parquet_bytes = parquet_obj.get()["Body"].read()

        # Read Parquet into PyArrow Table
        table = pq.read_table(BytesIO(parquet_bytes))
        print(f"📊 Read {table.num_rows} rows from Parquet")

        # Convert to list of tuples for clickhouse-driver (used by ClickHouseHook)
        columns = table.column_names
        rows = []
        for i in range(table.num_rows):
            row = tuple(table.column(col)[i].as_py() for col in columns)
            rows.append(row)

        # Insert into ClickHouse via hook
        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()

        # Truncate before full load to avoid duplicates
        client.query(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

        # Insert data using clickhouse-connect client.insert method
        # clickhouse-connect client expects rows to be a list of lists/tuples, column_names=columns
        client.insert(
            f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}",
            rows,
            column_names=columns
        )

        # Verify
        result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
        count = result.result_rows[0][0]
        print(f"✅ Loaded {count} rows into {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

    # DAG task dependencies
    raw_key = upload_csv_to_minio_raw()
    processed_key = csv_to_parquet(raw_key)
    table_ready = create_clickhouse_table()
    load_parquet_to_clickhouse(processed_key) << table_ready


greenery_ingest_orders()
