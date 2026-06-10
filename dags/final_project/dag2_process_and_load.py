"""
DAG 2: Process CSV and Load to ClickHouse

Triggered by the 'csv_ready' asset from DAG 1.
  1. Downloads the CSV from MinIO raw-zone.
  2. Converts to Parquet and uploads to MinIO processed-zone.
  3. Loads the data into ClickHouse landing table.

Declares the 'ch_landing_ready' asset so the downstream dbt DAG can react.
"""

import csv
import io
import os
from datetime import datetime

import pendulum
from airflow.sdk import asset, dag, task

from final_project.dag1_push_csv_to_minio import csv_ready

# ---------------------------------------------------------------------------
# MinIO config
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_RAW = "raw-zone"
BUCKET_PROCESSED = "processed-zone"


def _get_s3_client():
    """Return a boto3 S3 client pointing at the local MinIO instance."""
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


# ---------------------------------------------------------------------------
# ClickHouse config
# ---------------------------------------------------------------------------
CH_HOST = "clickhouse"
CH_PORT = 9000  # native port for clickhouse_driver
CH_USER = "default"
CH_PASSWORD = "clickhouse"
CH_DATABASE = "default"


# ---------------------------------------------------------------------------
# Asset definition — ClickHouse landing table (Airflow 3.x @asset decorator)
# ---------------------------------------------------------------------------
@asset(
    schedule=csv_ready,
    uri="clickhouse://default/landing_sales",
    name="ch_landing_ready",
)
def ch_landing_ready():
    """Download CSV from MinIO, convert to Parquet, and load into ClickHouse."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from clickhouse_driver import Client as CHClient

    # ----- Step 1: Download CSV from MinIO -----
    s3 = _get_s3_client()
    response = s3.get_object(Bucket=BUCKET_RAW, Key="sales.csv")
    csv_data = response["Body"].read().decode("utf-8")
    print(f"Downloaded sales.csv ({len(csv_data)} bytes) from {BUCKET_RAW}")

    # Parse CSV
    reader = csv.DictReader(io.StringIO(csv_data))
    rows = list(reader)

    # ----- Step 2: Convert to Parquet and upload -----
    table = pa.table({
        "order_id": pa.array([int(r["order_id"]) for r in rows], type=pa.int32()),
        "product": pa.array([r["product"] for r in rows], type=pa.string()),
        "quantity": pa.array([int(r["quantity"]) for r in rows], type=pa.int32()),
        "price": pa.array([float(r["price"]) for r in rows], type=pa.float64()),
        "order_date": pa.array([r["order_date"] for r in rows], type=pa.string()),
    })

    buf = io.BytesIO()
    pq.write_table(table, buf)
    parquet_bytes = buf.getvalue()

    # Upload Parquet to MinIO processed-zone
    try:
        s3.head_bucket(Bucket=BUCKET_PROCESSED)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_PROCESSED)
        print(f"Created bucket: {BUCKET_PROCESSED}")

    s3.put_object(
        Bucket=BUCKET_PROCESSED,
        Key="sales.parquet",
        Body=parquet_bytes,
        ContentType="application/octet-stream",
    )
    print(f"Uploaded sales.parquet ({len(parquet_bytes)} bytes) to {BUCKET_PROCESSED}")

    # ----- Step 3: Load into ClickHouse -----
    ch = CHClient(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )

    # Create the landing table if it doesn't exist
    ch.execute("""
        CREATE TABLE IF NOT EXISTS landing_sales (
            order_id   Int32,
            product    String,
            quantity   Int32,
            price      Float64,
            order_date String
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)

    # Truncate before loading (full refresh)
    ch.execute("TRUNCATE TABLE landing_sales")

    # Insert rows
    insert_rows = [
        (int(r["order_id"]), r["product"], int(r["quantity"]), float(r["price"]), r["order_date"])
        for r in rows
    ]

    ch.execute(
        "INSERT INTO landing_sales (order_id, product, quantity, price, order_date) VALUES",
        insert_rows,
    )
    print(f"Inserted {len(insert_rows)} rows into landing_sales")
