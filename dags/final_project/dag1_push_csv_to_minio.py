"""
DAG 1: Push CSV to MinIO (Raw Zone)

Generates sample sales CSV data and uploads it to the MinIO 'raw-zone' bucket.
Declares the 'csv_ready' asset so downstream DAGs can react when the file lands.
"""

import csv
import io
import os
from datetime import datetime

import pendulum
from airflow.sdk import asset, dag, task

# ---------------------------------------------------------------------------
# MinIO (S3-compatible) client helper
# ---------------------------------------------------------------------------
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"
BUCKET_RAW = "raw-zone"
CSV_PATH = "/tmp/airflow_sales.csv"


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
# Asset definition — uses @asset decorator (Airflow 3.x style)
# ---------------------------------------------------------------------------
@asset(
    schedule="@daily",
    uri="s3://raw-zone/sales.csv",
    name="csv_ready",
)
def csv_ready(self):
    """Generate sample sales CSV and upload to MinIO raw-zone."""
    now = pendulum.now("Asia/Bangkok")

    rows = [
        {"order_id": 1, "product": "Laptop", "quantity": 2, "price": 35000.00, "order_date": now.subtract(days=5).to_date_string()},
        {"order_id": 2, "product": "Mouse", "quantity": 10, "price": 450.00, "order_date": now.subtract(days=4).to_date_string()},
        {"order_id": 3, "product": "Keyboard", "quantity": 5, "price": 1200.00, "order_date": now.subtract(days=3).to_date_string()},
        {"order_id": 4, "product": "Monitor", "quantity": 3, "price": 12000.00, "order_date": now.subtract(days=2).to_date_string()},
        {"order_id": 5, "product": "Headset", "quantity": 8, "price": 2500.00, "order_date": now.subtract(days=1).to_date_string()},
        {"order_id": 6, "product": "Webcam", "quantity": 4, "price": 1800.00, "order_date": now.to_date_string()},
        {"order_id": 7, "product": "USB Hub", "quantity": 15, "price": 650.00, "order_date": now.to_date_string()},
        {"order_id": 8, "product": "SSD 1TB", "quantity": 6, "price": 3200.00, "order_date": now.subtract(days=6).to_date_string()},
        {"order_id": 9, "product": "RAM 16GB", "quantity": 7, "price": 2800.00, "order_date": now.subtract(days=7).to_date_string()},
        {"order_id": 10, "product": "Charger", "quantity": 12, "price": 890.00, "order_date": now.subtract(days=3).to_date_string()},
    ]

    # Write CSV to local file
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "product", "quantity", "price", "order_date"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Generated {len(rows)} rows of sales data to {CSV_PATH}")

    # Upload to MinIO
    s3 = _get_s3_client()

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=BUCKET_RAW)
    except Exception:
        s3.create_bucket(Bucket=BUCKET_RAW)
        print(f"Created bucket: {BUCKET_RAW}")

    with open(CSV_PATH, "rb") as f:
        s3.put_object(
            Bucket=BUCKET_RAW,
            Key="sales.csv",
            Body=f,
            ContentType="text/csv",
        )

    print(f"Uploaded sales.csv to s3://{BUCKET_RAW}/sales.csv")
