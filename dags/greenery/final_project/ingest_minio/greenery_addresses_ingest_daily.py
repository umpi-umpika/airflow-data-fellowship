"""
Greenery Addresses Ingest Pipeline
==================================
CSV (local) → MinIO raw/ (CSV)
"""

from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import dag, task, Asset

addresses_raw_asset = Asset("s3://greenery/raw/addresses")

MINIO_CONN_ID = "minio_conn"
MINIO_BUCKET = "greenery"
CSV_PATH = "/opt/airflow/docs/greenery/addresses.csv"


@dag(
    schedule="@daily",
    dag_id="greenery_addresses_ingest_daily",
    start_date=datetime(2026, 1, 1),
    tags=["greenery", "ingest", "addresses"],
    catchup=False,
    doc_md=__doc__,
)
def greenery_addresses_ingest_daily():
    """Load addresses.csv → MinIO raw."""

    @task(outlets=[addresses_raw_asset])
    def upload_csv_to_minio_raw(ds=None) -> None:
        """Read addresses.csv from local filesystem and upload to MinIO raw layer."""
        raw_key = f"raw/addresses/{ds}/addresses.csv"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

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

    upload_csv_to_minio_raw()


greenery_addresses_ingest_daily()
