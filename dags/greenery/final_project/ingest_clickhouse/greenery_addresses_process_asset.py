"""
Greenery Addresses Process Pipeline
===================================
MinIO raw/ (CSV) → MinIO processed/ (Parquet) → ClickHouse greenery.addresses
"""

from datetime import datetime
from io import BytesIO
import json

import pyarrow.csv as pcsv
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook
from airflow.sdk import dag, task, Asset
from greenery.schemas.addresses import pyarrow_schema, clickhouse_ddl

MINIO_CONN_ID = "minio_default"
MINIO_BUCKET = "greenery"

CLICKHOUSE_CONN_ID = "clickhouse_conn"
CLICKHOUSE_DB = "greenery"
CLICKHOUSE_TABLE = "addresses"

addresses_raw_asset = Asset("s3://greenery/raw/addresses")
addresses_asset = Asset("clickhouse://greenery/addresses")


@dag(
    schedule=[addresses_raw_asset],
    dag_id="greenery_addresses_process_asset",
    start_date=datetime(2026, 1, 1),
    tags=["greenery", "ingest", "addresses"],
    catchup=False,
    doc_md=__doc__,
)
def greenery_addresses_process_asset():
    """Convert CSV to Parquet and load into ClickHouse."""

    @task
    def csv_to_parquet(**context) -> str:
        """Download raw CSV from MinIO, convert to Parquet, upload to processed layer."""
        asset_events = context["triggering_asset_events"].get(addresses_raw_asset, [])

        print("\n" + "=" * 40 + " ASSET EVENT INFO " + "=" * 40)
        if asset_events:
            event = asset_events[0]
            print(f"🔹 Asset Name:  {event.asset.name}")
            print(f"🔹 Asset URI:   {event.asset.uri}")
            print(f"🔹 Source DAG:  {event.source_dag_id}")
            print(f"🔹 Source Task: {event.source_task_id}")
            print(f"🔹 Source Run:  {event.source_run_id}")
            print(f"🔹 Event Time:  {event.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            extra_meta = getattr(event, "extra", {}) or {}
            print(f"🔹 Custom Extra Meta: {json.dumps(extra_meta)}")

            execution_date = extra_meta.get("ds") or event.timestamp.strftime("%Y-%m-%d")
            print(f"🎯 Resolved Execution Date: {execution_date}")
        else:
            print("⚠️ Task was run manually. Using current system date.")
            execution_date = datetime.now().strftime("%Y-%m-%d")

        print("=" * 98 + "\n")

        raw_key = f"raw/addresses/{execution_date}/addresses.csv"
        processed_key = f"processed/addresses/{execution_date}/addresses.parquet"
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)

        csv_obj = s3_hook.get_key(key=raw_key, bucket_name=MINIO_BUCKET)
        csv_bytes = csv_obj.get()["Body"].read()

        convert_options = pcsv.ConvertOptions(column_types=pyarrow_schema())
        table = pcsv.read_csv(BytesIO(csv_bytes), convert_options=convert_options)

        print(f"📊 Parsed {table.num_rows} rows, {table.num_columns} columns")

        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        parquet_buffer.seek(0)

        s3_hook.load_bytes(
            bytes_data=parquet_buffer.getvalue(),
            key=processed_key,
            bucket_name=MINIO_BUCKET,
            replace=True,
        )
        print(f"✅ Uploaded Parquet to s3://{MINIO_BUCKET}/{processed_key}")
        return processed_key

    @task
    def create_clickhouse_table() -> None:
        """Create greenery database and addresses table in ClickHouse if not exists."""
        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()
        client.query(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DB}")

        client.query(clickhouse_ddl(CLICKHOUSE_DB, CLICKHOUSE_TABLE))
        print(f"✅ Table {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} ready")

    @task(outlets=[addresses_asset])
    def load_parquet_to_clickhouse(processed_key: str) -> None:
        """Download Parquet from MinIO processed layer and insert into ClickHouse."""
        s3_hook = S3Hook(aws_conn_id=MINIO_CONN_ID)
        parquet_obj = s3_hook.get_key(key=processed_key, bucket_name=MINIO_BUCKET)
        parquet_bytes = parquet_obj.get()["Body"].read()

        table = pq.read_table(BytesIO(parquet_bytes))
        print(f"📊 Read {table.num_rows} rows from Parquet")

        columns = table.column_names
        rows = []
        for i in range(table.num_rows):
            row = tuple(table.column(col)[i].as_py() for col in columns)
            rows.append(row)

        ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
        client = ch_hook.get_client()

        client.query(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

        client.insert(f"{CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}", rows, column_names=columns)

        result = client.query(f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
        count = result.result_rows[0][0]
        print(f"✅ Loaded {count} rows into {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")

    # DAG task dependencies
    processed_key = csv_to_parquet()
    table_ready = create_clickhouse_table()
    table_ready >> load_parquet_to_clickhouse(processed_key)


greenery_addresses_process_asset()
