from datetime import datetime
from airflow.sdk import dag, task, Asset  # 🚨 Airflow 3.0 SDK imports

# Define the central data asset with a unique URI
sap_orders_asset = Asset(
    name="sap_clickhouse_orders", 
    uri="clickhouse://clickhouse:8123/default/sap_orders"
)

@dag(
    dag_id="sap_producer_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "producer"]
)
def producer_dag():

    # 🚨 The 'outlets' parameter tells Airflow this task updates the asset
    @task(outlets=[sap_orders_asset])
    def extract_sap_to_clickhouse():
        print("📥 Fetching Purchase Orders from SAP module...")
        print("🚀 Bulk inserting raw rows into ClickHouse default.sap_orders table.")
        print("✅ Data landed safely. Emitting asset update event!")

    extract_sap_to_clickhouse()

producer_dag()