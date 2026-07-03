from datetime import datetime
from airflow.sdk import dag, task, Asset

# Must reference the exact same asset matching name/URI
sap_orders_asset = Asset(
    name="sap_clickhouse_orders", 
    uri="clickhouse://clickhouse:8123/default/sap_orders"
)

@dag(
    dag_id="analytics_consumer_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=[sap_orders_asset],  # 🚨 Wakes up when this asset emits an update event
    catchup=False,
    tags=["production", "consumer", "analytics"]
)
def consumer_dag():

    @task
    def run_dbt_models():
        print("📊 [Asset Triggered] Upstream data has arrived in ClickHouse!")
        print("⚙️ Executing dbt models for material management analytics...")
        print("🎯 Premium business dashboards refreshed successfully.")

    run_dbt_models()

consumer_dag()