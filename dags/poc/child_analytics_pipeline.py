from datetime import datetime
from airflow.sdk import dag, task

@dag(
    dag_id="child_analytics_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # 🚨 Crucial: Set to None so it only runs when triggered remotely
    catchup=False,
    tags=["production", "child", "analytics"]
)
def analytics_dag():

    @task
    def process_analytics(dag_run=None):
        # 📥 Extract the custom payload sent by the parent DAG
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        
        source = conf.get("source_pipeline", "UNKNOWN")
        target_date = conf.get("target_partition_date", "N/A")
        file_count = conf.get("processed_files", 0)
        
        print(f"📥 [Child Received] Triggered by master controller: {source}")
        print(f"📊 Running analytics model for partition date: {target_date}")
        print(f"⚙️ Crunching metrics across {file_count} certified clean files...")
        print("🎯 AI Model execution completed successfully!")

    process_analytics()

analytics_dag()