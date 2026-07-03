from datetime import datetime
from airflow.sdk import dag, task  # Airflow 3.0 SDK standard
# Airflow 3.0 Provider location for standard core operators
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="parent_ingestion_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "parent", "ingestion"]
)
def ingestion_dag():

    @task
    def extract_and_clean_data(logical_date=None):
        ds = logical_date.strftime("%Y-%m-%d")
        print(f"🧹 [Parent Step 1] Extracting raw data from SAP for date: {ds}...")
        print("✅ [Parent Step 2] Validation and cleaning completed.")
        
        # Return operational metrics to pass downstream
        return {
            "partition_date": ds,
            "file_count": 12
        }

    # 🔗 The Remote Control Operator
    trigger_downstream_analytics = TriggerDagRunOperator(
        task_id="trigger_child_analytics",
        trigger_dag_id="child_analytics_pipeline",  # Must match the child's dag_id exactly
        
        # 📦 The dynamic payload wrapped inside the conf argument
        # ดึงค่า XCom จาก task 'extract_and_clean_data' ผ่าน Jinja template
        conf={
            "source_pipeline": "parent_ingestion_pipeline",
            "target_partition_date": "{{ task_instance.xcom_pull(task_ids='extract_and_clean_data')['partition_date'] }}",
            "processed_files": "{{ task_instance.xcom_pull(task_ids='extract_and_clean_data')['file_count'] | int }}"
        },
        
        # 🚦 Control Flow Strategy
        wait_for_completion=False,  # True = รอให้ child run เสร็จสมบูรณ์
        poke_interval=15          # เช็คสถานะ child ทุก 15 วินาที
    )

    # Building the pipeline dependency
    pipeline_metrics = extract_and_clean_data()
    pipeline_metrics >> trigger_downstream_analytics

ingestion_dag()