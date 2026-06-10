"""
Module 1 — Airflow Architecture Demo
=====================================
A simple ETL demo DAG that demonstrates core Airflow architecture concepts:
- Scheduler: picks up this DAG and creates DAG Runs on schedule
- Worker: executes the tasks
- Webserver: visualise this DAG in the Airflow UI
- Metadata DB: stores task state, XComs, and run history

Uses @dag decorator style (recommended in Airflow 3.x).
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="module1_architecture_demo",
    start_date=datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    tags=["module1", "architecture", "demo"],
    doc_md=__doc__,
)
def architecture_demo():
    """Demonstrate Airflow architecture with a simple ETL pipeline."""

    # ---------- Extract ----------
    @task.python
    def extract():
        """Simulate data extraction — this task runs on a Worker."""
        print("📥 Extracting data from source system...")
        raw_data = {"orders": [100, 200, 300], "source": "demo_api"}
        print(f"Extracted: {raw_data}")
        return raw_data

    # ---------- Transform (BashOperator) ----------
    transform = BashOperator(
        task_id="transform",
        bash_command=(
            "echo '🔄 Transforming data...' && "
            "echo 'Applying business rules — cleaning & aggregating' && "
            "echo 'Transform complete!'"
        ),
    )

    # ---------- Load ----------
    @task.python
    def load():
        """Simulate loading data into a data warehouse."""
        print("📤 Loading transformed data into the warehouse...")
        print("Data successfully loaded! ✅")

    # ---------- Notify ----------
    @task.bash
    def notify():
        """Send a notification after the pipeline completes."""
        return "echo '🔔 Pipeline finished for {{ ds }}. Check the Airflow UI for details!'"

    # Define task dependencies (the >> operator creates the DAG structure)
    extract() >> transform >> load() >> notify()


# Instantiate the DAG — this must be at module top-level for the
# DAG Processor to discover it.
architecture_demo()
