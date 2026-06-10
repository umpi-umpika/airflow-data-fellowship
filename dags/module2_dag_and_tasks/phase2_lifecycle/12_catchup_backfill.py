"""
Module 2 Phase 2 — Catchup & Backfill
========================================
Demonstrates the difference between catchup=True and catchup=False.

From the curriculum:
  • catchup=True  → Airflow auto-creates DAG Runs for ALL past intervals
                    since start_date (e.g. sales reports that need historical data)
  • catchup=False → Airflow only creates a DAG Run for the LATEST interval
                    (e.g. real-time alerts that don't need history)

  Backfill is a separate manual process triggered via UI or CLI:
    airflow backfill create --dag-id DAG_ID --start-date ... --end-date ...
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from datetime import datetime

# ──────────────────────────────────────────────
# DAG 1: catchup=True
# ──────────────────────────────────────────────
# This DAG will backfill all missed runs from start_date to today.
# Suitable for: daily sales reports, historical data processing.
with DAG(
    dag_id="daily_sales_report_catchup",
    start_date=datetime(2026, 6, 1),  # Runs will be created from June 1 to present
    schedule="@daily",
    catchup=True,  # Important: backfill all past intervals
    tags=["module2", "phase2", "catchup"],
) as dag_catchup:

    # This task runs once per day for every day since start_date
    generate_report = BashOperator(
        task_id="generate_report",
        bash_command="echo 'Generating sales report for {{ ds }}'",
    )

# ──────────────────────────────────────────────
# DAG 2: catchup=False
# ──────────────────────────────────────────────
# This DAG only runs for the most recent interval.
# Suitable for: system alerts, real-time notifications.
with DAG(
    dag_id="system_alert_no_catchup",
    start_date=datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,  # Important: ignore past intervals
    tags=["module2", "phase2", "catchup"],
) as dag_no_catchup:

    # This task only runs from today onwards (no backfill)
    send_alert = BashOperator(
        task_id="send_alert",
        bash_command="echo 'Sending alert for today: {{ ds }}'",
    )
