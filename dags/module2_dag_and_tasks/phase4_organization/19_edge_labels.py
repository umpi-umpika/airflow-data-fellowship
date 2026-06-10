"""
19_edge_labels.py — Edge Labels
Module 2, Phase 4: Organization & Dynamic

ใส่ Label บนเส้น Dependency ใน Graph View
เหมาะสำหรับ Branching เพื่อระบุเงื่อนไขแต่ละเส้นทาง
"""
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label


with DAG(
    "example_branch_labels",
    schedule="@daily",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["edge_labels"],
) as dag:
    ingest = EmptyOperator(task_id="ingest")
    analyse = EmptyOperator(task_id="analyze")
    check = EmptyOperator(task_id="check_integrity")
    describe = EmptyOperator(task_id="describe_integrity")
    error = EmptyOperator(task_id="email_error")
    save = EmptyOperator(task_id="save")
    report = EmptyOperator(task_id="report")

    ingest >> analyse >> check
    check >> Label("No errors") >> save >> report
    check >> Label("Errors found") >> describe >> error >> report
