"""
18_task_groups.py — TaskGroup
Module 2, Phase 4: Organization & Dynamic

TaskGroup จัดระเบียบ Tasks ให้เป็นลำดับชั้นใน Graph View
ช่วยลดความซับซ้อนของภาพรวม DAG
"""
import datetime

from airflow.sdk import DAG
from airflow.sdk import task_group
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator


with DAG(
    dag_id="task_group_demo",
    start_date=datetime.datetime(2026, 6, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1},
    tags=["task_group"],
):

    @task_group(default_args={"retries": 3})
    def group1():
        """This docstring will become the tooltip for the TaskGroup."""
        task1 = EmptyOperator(task_id="task1")
        task2 = BashOperator(task_id="task2", bash_command="echo Hello World!", retries=2)
        print(task1.retries)  # 3
        print(task2.retries)  # 2

    @task_group()
    def group2():
        """Second group of tasks."""
        task3 = EmptyOperator(task_id="task3")
        task4 = EmptyOperator(task_id="task4")

    task5 = EmptyOperator(task_id="final_task")

    group1() >> group2() >> task5
