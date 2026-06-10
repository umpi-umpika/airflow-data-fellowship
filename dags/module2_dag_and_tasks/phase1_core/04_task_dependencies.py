"""
Module 2 Phase 1 — Task Dependencies
======================================
Demonstrates two ways to define task dependencies:
1. >> and << operators (recommended)
2. set_upstream() and set_downstream() (explicit methods)

From the curriculum:
  first_task >> [second_task, third_task]
  third_task << fourth_task
  --- is equivalent to ---
  first_task.set_downstream([second_task, third_task])
  third_task.set_upstream(fourth_task)
"""

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import dag
from pendulum import datetime


@dag(
    dag_id="04_task_dependencies",
    start_date=datetime(2026, 1, 1, tz="Asia/Bangkok"),
    schedule="@daily",
    catchup=False,
    tags=["module2", "phase1", "dependencies"],
)
def task_dependencies_demo():

    first_task = EmptyOperator(task_id="first_task")
    second_task = EmptyOperator(task_id="second_task")
    third_task = EmptyOperator(task_id="third_task")
    fourth_task = EmptyOperator(task_id="fourth_task")

    # ── Method 1: >> and << operators (recommended) ──
    first_task >> [second_task, third_task]
    third_task << fourth_task

    # The above is equivalent to:
    # ── Method 2: Explicit methods ──
    # first_task.set_downstream([second_task, third_task])
    # third_task.set_upstream(fourth_task)


task_dependencies_demo()
