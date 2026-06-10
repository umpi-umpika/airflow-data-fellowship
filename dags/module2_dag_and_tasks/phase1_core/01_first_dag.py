"""
Module 2 Phase 1 — Declaring a DAG (3 ways)
=============================================
Demonstrates the three ways to declare a DAG in Airflow:
1. with statement (context manager)
2. Standard constructor
3. @dag decorator
"""

import datetime

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, dag

# ──────────────────────────────────────────────
# Method 1: with statement (context manager)
# ──────────────────────────────────────────────
with DAG(
    dag_id="01_first_dag",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["module2", "phase1", "declaring"],
):
    EmptyOperator(task_id="task")

# ──────────────────────────────────────────────
# Method 2: Standard constructor
# ──────────────────────────────────────────────
my_dag = DAG(
    dag_id="01_first_dag_constructor",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["module2", "phase1", "declaring"],
)
EmptyOperator(task_id="task", dag=my_dag)

# ──────────────────────────────────────────────
# Method 3: @dag decorator (recommended)
# ──────────────────────────────────────────────


@dag(
    dag_id="01_first_dag_decorator",
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["module2", "phase1", "declaring"],
)
def generate_dag():
    EmptyOperator(task_id="task")


generate_dag()
