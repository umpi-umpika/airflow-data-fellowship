from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


def hello_world():
    print("Hello from Airflow!")


with DAG(
    dag_id="example_python_dag",
    start_date=datetime(2026, 5, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello_world,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> hello_task >> end
