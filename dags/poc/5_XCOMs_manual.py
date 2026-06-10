from curses import raw
from typing import Any

from airflow.sdk import dag, task


@dag(dag_id="xcoms_manual", schedule=None, catchup=False)
def xcoms_manual():

    @task.python
    def fetch_data(**kwargs):
        # Extracting ti from kwargs to push Xcoms manyally
        ti = kwargs["ti"]

        print("Extracting data... This is the first task")
        fetch_data = {"data": [1, 2, 3, 4, 5]}

        # Pushing the data using a custom key to pull it in the next task
        ti.xcom_push(key="manual_return_result", value=fetch_data)

    @task.python
    def transformed_data(**kwargs):
        # Extracting ti from kwargs to pull Xcoms manyally
        ti = kwargs["ti"]

        print("Transforming data... This is the second task")

        # Pulling the data using the same key used to push it in the previous task
        data = ti.xcom_pull(key="manual_return_result", task_ids="fetch_data")

        # Changed to return a dictionary containing a list
        transformed = {"data": [x * 2 for x in data["data"]]}
        ti.xcom_push(key="manual_transformed_result", value=transformed)

    @task.python
    def load_data(**kwargs):
        # Extracting ti from kwargs to pull Xcoms manyally
        ti = kwargs["ti"]

        # Pulling the data using the same key used to push it in the previous task
        data = ti.xcom_pull(
            key="manual_transformed_result", task_ids="transformed_data"
        )

        print(f"Loading data: {data}")

    # Defining task dependencies
    fetch = fetch_data()
    transfromed = transformed_data()
    load = load_data()

    fetch >> transfromed >> load


xcoms_manual()
