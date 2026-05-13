from curses import raw
from typing import Any

from airflow.sdk import dag, task


@dag(dag_id="xcoms_auto_dag", schedule=None, catchup=False)
def xcoms_auto_dag():

    @task.python
    def fetch_data() -> dict:
        print("Extracting data... This is the first task")
        return {"data": [1, 2, 3, 4, 5]}

    @task.python
    def transformed_data(data: dict | Any) -> dict:
        # Using dict | Any satisfies the linter at parse time
        # while keeping context for what it is at run time.
        print("Transforming data... This is the second task")

        # Changed to return a dictionary containing a list
        return {"data": [x * 2 for x in data["data"]]}

    @task.python
    def load_data(data: dict | Any):
        print(f"Loading data: {data}")
        return data

    # Defining task dependencies
    raw_data = fetch_data()
    transformed = transformed_data(raw_data)
    load_data(transformed)


xcoms_auto_dag()
