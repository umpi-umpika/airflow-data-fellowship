from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task


@dag(
    dag_id="operator_dag",
)
def operator_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.bash
    def run_after_loop():
        return "echo https://airflow.apache.org/"

    run = BashOperator(
        task_id="run_this", bash_command="echo https://airflow.apache.org/"
    )

    # Defining task dependencies
    first = first_task()
    second = second_task()
    bash = run_after_loop()

    first >> second >> bash >> run


# Instantiating the DAG
operator_dag()
