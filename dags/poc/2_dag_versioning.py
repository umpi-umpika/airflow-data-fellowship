from airflow.sdk import dag, task


@dag(
    dag_id="versioning_dag",
)
def versioning_dag():

    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")

    @task.python
    def third_task():
        print("This is the third task. DAG complete!")

    ## New task added in version 2.0 of the DAG after show first version of the DAG in Airflow UI
    @task.python
    def version_task():
        print("This is the version task. This DAG has been updated to version 2.0!")

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    ## New task added in version 2.0 of the DAG after show first version of the DAG in Airflow UI
    # version = version_task()

    first >> second >> third
    # first >> second >> third >> version


# Instantiating the DAG
versioning_dag()
