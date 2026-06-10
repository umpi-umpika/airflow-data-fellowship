"""
DAG: dbt_clickhouse_cosmos

Runs the dbt project against ClickHouse using Astronomer Cosmos.
This DAG will render each dbt model as an individual Airflow task,
preserving the dbt dependency graph.
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior, LoadMode

dbt_clickhouse_cosmos = DbtDag(
    dag_id="dbt_clickhouse_cosmos",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    project_config=ProjectConfig(
        dbt_project_path="/opt/airflow/dbt",
    ),
    profile_config=ProfileConfig(
        profile_name="airflow_data_fellowship",
        target_name="dev",
        profiles_yml_filepath=Path("/opt/airflow/dbt/profiles.yml"),
    ),
    render_config=RenderConfig(
        # Emit dbt test nodes as Airflow tasks
        test_behavior=TestBehavior.AFTER_EACH,
        # Use custom load mode to avoid dbt ls parsing issues
        load_method=LoadMode.DBT_LS,
    ),
    operator_args={
        # Install dbt deps before running (if you add packages.yml later)
        "install_deps": True,
    },
    tags=["dbt", "clickhouse"],
)
