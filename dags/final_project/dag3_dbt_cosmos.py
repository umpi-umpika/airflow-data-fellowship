"""
DAG 3: dbt Run via Astronomer Cosmos

Triggered by the 'ch_landing_ready' asset from DAG 2.
Runs the dbt project (staging views → mart tables) against ClickHouse
using Astronomer Cosmos, which renders each dbt model as an Airflow task.
"""

from datetime import datetime
from pathlib import Path

from cosmos import DbtDag, ProjectConfig, ProfileConfig, RenderConfig
from cosmos.constants import TestBehavior, LoadMode

from final_project.dag2_process_and_load import ch_landing_ready

# ---------------------------------------------------------------------------
# DbtDag — scheduled on the ch_landing_ready asset
# ---------------------------------------------------------------------------
dag3_dbt_cosmos = DbtDag(
    dag_id="final_dag3_dbt_cosmos",
    schedule=(ch_landing_ready,),
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
        # Emit dbt test nodes as Airflow tasks after each model
        test_behavior=TestBehavior.AFTER_EACH,
        # Use dbt ls to discover models
        load_method=LoadMode.DBT_LS,
    ),
    operator_args={
        # Install dbt deps before running
        "install_deps": True,
    },
    tags=["final_project", "dbt", "clickhouse", "cosmos"],
    doc_md=__doc__,
)
