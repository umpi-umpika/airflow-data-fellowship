import os

import pendulum
from airflow.sdk import asset, dag, task
from asset_13 import fetch_data


@asset(
    schedule=fetch_data,
    # This is optional but good to include for clarity about the asset's location
    # it can be anything database name or file path, etc. depending on the asset type
    uri="/opt/airflow/logs/data/data_processed.txt",
    name="process_data",
)
def process_data(self):
    # Ensure the directory exists
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)

    # Simulate data fetching and write to a file
    with open(self.uri, "w") as f:
        f.write(f"Data procressed at {pendulum.now('Asia/Bangkok')}\n")

    print(f"Data processed to {self.uri}")
