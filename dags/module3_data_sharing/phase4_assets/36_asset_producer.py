from airflow.sdk import dag, task, Asset
from datetime import datetime


# สร้างป้ายชื่อ Asset เตรียมไว้
customer_table = Asset("file://db/customers")


# ---------------------------------------------------------
# DAG 1: คนสร้างข้อมูล (Producer)
# ---------------------------------------------------------
@dag(
    dag_id='36_extract_customer_dag',
    start_date=datetime(2026, 6, 6),
    schedule="@daily",
    catchup=False,
    tags=['asset', 'producer']
)
def extract_customer_dag():
    
    # พอทำ Task นี้เสร็จ มันจะประกาศว่า customer_table อัปเดตแล้ว!
    @task(outlets=[customer_table]) 
    def pull_data_from_api():
        print("ดึงข้อมูลเสร็จแล้ว!")

    pull_data_from_api()


extract_customer_dag()
