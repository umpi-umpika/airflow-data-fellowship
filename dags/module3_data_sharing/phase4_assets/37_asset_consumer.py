from airflow.sdk import dag, task, Asset
from datetime import datetime


# ต้องสร้าง Asset เดียวกันกับ Producer เพื่อให้ Airflow รู้ว่าเชื่อมต่อกัน
customer_table = Asset("file://db/customers")


# ---------------------------------------------------------
# DAG 2: คนรอใช้ข้อมูล (Consumer)
# ---------------------------------------------------------
# DAG นี้ไม่มีเวลา (Cron) แต่จะรันทันทีที่ DAG 1 ทำ customer_table เสร็จ
@dag(
    dag_id='37_make_report_dag',
    start_date=datetime(2026, 6, 6),
    schedule=[customer_table],
    catchup=False,
    tags=['asset', 'consumer']
)
def make_report_dag():
    
    @task
    def calculate_sales():
        print("คำนวณยอดขายจากตารางลูกค้า...")

    calculate_sales()


make_report_dag()
