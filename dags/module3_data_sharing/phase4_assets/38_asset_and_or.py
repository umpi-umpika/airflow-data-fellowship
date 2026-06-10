from airflow.sdk import dag, task, Asset
from datetime import datetime


sales_data = Asset("s3://bucket/sales.csv")
marketing_data = Asset("s3://bucket/marketing.csv")
urgent_fix_data = Asset("s3://bucket/hotfix.csv")


# ตัวอย่างที่ 1: ต้องรอทั้ง Sales และ Marketing มาครบ (AND)
@dag(
    dag_id='standard_report_dag',
    start_date=datetime(2026, 6, 6),
    schedule=[sales_data, marketing_data],
    catchup=False,
    tags=['asset', 'and_condition']
)
def standard_report_dag():

    @task
    def generate_standard_report():
        print("ข้อมูล Sales และ Marketing มาครบแล้ว กำลังสร้างรายงาน...")

    generate_standard_report()


standard_report_dag()


# ตัวอย่างที่ 2: ถ้ามีอัปเดตจาก Sales หรือ Marketing ตัวใดตัวหนึ่ง ให้รันเลย (OR)
@dag(
    dag_id='fast_update_report_dag',
    start_date=datetime(2026, 6, 6),
    schedule=(sales_data | marketing_data),
    catchup=False,
    tags=['asset', 'or_condition']
)
def fast_update_report_dag():

    @task
    def generate_fast_report():
        print("มีข้อมูลอัปเดตจาก Sales หรือ Marketing กำลังรีเฟรชรายงาน...")

    generate_fast_report()


fast_update_report_dag()


# ตัวอย่างที่ 3: คอมโบขั้นสุด (ประยุกต์)
# ความหมาย: ปกติต้องรอครบ 2 ตัวนะ (Sales + Marketing) 
# แต่!! ถ้ามีไฟล์ hotfix ส่งมาด่วนเมื่อไหร่ ให้ลัดคิวรันได้เลยไม่ต้องรอใคร!
@dag(
    dag_id='complex_report_dag',
    start_date=datetime(2026, 6, 6),
    schedule=(sales_data & marketing_data) | urgent_fix_data,
    catchup=False,
    tags=['asset', 'combo_condition']
)
def complex_report_dag():

    @task
    def generate_complex_report():
        print("กำลังสร้างรายงานแบบคอมโบ (AND/OR ผสม)...")

    generate_complex_report()


complex_report_dag()
