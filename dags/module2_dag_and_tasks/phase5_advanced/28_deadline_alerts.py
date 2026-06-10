"""
28_deadline_alerts.py — Deadline Alerts & execution_timeout
Module 2, Phase 5: Advanced Interactions

ตั้ง Deadline ให้ Task: ถ้ารันเกินเวลาที่กำหนด → สั่ง Fail
+ on_failure_callback ส่งแจ้งเตือนอัตโนมัติ
"""
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import time


# 1. ฟังก์ชันสำหรับ Callback เมื่อเกิด Deadline (หรือ Task พัง)
def alert_callback(context):
    task_instance = context.get('task_instance')
    print(f"🚨 ALERT: Task {task_instance.task_id} มีปัญหา! ตรวจสอบด่วนที่ {datetime.now()}")
    # ตรงนี้คุณสามารถใส่โค้ดส่ง Slack หรือ Email ได้เลย


@dag(
    dag_id="deadline_alert_demo",
    start_date=datetime(2026, 6, 6),
    schedule="@daily",
    catchup=False,
    # นี่คือการกำหนดว่าถ้า Task ใน DAG นี้พัง ให้รันฟังก์ชัน callback
    default_args={'on_failure_callback': alert_callback},
    tags=["deadline"],
)
def deadline_alert_demo():

    @task(
        task_id="long_running_task",
        # กำหนด timeout ของ Task หากรันเกิน 60 วินาที ให้สั่ง Fail ทันที
        execution_timeout=timedelta(seconds=60)
    )
    def long_task():
        print("เริ่มงานที่ใช้เวลานาน...")
        time.sleep(70)  # จงใจให้เกินเวลาที่ตั้งไว้ (60 วินาที)
        print("งานเสร็จสิ้น")

    long_task()


deadline_alert_demo()
