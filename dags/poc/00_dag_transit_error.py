import random
import time
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# 🧪 ฟังก์ชันสุ่มโรยเกลือ (Jitter) ดักหน้างานก่อนรันจริง
def apply_jitter_before_run(context):
    try_number = context["task_instance"].try_number # รอบการลองปัจจุบัน
    
    if try_number > 1: # ให้ Jitter ทำงานเฉพาะตอนเกิดการ Retry เท่านั้น
        base_delay = 5
        max_backoff = base_delay * (2 ** (try_number - 1))

        # ล็อกเวลาฐานไว้ครึ่งหนึ่งชัวร์ ๆ + สุ่มเนื้อเค้กเติมเข้าไปอีกครึ่งหนึ่ง
        # วิธีนี้จะทำให้รอบท้าย ๆ เวลารอจะขยับยาวขึ้นแน่นอน ไม่สุ่มหลุดมาหลักหน่วยแล้วครับ
        half_backoff = max_backoff / 2
        random_sleep = half_backoff + random.uniform(0, half_backoff)
        
        print(f"🚨 Retry attempt #{try_number}: Anti-Thundering Herd activated!")
        print(f"🤖 Worker is sleeping for {random_sleep:.2f} seconds to dissolve the crowd...")
        time.sleep(random_sleep)

default_args = {
    "owner": "boss",
    "retries": 4,
    "retry_delay": timedelta(seconds=2), # ให้ค่าไทป์ผ่าน Validation ฉลุย (คงที่ไว้สั้นๆ)
}

with DAG(
    dag_id="airflow3_jitter_solution",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False
) as dag:

    # 🚨 ใช้ pre_execute มารับฟังก์ชันสุ่มเวลาหน่วงขัดตาทัพก่อนรันงานจริง
    fetch_sap_data = BashOperator(
        task_id="fetch_with_pre_execute_jitter",
        bash_command="curl -f https://api.internal-sap-module.com/ptp/po_data",
        pre_execute=apply_jitter_before_run
    )