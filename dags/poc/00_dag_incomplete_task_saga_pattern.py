import random
from datetime import datetime
from airflow.sdk import dag, task 
from airflow.task.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context

@dag(
    dag_id="airflow3_saga_pattern",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "resilience", "airflow3"]
)
def real_saga_production_dag():

    @task
    def deduct_payment():
        """Step 1: ตัดเงินลูกค้าผ่าน Payment Gateway API หรือยัดลงตู้ Database"""
        tx_id = "tx_booking_" + datetime.now().strftime("%Y%m%d%H%M%S")
        print(f"💳 [Saga Step 1] API Call: Charging user $100. Generated ID: {tx_id}")
        
        # 🚨 โลกจริง: เราจะแอบฝากหมายเลขธุรกรรมนี้ไว้ใน XCom เพื่อให้ Task อื่นๆ ในอนาคตมาดึงไปใช้ได้
        return tx_id

    @task
    def reserve_tickets(transaction_id: str):
        """Step 2: จองที่นั่งในระบบคอนเสิร์ต"""
        print(f"🎟️ [Saga Step 2] Interacting with Ticket Service for: {transaction_id}")
        
        # จำลองสถานการณ์ระบบล่ม (เช่น ที่นั่งชนกันตอนวินาทีสุดท้าย)
        if random.random() < 0.7:
            print("🚨 [Saga Failure] Seating inventory conflict detected!")
            raise RuntimeError("Seating Conflict")
            
        print("🎯 Concert seat locked successfully!")

    # 🚨 นี่คือกล่องชดเชยความเสียหาย (Compensating Transaction) ของจริง
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def compensate_refund_payment():
        """Saga Compensation Action: วิ่งไปดึงค่าตัวแปรจากงานก่อนหน้า แล้วยิง API ทำเรื่องคืนเงินจริง"""
        
        # 1. ดึงพลังบริบท (Live Context) รอบรันนั้นขึ้นมา
        context = get_current_context()
        ti = context["task_instance"]
        
        # 2. ทำการส่องมองกล่องดำดึงค่า XCom จากขั้นตอนแรก (`deduct_payment`) 
        # เพื่อเอาหมายเลข Transaction ID ตัวปัญหาที่เคยก่อไว้ขึ้นมาใช้งาน!
        target_tx_id = ti.xcom_pull(task_ids="deduct_payment")
        
        print(f"⏪ [Saga Compensation] Activating Refund Sequence for target ID: {target_tx_id}")
        
        if target_tx_id:
            # 🚨 โลกจริง: ตรงนี้เราจะเขียนโค้ดเชื่อมต่อระบบปลายทางจริง เช่น:
            # import requests
            # requests.post(f"https://api.payment-gateway.com/refund", json={"tx_id": target_tx_id})
            
            # หรือการสั่งรัน SQL ไปเคลียร์ข้อมูลในฐานข้อมูล:
            # db_hook.run(f"DELETE FROM bookings WHERE transaction_id = '{target_tx_id}'")
            
            print(f"💰 SUCCESS: REST API Request sent to Payment Gateway. Transaction {target_tx_id} refunded fully!")
        else:
            print("⚠️ WARNING: No prior transaction ID found in XCom. Skipping refund invocation.")

    # 🔗 ผูกร้อยเรียงสายงานระบบจัดซื้อแบบกระจายศูนย์
    tx_id = deduct_payment()
    booking = reserve_tickets(tx_id)
    
    # สั่งให้กล่องกู้ชีพแอบเปิดอ่านใบเสร็จจากก้อน XCom เพื่อทำการถอนรากถอนโคนคำสั่งซื้อที่เน่าเสีย
    booking >> compensate_refund_payment()

real_saga_production_dag()