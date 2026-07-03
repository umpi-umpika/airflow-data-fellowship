import random
from datetime import datetime
from airflow.sdk import dag, task, Variable 

@dag(
    dag_id="airflow3_dependency_failure_isolation",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "resilience", "circuit-breaker", "dlq"]
)
def dependency_isolation_dag():

    # ==========================================
    # 🧩 STRATEGY 1: CIRCUIT BREAKER PATTERN
    # ==========================================
    @task
    def check_circuit_breaker():
        """
        ตรวจสอบสถานะสะพานไฟก่อนก้าวเท้าไปเรียกระบบภายนอก
        """
        # ดึงสถานะปัจจุบันของสะพานไฟจากคลังระบบ (CLOSED = ปกติ, OPEN = ตัดไฟ)
        cb_status = Variable.get("circuit_breaker_sap_api", default="CLOSED")
        
        if cb_status == "OPEN":
            print("🚨 [Circuit Breaker] STATE IS OPEN! Subsystem is unstable.")
            print("🛑 Instantly failing fast to give the external system room to recover.")
            # ทำการเตะตัดขาตัวเองทันทีโดยไม่ต้องเสี่ยงวิ่งไปยิง API ปลายทางให้เสียเวลา
            raise RuntimeError("CircuitBreakerOpenException: Fast-failing downstream tasks.")
            
        print("🟢 [Circuit Breaker] STATE IS CLOSED. Proceeding to dependency call...")

    # ==========================================
    # 🧩 STRATEGY 2: DEAD LETTER QUEUE (DLQ) PATTERN
    # ==========================================
    @task
    def process_data_with_dlq():
        """
        ทำการประมวลผลข้อมูลแถวต่อแถว หากเจอข้อมูลเน่าเสีย (Unprocessable) 
        จะโยนแยกเข้ากล่อง DLQ เพื่อให้ท่อหลักเดินหน้าต่อไปได้โดยไม่บึ้มตาย
        """
        # จำลองข้อมูลดิบที่ดึงมาได้ (มีไอเทมเน่าเสียปนมา 1 ตัว)
        incoming_payload = [
            {"row_id": 101, "payload": "Valid PO Data", "is_corrupted": False},
            {"row_id": 102, "payload": "MALFORMED_DATA_NULL_PO", "is_corrupted": True}, # <-- ตัวปัญหา
            {"row_id": 103, "payload": "Valid GR Data", "is_corrupted": False},
        ]
        
        healthy_rows = []
        dead_letter_queue = []
        
        success_count = 0
        failure_count = 0
        
        for row in incoming_payload:
            try:
                print(f"⚙️ Parsing row {row['row_id']}...")
                
                # ลอจิกจำลองอาการคอขวด หากข้อมูลบิดเบี้ยวให้สั่งเด้งเข้าบล็อก Exception
                if row["is_corrupted"]:
                    raise ValueError(f"Corrupted record format found at row {row['row_id']}")
                
                # หากข้อมูลสมบูรณ์ดี ให้ยัดลงท่อหลัก
                healthy_rows.append(row)
                success_count += 1
                
            except Exception as e:
                # 🚨 หัวใจหลักของ DLQ: เก็บกวาดข้อมูลเน่าพร้อมแปะป้ายเหตุผลลงกล่องแยกตัว
                print(f"⚠️ [Data Error] Row {row['row_id']} failed parsing! Isolating to DLQ. Reason: {str(e)}")
                dead_letter_queue.append({
                    "data": row,
                    "error_log": str(e),
                    "isolated_at": datetime.now().isoformat()
                })
                failure_count += 1

        # 🚨 จัดการกับข้อมูลเน่าเสีย (DLQ Handling)
        if dead_letter_queue:
            # โลกโปรดักชันจริง: ตรงนี้เราจะยิงคำสั่งเซฟก้อนเนื้อเน่านี้ลงตู้เคลมย่อยแยกต่างหาก เช่น:
            # clickhouse_client.insert('default.dlq_table', dead_letter_queue)
            # หรือส่งเข้าคิวแยก: sqs_client.send_message(QueueUrl=DLQ_URL, MessageBody=str(dead_letter_queue))
            print(f"📥 [DLQ Export] Successfully isolated {len(dead_letter_queue)} poison messages to the Dead Letter Store.")
            
            # บันทึกยอดความล้มเหลวสะสมเข้าระบบ Circuit Breaker หลังบ้าน
            current_failures = int(Variable.get("sap_api_failure_count", default="0"))
            new_failures = current_failures + failure_count
            Variable.set("sap_api_failure_count", str(new_failures))
            
            # 🚨 ถ้าล้มเหลวสะสมเกิน 3 ครั้ง ให้ทำการสับคัตเอาต์เปิดสะพานไฟ (Open Circuit) ทันที!
            if new_failures >= 3:
                Variable.set("circuit_breaker_sap_api", "OPEN")
                print("🚨🚨🚨 [CRITICAL] Failure threshold exceeded! Circuit Breaker has been flipped to OPEN!")

        # ส่งเฉพาะก้อนข้อมูลที่สะอาดบริสุทธิ์ไปให้คนงานท่อนถัดไปลุยต่อ
        print(f"🚀 Main pipeline process continues with {success_count} healthy records.")
        return healthy_rows

    @task
    def load_clean_data(clean_payload: list):
        """
        ทำงานประมวลผลต่อเฉพาะข้อมูลดี ๆ ที่ผ่านการคัดกรองมาแล้ว
        """
        print(f"💾 Ingesting {len(clean_payload)} rows of certified clean data into production layer.")

    # 🔗 ผูกร้อยเรียงโครงสร้างแผนผังการแยกตัวของความล้มเหลว
    cb_check = check_circuit_breaker()
    etl_process = process_data_with_dlq()
    load_data = load_clean_data(etl_process)

    # ตรวจสอบสะพานไฟก่อน -> คัดกรองแยกเนื้อดีเนื้อเน่า (DLQ) -> โหลดเฉพาะเนื้อดีเข้าคลัง
    cb_check >> etl_process >> load_data

dependency_isolation_dag()