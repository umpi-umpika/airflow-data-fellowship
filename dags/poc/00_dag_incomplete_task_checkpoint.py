from datetime import datetime
from airflow.sdk import Variable, dag, task 

@dag(
    dag_id="airflow3_checkpoint",
    start_date=datetime(2026, 7, 1),
    schedule="@daily",
    catchup=False
)
def checkpoint_fix_dag():

    @task
    def process_large_files_with_checkpoint(ti=None):
        current_index = Variable.get("file_processing_checkpoint", default=0, deserialize_json=True)
        
        files = ["file_A.csv", "file_B.csv", "file_C.csv"]
        print(f"🔄 Starting file processing. Last completed checkpoint index: {current_index}")
        
        for i in range(current_index, len(files)):
            print(f"⚙️ Processing {files[i]}...")
            
            # 🚨 คีย์เวิร์ดแก้ล็อกตาย: จำลองเน็ตหลุดเฉพาะการรันรอบแรก (Try 1) เท่านั้น
            # พอเรากด Rerun/Clear (กลายเป็น Try 2) ลอจิกนี้จะไม่ทำงาน ทำให้รันผ่านฉลุยครับ!
            if files[i] == "file_B.csv" and ti.try_number == 1:
                raise RuntimeError("💥 Simulating temporary network timeout on Try 1!")
                
            next_checkpoint = i + 1
            Variable.set("file_processing_checkpoint", next_checkpoint, serialize_json=True)
            print(f"✅ Checkpoint advanced to integer: {next_checkpoint}")

    process_large_files_with_checkpoint()

checkpoint_fix_dag()