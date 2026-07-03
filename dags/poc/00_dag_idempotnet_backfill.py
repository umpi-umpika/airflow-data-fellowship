import random
from datetime import datetime
from airflow.sdk import dag, task 

import clickhouse_connect


CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "clickhouse"

@dag(
    dag_id="airflow3_clickhouse_idempotent_backfill",
    start_date=datetime(2026, 6, 25),       # เริ่มต้นย้อนหลังเพื่อทำระบบ Backfill 7 วัน
    schedule="@daily",                      # ทำงานวันละ 1 ครั้งรายวัน
    catchup=True,                           # 🚨 เปิดระบบ Catchup เพื่อสั่งวิ่งเก็บตกอดีตแบบคู่ขนานอัตโนมัติ
    default_args={
        "owner": "boss_de_lead",
        "retries": 2
    },
    tags=["production", "clickhouse", "idempotency"]
)
def clickhouse_pipeline_dag():

    # 🧩 Task 1: เตรียมโครงสร้างตารางและจัด Partition บน ClickHouse
    @task
    def init_clickhouse_table():
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
        )
        
        # 🚨 หัวใจหลัก: สั่งแบ่งพาร์ทิชันตารางตามวัน (PARTITION BY trend_date)
        # เพื่อให้เราสามารถสั่งสับบอร์ดหั่นลบข้อมูลเฉพาะวันนั้น ๆ ได้อย่างรวดเร็ว
        client.command("""
            CREATE TABLE IF NOT EXISTS default.trending_videos (
                trend_date Date,
                video_id String,
                title String,
                views UInt64
            ) ENGINE = MergeTree()
            PARTITION BY trend_date
            ORDER BY (trend_date, video_id);
        """)
        print("📦 ClickHouse MergeTree table initialized with dynamic partitioning.")

    # 🧩 Task 2: Fetch & Clean (Mock Data & Filterรายวัน)
    @task
    def extract_and_transform(logical_date=None) -> list:
        # ดึงตัวแปร ds ไดนามิกของ Airflow (เช่น "2026-06-25")
        ds_str = logical_date.strftime("%Y-%m-%d")
        print(f"📥 [Extract] Generating daily mock data for execution date: {ds_str}")
        
        # จำลองข้อมูลดิบรายวัน (Raw Mock Data)
        raw_mock_videos = [
            {"video_id": "vid_01", "title": "Optimizing ClickHouse Partitioning", "views": 720000},
            {"video_id": "vid_02", "title": "Keyboard Thocky Switches Review", "views": 150000},
            {"video_id": "vid_03", "title": "Advanced dbt Lineage Patterns", "views": 840000},
            {"video_id": "vid_04", "title": "Airflow 3.0 Strict Validation", "views": 510000},
        ]
        
        # [Transform] คัดกรองคุณภาพข้อมูล: กรองเอาเฉพาะวิดีโอที่มียอดวิวกินเกณฑ์ 500,000 views ขึ้นไป
        filtered_videos = []
        for video in raw_mock_videos:
            if video["views"] >= 500000:
                filtered_videos.append([
                    datetime.strptime(ds_str, "%Y-%m-%d").date(), # trend_date
                    video["video_id"],
                    video["title"],
                    video["views"]
                ])
                
        print(f"✨ [Transform] Filtered down to {len(filtered_videos)} high-performing videos.")
        return filtered_videos

    # 🧩 Task 3: Idempotent Load (ลบพาร์ทิชันเก่าของวันนั้นทิ้งก่อนเขียนถมใหม่)
    @task
    def idempotent_load_to_clickhouse(data_to_load: list, logical_date=None):
        ds_str = logical_date.strftime("%Y-%m-%d")
        
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
        )
        
        # 🚨 ท่าไม้ตายระดับพระกาฬบน ClickHouse (Drop Partition Strategy)
        # คำสั่งนี้จะทำการเป่าลบข้อมูลของวันนั้น ๆ ออกจากดิสก์ทันทีแบบเด็ดขาด (Zero Residual Data)
        # ไม่ว่าจะกด Rerun กี่รอบ ข้อมูลเก่ายังไงก็โดนเคลียร์เป็นหน้ากลอง สลัดปัญหาเบิ้ลซ้ำทิ้งไปร้อยเปอร์เซ็นต์
        print(f"🧹 [ClickHouse] Dropping old partition for date: '{ds_str}'")
        client.command(f"ALTER TABLE default.trending_videos DROP PARTITION '{ds_str}'")
        
        # ทำการเขียนโหลดข้อมูลก้อนสะอาดถมกลับเข้าไปใหม่ทันทีด้วยท่าถล่มใส่ Bulk Insert
        if data_to_load:
            client.insert(
                'default.trending_videos', 
                data_to_load, 
                column_names=['trend_date', 'video_id', 'title', 'views']
            )
            print(f"🚀 [ClickHouse] Bulk inserted {len(data_to_load)} clean records into partition '{ds_str}'.")
        else:
            print("📝 No data met the criteria. Partition remains clean and empty.")

    # 🧩 [Bonus Task]: คำสั่งพิมพ์สรุปอันดับวิดีโอสุดฮิตประจำวันสูงสุด Top 3 ลงระบบล็อก
    @task
    def log_top_videos(logical_date=None):
        ds_str = logical_date.strftime("%Y-%m-%d")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, 
            username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD
        )
        
        result = client.query(f"""
            SELECT title, views FROM default.trending_videos 
            WHERE trend_date = '{ds_str}' 
            ORDER BY views DESC LIMIT 3
        """)
        
        print(f"📊 === CLICKHOUSE TOP 3 VIDEOS FOR {ds_str} ===")
        for idx, row in enumerate(result.result_rows, 1):
            print(f"🏆 Rank #{idx}: {row[0]} with {row[1]:,} views")

    # 🔗 ผูกร้อยเรียงโครงสร้างแผนผังข้อมูล (Data Dependency Map)
    init = init_clickhouse_table()
    data = extract_and_transform()
    load = idempotent_load_to_clickhouse(data)
    analytics = log_top_videos()

    init >> data >> load >> analytics

# ลงทะเบียนระบบ DAG ขึ้นหน้าจอ UI
clickhouse_pipeline_dag()