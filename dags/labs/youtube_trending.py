from airflow.decorators import dag, task
from datetime import datetime, timedelta
import random
from airflow.providers.clickhousedb.hooks.clickhouse import ClickHouseHook

default_args = {
    "owner": "data-team",
    # ✅ TODO 1: เพิ่ม retries=2 และ retry_delay=timedelta(minutes=2)
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def get_client():
    ch_hook = ClickHouseHook(clickhouse_conn_id="clickhouse_conn")
    client = ch_hook.get_client()
    client.query("CREATE DATABASE IF NOT EXISTS labs")
    client.query("""
        CREATE TABLE IF NOT EXISTS labs.trending (
            video_id   String,
            title      String,
            channel    String,
            views      Int64,
            likes      Int64,
            trend_date String,
            created_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY video_id
    """)
    return client


def mock_trending_api(date: str) -> list:
    """Mock YouTube Data API — seed เดิม = ผลลัพธ์เดิม (idempotent โดยธรรมชาติ)"""
    random.seed(date)
    channels = ["WorkpointOfficial", "OneHD", "GMM Grammy", "Gmmtv", "Modernine TV"]
    titles = [
        "เพลงใหม่มาแรง", "ซีรีย์ไทยฮิต", "ข่าวเช้า", "รายการวาไรตี้",
        "คอนเสิร์ตสด", "ฮาวทู", "รีวิวอาหาร", "วล็อกท่องเที่ยว",
        "เกมมิ่ง", "แข่งกีฬา"
    ]
    videos = []
    for i in range(10):
        videos.append({
            "video_id":  f"VDO-{date}-{i+1:03d}",
            "title":     f"{random.choice(titles)} EP.{random.randint(1,100)}",
            "channel":   random.choice(channels),
            "views":     random.randint(100_000, 5_000_000),
            "likes":     random.randint(1_000, 200_000),
            "trend_date": date,
        })
    return videos


@dag(
    dag_id="lab2_youtube_trending",
    default_args=default_args,
    # ✅ TODO 2: กำหนด schedule="@daily"
    schedule="@daily",
    # ✅ TODO 3: กำหนด start_date ย้อนหลัง 7 วัน
    start_date=datetime.now() - timedelta(days=7),
    # ✅ TODO 4: เปิด catchup=True เพื่อให้ backfill ได้
    catchup=True,
    tags=["lab2", "backfill", "trending"],
)
def lab2_youtube_trending():

    @task
    def fetch(ds=None):
        """
        ✅ TODO 5: เรียก mock_trending_api(ds) แล้ว return ผลลัพธ์
        """
        print(f"[fetch] กำลังดึง trending วันที่ {ds}")
        videos = mock_trending_api(ds)
        print(f"[fetch] ดึงข้อมูลมาได้ทั้งหมด {len(videos)} วิดีโอ")
        return videos

    @task
    def clean(videos: list) -> list:
        """
        ✅ TODO 6: กรองวิดีโอที่มี views < 500,000 ออก
        """
        cleaned_videos = [v for v in videos if v["views"] >= 500000]
        print(f"[clean] กรองวิดีโอที่มี views >= 500,000 เหลือ {len(cleaned_videos)} จาก {len(videos)}")
        return cleaned_videos

    @task
    def load(videos: list, ds=None):
        """
        ✅ TODO 7: บันทึกลง DB แบบ idempotent (DELETE เก่าก่อนค่อย INSERT ใหม่)
        """
        client = get_client()
        # ลบข้อมูลเก่าของวันนั้นออกก่อน เพื่อให้รันซ้ำกี่ครั้งก็ได้ผลลัพธ์เท่าเดิม ไม่ซ้ำซ้อน
        client.query(f"DELETE FROM labs.trending WHERE trend_date = '{ds}'")
        
        # เตรียมข้อมูลเพื่อ Insert
        rows = [
            (v["video_id"], v["title"], v["channel"], v["views"], v["likes"], v["trend_date"])
            for v in videos
        ]
        
        client.insert(
            "labs.trending",
            rows,
            column_names=["video_id", "title", "channel", "views", "likes", "trend_date"]
        )
        print(f"[load] บันทึกข้อมูล {len(rows)} แถว ลงในกลุ่มวันที่ {ds} เรียบร้อยแล้ว")

    @task
    def bonus_top_3_videos(videos: list, ds=None):
        """
        ⭐️ (Bonus) พิมพ์ Top 3 วิดีโอที่มียอด views สูงสุดของวันนั้น
        """
        sorted_videos = sorted(videos, key=lambda x: x["views"], reverse=True)
        top_3 = sorted_videos[:3]
        
        print(f"\n{'='*50}")
        print(f"⭐️ TOP 3 TRENDING VIDEOS ON {ds} ⭐️")
        print(f"{'='*50}")
        for idx, v in enumerate(top_3, 1):
            print(f"{idx}. {v['title']} | ช่อง: {v['channel']} | Views: {v['views']:,}")
        print(f"{'='*50}\n")

    # ✅ TODO 8: เชื่อมโยง Task Dependencies (fetch → clean → load และแตกสายไปทำ bonus)
    raw_data = fetch()
    cleaned_data = clean(raw_data)
    
    # รันโหลดลง DB และรันแสดงผลท็อป 3
    load_to_db = load(cleaned_data)
    show_top_3 = bonus_top_3_videos(cleaned_data)

    raw_data >> cleaned_data >> [load_to_db, show_top_3]


# เรียกใช้งาน DAG
lab2_youtube_trending()