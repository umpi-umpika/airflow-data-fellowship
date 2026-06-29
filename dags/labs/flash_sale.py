"""
Lab 1 — Flash Sale Pipeline (Batch Script Version)
==================================================
This is a standalone batch script to run the flash sale pipeline.
"""

import argparse
import random
from datetime import datetime, timedelta
import clickhouse_connect


def get_client(host="localhost"):
    try:
        client = clickhouse_connect.get_client(host=host, port=8123, username='default', password='clickhouse')
    except Exception as e:
        if host == "localhost":
            print(f"Failed to connect to localhost, trying 'clickhouse' host: {e}")
            client = clickhouse_connect.get_client(host='clickhouse', port=8123, username='default', password='clickhouse')
        else:
            raise e
    
    client.query("CREATE DATABASE IF NOT EXISTS labs")
    client.query("""
        CREATE TABLE IF NOT EXISTS labs.orders (
            order_id    String,
            customer_id String,
            product     String,
            amount      Float64,
            sale_date   String,
            created_at  DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY order_id
    """)
    return client


def generate_mock_orders(sale_date: str) -> list:
    """สร้าง mock orders สำหรับวันนั้น (seed เดิม = ข้อมูลเดิมทุกครั้ง)"""
    random.seed(sale_date)
    products = ["iPhone 16", "AirPods Pro", "MacBook Air", "iPad Mini", "Apple Watch"]
    orders = []
    for i in range(10):
        orders.append({
            "order_id":    f"ORD-{sale_date}-{i+1:03d}",
            "customer_id": f"CUST-{random.randint(1000, 9999)}",
            "product":     random.choice(products),
            "amount":      round(random.uniform(1000, 80000), 2),
            "sale_date":   sale_date,
        })
    return orders


def extract(ds: str):
    """ดึง orders จาก mock API"""
    print(f"[extract] กำลังดึง orders วันที่ {ds}")
    orders = generate_mock_orders(ds)
    print(f"[extract] ได้ {len(orders)} orders")
    return orders


def insert_orders(client, orders: list, ds: str):

    client.query(f"DELETE FROM labs.orders WHERE sale_date = '{ds}'")

    rows = [
        (order["order_id"], order["customer_id"], order["product"], order["amount"], order["sale_date"])
        for order in orders
    ]
    
    client.insert(
        "labs.orders",
        rows,
        column_names=["order_id", "customer_id", "product", "amount", "sale_date"]
    )

    # ตรวจสอบ
    result = client.query("SELECT count() FROM labs.orders WHERE sale_date = {ds:String}", parameters={"ds": ds})
    total = result.result_rows[0][0]
    print(f"[insert_orders] rows ใน DB วัน {ds}: {total}")
    print(f"[insert_orders] คาดหวัง 10, จริง {total} {'✅' if total == 10 else '❌ มีซ้ำ!'}")


def summarize(client, ds: str):
    """สรุปยอดขายรายวัน"""
    result = client.query(
        "SELECT count() as cnt, sum(amount) as rev FROM labs.orders WHERE sale_date = {ds:String}",
        parameters={"ds": ds}
    )
    row = result.result_rows[0]
    count = row[0]
    revenue = row[1] if row[1] is not None else 0.0

    print(f"\n{'='*40}")
    print(f"Flash Sale Summary — {ds}")
    print(f"  Orders  : {count}")
    print(f"  Revenue : {revenue:,.2f} THB")
    print(f"{'='*40}\n")
    if count != 10:
        raise ValueError(f"❌ พบ {count} orders แต่คาดหวัง 10 — มีข้อมูลซ้ำ!")


def main():
    parser = argparse.ArgumentParser(description="Flash Sale Batch Pipeline")
    parser.add_argument(
        "--date", 
        type=str, 
        default=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
        help="Date to process in YYYY-MM-DD format (default: yesterday)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="ClickHouse host (default: localhost)"
    )
    args = parser.parse_args()

    print(f"Starting pipeline run for date: {args.date} using ClickHouse host: {args.host}")
    
    client = get_client(args.host)
    
    orders = extract(args.date)
    insert_orders(client, orders, args.date)
    summarize(client, args.date)
    print("Pipeline run completed successfully!")


if __name__ == "__main__":
    main()