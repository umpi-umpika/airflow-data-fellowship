"""
03_task_dependencies.py — การกำหนด Task Dependencies ใน Airflow
================================================================
📖 Slide Reference: หน้า 32 (Task Dependencies)

ไฟล์นี้สาธิตวิธีกำหนดลำดับการทำงานของ task (dependencies):
1. >> (bitshift right)    — task_a >> task_b  หมายถึง "a ทำก่อน b"
2. << (bitshift left)     — task_b << task_a  หมายถึง "b ขึ้นอยู่กับ a" (เหมือนข้อ 1)
3. set_downstream()       — task_a.set_downstream(task_b) เหมือน >>
4. set_upstream()         — task_b.set_upstream(task_a) เหมือน <<

💡 Fan-out / Fan-in Pattern:
                      ┌─ validate_payment ─┐
   extract_order ─────┤                    ├──── generate_report ── notify_customer
                      └─ validate_stock  ──┘

Fan-out: 1 task แตกออกเป็นหลาย task (extract → [payment, stock])
Fan-in:  หลาย task รวมกลับเป็น 1 task ([payment, stock] → report)

Scenario: E-Commerce Order Processing Pipeline
"""

from airflow.sdk import dag, task
import pendulum


@dag(
    dag_id="poc_dependencies_ecommerce",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1),
    catchup=False,
    tags=["poc", "dependencies"],
    doc_md="""
    ### E-Commerce Order Processing
    สาธิต Task Dependencies: `>>`, `<<`, `set_upstream`, `set_downstream`
    พร้อม Fan-out / Fan-in pattern
    """,
)
def ecommerce_order_processing():
    """
    Pipeline ประมวลผลคำสั่งซื้อ e-commerce
    สาธิตการกำหนด dependency หลายรูปแบบ
    """

    # ====================================================================
    # Task Definitions
    # ====================================================================

    @task
    def extract_order():
        """
        ดึงข้อมูลคำสั่งซื้อจากระบบ
        นี่คือ task แรก — ไม่มี upstream dependency
        """
        order = {
            "order_id": "ORD-2026-001",
            "customer": "สมชาย",
            "items": ["laptop", "mouse"],
            "total": 45000.00,
            "payment_method": "credit_card",
        }
        print(f"📦 Extracted order: {order['order_id']}")
        print(f"   Customer: {order['customer']}")
        print(f"   Total: ฿{order['total']:,.2f}")
        return order

    @task
    def validate_payment(order: dict):
        """
        ตรวจสอบการชำระเงิน
        Fan-out: task นี้รันพร้อมกับ validate_stock
        """
        print(f"💳 Validating payment for order {order['order_id']}...")
        print(f"   Method: {order['payment_method']}")
        print(f"   Amount: ฿{order['total']:,.2f}")
        # simulate validation
        result = {"order_id": order["order_id"], "payment_valid": True}
        print(f"   ✅ Payment validated!")
        return result

    @task
    def validate_stock(order: dict):
        """
        ตรวจสอบสต็อกสินค้า
        Fan-out: task นี้รันพร้อมกับ validate_payment
        """
        print(f"📦 Checking stock for order {order['order_id']}...")
        for item in order["items"]:
            print(f"   Checking '{item}' → In stock ✅")
        result = {"order_id": order["order_id"], "stock_valid": True}
        return result

    @task
    def generate_report(payment_result: dict, stock_result: dict):
        """
        สร้างรายงานสรุปคำสั่งซื้อ
        Fan-in: รอทั้ง validate_payment และ validate_stock เสร็จก่อน
        """
        print(f"📊 Generating report for order {payment_result['order_id']}...")
        report = {
            "order_id": payment_result["order_id"],
            "payment_ok": payment_result["payment_valid"],
            "stock_ok": stock_result["stock_valid"],
            "status": "approved",
        }
        print(f"   Payment: {'✅' if report['payment_ok'] else '❌'}")
        print(f"   Stock:   {'✅' if report['stock_ok'] else '❌'}")
        print(f"   Status:  {report['status'].upper()}")
        return report

    @task
    def notify_customer(report: dict):
        """
        แจ้งเตือนลูกค้า — task สุดท้ายใน pipeline
        """
        print(f"📧 Notifying customer about order {report['order_id']}...")
        print(f"   Status: {report['status']}")
        print(f"   Message: คำสั่งซื้อ {report['order_id']} ได้รับการอนุมัติแล้ว ✅")

    # ====================================================================
    # การรันและเชื่อมโยง Task (TaskFlow API Style)
    # ====================================================================
    order_data = extract_order()

    # Fan-out: รันคู่ขนานกัน
    payment = validate_payment(order_data)
    stock = validate_stock(order_data)

    # Fan-in: รอให้ทั้งคู่เสร็จแล้วส่งผลลัพธ์ไปรวมกัน
    report = generate_report(payment, stock)

    # รัน task แจ้งเตือน
    notification = notify_customer(report)

    # ====================================================================
    # รูปแบบการกำหนด Dependency ทั้งหมดใน Airflow (สำหรับศึกษาเพิ่มเติม)
    # ====================================================================
    #
    # สำหรับ TaskFlow API (@task) แค่ส่ง output ไปเป็น parameter Airflow ก็สร้าง dependency ให้แล้ว
    # แต่หากจำเป็นต้องระบุ หรือกรณีใช้ Classic Operator สามารถระบุได้ดังนี้:
    #
    # 1) >> (Bitshift Right) — นิยมที่สุด ✅
    #    - code_a >> code_b (a ทำก่อน b)
    #
    # 2) << (Bitshift Left) — อ่านย้อนกลับ (ไม่แนะนำ ❌)
    #    - code_b << code_a (b ขึ้นกับ a)
    #
    # 3) set_downstream() — เหมือน >> แต่เป็น method
    #    - code_a.set_downstream(code_b)
    #
    # 4) set_upstream() — เหมือน << แต่เป็น method
    #    - code_b.set_upstream(code_a)
    #
    # --------------------------------------------------------------------
    # สรุปตารางเปรียบเทียบ:
    # | วิธี               | ตัวอย่าง                        | ความหมาย           |
    # |--------------------|---------------------------------|-------------------|
    # | >> (bitshift)      | task_a >> task_b                | a ทำก่อน b         |
    # | << (reverse)       | task_b << task_a                | b ขึ้นอยู่กับ a     |
    # | set_downstream()   | task_a.set_downstream(task_b)   | a → b             |
    # | set_upstream()     | task_b.set_upstream(task_a)      | b ← a             |
    # | function call      | task_b(task_a())                | a → b (TaskFlow)  |
    # --------------------------------------------------------------------



# ⚠️ สำคัญ: ต้อง call function เพื่อ instantiate DAG
ecommerce_order_processing()
