# 🧠 Quiz: Data Pipeline & Apache Airflow

> สร้างจากสไลด์ **"Add a heading.pdf"** (44 หน้า) — ครอบคลุม Module 1: Data Pipeline Theory & Module 2: Airflow & Architecture

---

## Section 1 — Data Pipeline Fundamentals (Pages 2-16)

### Q1. Batch vs. Streaming
**Batch Processing Pipeline** กับ **Stream Processing Pipeline** ต่างกันอย่างไรในด้าน **Resource Profile**?

- A) Batch ใช้ resource ต่ำตลอดเวลา, Stream ใช้ resource สูงเป็นช่วง ๆ
- B) Batch ทำงานไม่บ่อยแต่ต้องใช้ computing power สูงในช่วงสั้น, Stream ทำงานต่อเนื่องแต่ใช้ computing spike ต่ำกว่า
- C) ทั้งสองแบบใช้ resource เท่ากัน ขึ้นอยู่กับปริมาณข้อมูล
- D) Stream ต้องการ computing power สูงกว่าเสมอเนื่องจากทำงาน real-time

> **✅ คำตอบ: B**
> Batch = ทำงานไม่บ่อย (off-peak hours) แต่ต้องใช้ computing power สูง, Stream = ทำงานต่อเนื่อง spike ต่ำกว่าแต่พึ่งพา network latency ต่ำ

---

### Q2. Idempotency — Core Concept
ข้อใดอธิบาย **Idempotency** ได้ถูกต้องที่สุด?

- A) Operation ที่ทำงานได้เร็วขึ้นทุกครั้งที่ execute ซ้ำ
- B) Operation ที่ execute กี่ครั้งก็ได้ผลลัพธ์เหมือนกับ execute ครั้งเดียว
- C) Operation ที่ต้อง execute เพียงครั้งเดียวเท่านั้น
- D) Operation ที่สามารถ rollback ได้ทุกสถานการณ์

> **✅ คำตอบ: B**
> Idempotency = execute หลายครั้งก็ได้ผลลัพธ์เหมือนกับ execute ครั้งเดียว ไม่สร้าง duplicate หรือ side effect

---

### Q3. Idempotent Design — Scenario
ทีม Data Engineer เขียน daily SQL ดังนี้:
```sql
INSERT INTO daily_sales SELECT * FROM raw_orders WHERE date = '2026-06-15';
```
ถ้า script นี้ถูกรันซ้ำ 3 ครั้ง จะเกิดอะไรขึ้น และควรแก้ไขอย่างไร?

- A) ข้อมูลจะถูก overwrite — ไม่ต้องแก้อะไร
- B) ข้อมูลจะซ้ำ 3 เท่า — ควรเปลี่ยนเป็น `REPLACE INTO` หรือลบ partition ก่อน insert
- C) ข้อมูลจะซ้ำ 3 เท่า — ควรเพิ่ม retry logic
- D) Database จะ reject row ซ้ำ automatically

> **✅ คำตอบ: B**
> INSERT ธรรมดา = non-idempotent → ข้อมูลซ้ำ 3 เท่า แก้ด้วย UPSERT / REPLACE INTO / Write-Truncate pattern

---

### Q4. 3 Pillars of Idempotent Design
ข้อใด **ไม่ใช่** หนึ่งใน 3 Pillars of Idempotent Design?

- A) Unique Idempotency Keys
- B) Upsert (Insert or Update)
- C) Exponential Backoff with Jitter
- D) Deterministic Overwrites

> **✅ คำตอบ: C**
> Exponential Backoff with Jitter เป็น Recovery Strategy สำหรับ Transient Errors ไม่ใช่ Pillar ของ Idempotent Design

---

### Q5. Failure Recovery — Matching
จับคู่ **Failure Mode** กับ **Recovery Strategy** ให้ถูกต้อง:

| Failure Mode | Recovery Strategy |
|---|---|
| 1. Transient Errors | a. Idempotent Backfilling |
| 2. Incomplete Tasks | b. Circuit Breaker + Dead Letter Queue |
| 3. Historical Data Corruption | c. Exponential Backoff with Jitter |
| 4. Broken Upstream Systems | d. Checkpointing / Saga Pattern |

- A) 1-c, 2-d, 3-a, 4-b
- B) 1-a, 2-c, 3-d, 4-b
- C) 1-c, 2-a, 3-d, 4-b
- D) 1-d, 2-c, 3-b, 4-a

> **✅ คำตอบ: A**
> Transient → Backoff+Jitter, Incomplete → Checkpoint/Saga, Corruption → Idempotent Backfill, Broken Upstream → Circuit Breaker+DLQ

---

### Q6. Data Pipeline Architecture
Data Pipeline Architecture ประกอบด้วย 5 Layer โดย Layer ใดทำหน้าที่ **"ให้บริการข้อมูลสุดท้ายแก่ BI tools หรือ ML models"**?

- A) Ingestion Layer
- B) Processing Layer
- C) Storage Layer
- D) Consumption Layer

> **✅ คำตอบ: D**
> Consumption Layer = ส่งมอบข้อมูลพร้อมใช้ให้กับ BI tools หรือ ML models

---

## Section 2 — Airflow Fundamentals (Pages 17-30)

### Q7. Airflow's Role
อุปมาที่ถูกต้องที่สุดสำหรับ Apache Airflow คือข้อใด?

- A) Airflow คือ "แรงงาน" (Laborer) ที่ทำการประมวลผลข้อมูลจริง ๆ
- B) Airflow คือ "ผู้จัดการโรงงาน" (Factory Manager) ที่สั่งการว่า task ใดต้องทำก่อน-หลัง
- C) Airflow คือ "ฐานข้อมูล" (Database) ที่เก็บข้อมูลทั้งหมด
- D) Airflow คือ "เครื่องมือ ETL" ที่ทำ Extract, Transform, Load ด้วยตัวเอง

> **✅ คำตอบ: B**
> Airflow = Workflow Orchestrator (Factory Manager) ไม่ใช่ตัวประมวลผลข้อมูลเอง

---

### Q8. Airflow Components
ข้อใดจัดกลุ่ม **Required Components** ของ Airflow ได้ถูกต้อง?

- A) Scheduler, Webserver, Worker, Triggerer
- B) Scheduler, DAG Processor, Webserver, DAG Files Folder, Metadata Database
- C) Scheduler, DAG Processor, Worker, Plugins
- D) Webserver, Metadata Database, Triggerer, Plugins

> **✅ คำตอบ: B**
> Required = Scheduler, DAG Processor, Webserver, DAG Files Folder, Metadata Database / ส่วน Worker, Triggerer, Plugins = Optional

---

### Q9. DAG Anatomy
DAG ใน Airflow ประกอบด้วยส่วนย่อยหลายส่วน ข้อใด **ไม่ใช่** ส่วนประกอบหลักของ DAG?

- A) Schedule (The Timing)
- B) Tasks (The Workforce)
- C) Data Lake (The Storage)
- D) Callbacks (The Event Handlers)

> **✅ คำตอบ: C**
> Data Lake เป็น storage ภายนอก ไม่ใช่ส่วนประกอบของ DAG ส่วนประกอบหลัก = Schedule, Tasks, Task Dependencies, Callbacks, Additional Parameters

---

### Q10. Declaring a DAG
สไลด์แนะนำ **วิธีการหลัก (primary method)** ในการประกาศ DAG ในคอร์สนี้คือวิธีใด?

- A) ใช้ `with` statement (context manager)
- B) ใช้ standard constructor แล้ว pass DAG object เข้า operator
- C) ใช้ `@dag` decorator เพื่อแปลง function เป็น DAG generator
- D) เขียน YAML configuration file

> **✅ คำตอบ: C**
> `@dag` decorator เป็น primary method ที่ใช้ในคอร์สนี้

---

### Q11. Task Dependencies
วิธีการกำหนด Task Dependencies ที่ **แนะนำ (most highly recommended)** ใน Airflow คือข้อใด?

- A) `set_upstream()` และ `set_downstream()`
- B) Bitshift Operators (`>>` และ `<<`)
- C) ใช้ `depends_on_past=True`
- D) เขียน dependency ใน YAML file

> **✅ คำตอบ: B**
> Bitshift operators (`>>` และ `<<`) เป็นวิธีที่แนะนำมากที่สุด

---

### Q12. Operators & @task
ข้อใดเป็น **caveat (ข้อจำกัด)** สำคัญของ `@task` decorator?

- A) ใช้กับ BashOperator ไม่ได้
- B) ไม่สามารถ render Jinja templates เมื่อ pass ผ่าน arguments ได้โดยตรง
- C) ไม่รองรับการ return ค่าระหว่าง tasks
- D) ใช้ได้เฉพาะ Python 3.10 ขึ้นไป

> **✅ คำตอบ: B**
> `@task` decorator ไม่ support rendering Jinja templates เมื่อ pass ผ่าน arguments แบบเดียวกับ traditional Operators

---

## Section 3 — Execution, Lifecycle & Scheduling (Pages 31-36)

### Q13. Cron Expression
Cron expression `0 0 * * 1-5` หมายความว่าอย่างไร?

- A) ทำงานทุก 5 นาที ตลอด 24 ชั่วโมง
- B) ทำงานเที่ยงคืน (00:00) เฉพาะวันจันทร์ถึงวันศุกร์
- C) ทำงานทุกชั่วโมง เฉพาะเดือน 1-5
- D) ทำงานทุกวันที่ 1-5 ของทุกเดือน

> **✅ คำตอบ: B**
> `0 0 * * 1-5` = Minute 0, Hour 0, ทุกวันของเดือน, ทุกเดือน, วันจันทร์-วันศุกร์

---

### Q14. Cron vs. Timedelta
ข้อใดอธิบายความแตกต่างระหว่าง **Cron** และ **Timedelta** ได้ถูกต้อง?

- A) Cron = กำหนดความถี่ (เช่น ทุก 24 ชม.), Timedelta = กำหนดเวลาตายตัว (เช่น ตี 2 ทุกวัน)
- B) Cron = ปักหมุดเวลาบนนาฬิกา (Wall-Clock), Timedelta = พลิกนาฬิกาทราย (Frequency/Duration)
- C) ทั้งคู่ทำงานเหมือนกัน ต่างแค่ syntax
- D) Timedelta รองรับ Timezone แต่ Cron ไม่รองรับ

> **✅ คำตอบ: B**
> Cron = "ปักหมุดเวลาบนนาฬิกา" (Wall-Clock Time), Timedelta = "พลิกนาฬิกาทราย" (ดู elapsed time)

---

### Q15. Timetable
**Timetable** ถูกสร้างขึ้นเพื่อแก้ปัญหาอะไรของ Cron?

- A) Cron ไม่รองรับการรัน task แบบ parallel
- B) Cron ไม่สามารถแสดงเงื่อนไขอย่าง "วันทำการสุดท้ายของเดือน" หรือ "ข้ามวันหยุด" ได้
- C) Cron ทำงานช้ากว่า Timetable
- D) Cron ไม่สามารถรันทุกชั่วโมงได้

> **✅ คำตอบ: B**
> Timetable แก้ปัญหาที่ Cron expression เขียนเงื่อนไขซับซ้อนไม่ได้ และเพิ่ม timezone awareness

---

### Q16. Timezone Trap 🇹🇭
ถ้าเขียน `schedule="0 12 * * *"` โดยไม่ใช้ Timetable ใน Airflow DAG ที่ต้องการรันเที่ยง **เวลาประเทศไทย (UTC+7)** จะเกิดอะไรขึ้น?

- A) DAG จะรันเที่ยงเวลาไทยตามปกติ
- B) DAG จะรันตอน 5:00 AM เวลาไทย
- C) DAG จะรันตอน 7:00 PM (19:00) เวลาไทย
- D) DAG จะ error เพราะไม่ได้กำหนด timezone

> **✅ คำตอบ: C**
> Airflow ตีความ cron string เป็น UTC เสมอ → 12:00 UTC = 19:00 Thailand time

---

### Q17. Data Interval & Logical Date
สำหรับ DAG ที่ตั้ง schedule เป็น `@daily` และ `start_date = 2026-06-17` — DAG Run แรกจะถูก trigger **เมื่อไหร่?**

- A) ทันทีที่ 2026-06-17 00:00:00
- B) หลังจากผ่านไปหนึ่ง interval คือ 2026-06-18 00:00:00
- C) ขึ้นอยู่กับ Scheduler ว่าจะ detect เมื่อไหร่
- D) ไม่ trigger เพราะต้อง manual trigger เท่านั้น

> **✅ คำตอบ: B**
> Airflow ออกแบบให้ DAG Run trigger **หลังจาก** Data Interval จบแล้ว → start_date 17 มิ.ย. + 1 full daily interval = trigger 18 มิ.ย.

---

## Section 4 — Connections, Hooks, Variables & Templating (Pages 37-44)

### Q18. Connection vs. Hook
ข้อใดอธิบายความสัมพันธ์ระหว่าง **Connection** กับ **Hook** ได้ถูกต้อง?

- A) Connection = Python interface สำหรับ interact กับระบบภายนอก, Hook = credentials ที่เก็บใน metadata DB
- B) Connection = credentials (เช่น password, API key) ที่อ้างอิงด้วย conn_id, Hook = Python interface ที่ abstract low-level API calls
- C) ทั้งสองเป็นสิ่งเดียวกัน แค่ชื่อต่างกัน
- D) Connection อยู่ใน DAG file, Hook อยู่ใน metadata database

> **✅ คำตอบ: B**
> Connection = credentials เก็บใน Airflow (UI/Env Var/Secret Manager) อ้างอิงด้วย conn_id, Hook = high-level Python interface ที่ abstract boilerplate

---

### Q19. Airflow Variables — Types
Airflow Variable มีกี่ประเภท (types) และคืออะไรบ้าง?

- A) 3 ประเภท: string, integer, boolean
- B) 2 ประเภท: regular values และ JSON serialized values
- C) 1 ประเภท: key-value pair เท่านั้น
- D) 4 ประเภท: string, number, json, secret

> **✅ คำตอบ: B**
> 2 types = regular values (plain string) และ JSON serialized values

---

### Q20. Jinja Templating — Best Practice
ถ้าต้อง filter ข้อมูลด้วยวันที่ใน SQL template ของ Airflow ข้อใดเป็นวิธีที่ **ถูกต้อง**?

```sql
-- Option A
WHERE date = '{{ logical_date }}'

-- Option B
WHERE date = '{{ ds }}'
```

- A) Option A ถูกต้อง เพราะ logical_date แม่นยำกว่า
- B) Option B ถูกต้อง เพราะ `{{ ds }}` ให้ string format YYYY-MM-DD ที่เหมาะกับ SQL, ส่วน `{{ logical_date }}` มี timezone ติดมาทำให้กรองข้อมูลผิด
- C) ทั้งสองถูกต้องเท่าเทียมกัน
- D) ทั้งสองผิด ต้องใช้ `{{ data_interval_end }}` เท่านั้น

> **✅ คำตอบ: B**
> `{{ logical_date }}` มี timezone → กรองข้อมูลผิด | `{{ ds }}` = shorthand ของ `{{ data_interval_start | ds }}` → output "YYYY-MM-DD" (string)

---

### Q21. Context Variables — Practical
เมื่อใช้ `@task` decorated function ต้องการเข้าถึง `task_instance` object ต้องทำอย่างไร?

- A) ใช้ Jinja template `{{ task_instance }}` ใน argument
- B) Access ผ่าน Task Context โดยตรงภายใน function
- C) Import จาก `airflow.models`
- D) ไม่สามารถเข้าถึง task_instance จาก `@task` ได้

> **✅ คำตอบ: B**
> `@task` decorated tasks สามารถ access context variables ได้โดยตรง (ไม่ผ่าน Jinja) แต่ต้อง access จาก Task Context

---

## Section 5 — Bonus: Short Answer / Open-Ended

### Q22. (Open-ended)
อธิบายว่าทำไม **Exponential Backoff with Jitter** จึงดีกว่า retry แบบ fixed interval ธรรมดา โดยเฉพาะในสถานการณ์ที่มี worker หลายตัวเกิด failure พร้อมกัน

> **แนวคำตอบ:** Exponential Backoff เพิ่มระยะเวลารอแบบทวีคูณ (1s, 2s, 4s, 8s...) ลดโหลดที่ server ปลายทาง ส่วน Jitter เพิ่ม random noise ป้องกัน "thundering herd" — หลาย worker retry พร้อมกันจน server ล่ม

---

### Q23. (Open-ended)
ในสถานการณ์ที่ external API ที่ pipeline พึ่งพาอยู่ down ต่อเนื่อง อธิบายว่า **Circuit Breaker Pattern** และ **Dead Letter Queue (DLQ)** ทำงานร่วมกันอย่างไรเพื่อป้องกันไม่ให้ pipeline ทั้งระบบล่ม

> **แนวคำตอบ:** Circuit Breaker = ถ้า dependency fail ซ้ำ ๆ จะ "เปิดวงจร" หยุด call ไปยังระบบที่พังทันที ให้เวลา recover / DLQ = แยกข้อมูลที่ process ไม่ได้ไปเก็บใน queue พิเศษเพื่อตรวจสอบภายหลัง ทำให้ส่วนที่เหลือของ pipeline ทำงานต่อได้

---

### Q24. (Scenario-based)
คุณมี DAG ที่ schedule `@daily` โดย `start_date = 2026-06-15` และวันนี้คือ **2026-06-18** — Airflow จะสร้างกี่ DAG Runs (สมมติ `catchup=True`)? และ logical_date ของแต่ละ run คืออะไร?

> **แนวคำตอบ:** 3 DAG Runs:
> 1. logical_date = 2026-06-15 (trigger หลัง interval 15→16 จบ)
> 2. logical_date = 2026-06-16 (trigger หลัง interval 16→17 จบ)
> 3. logical_date = 2026-06-17 (trigger หลัง interval 17→18 จบ)

---

## 📊 สรุปภาพรวม Quiz

| Section | หัวข้อ | จำนวนข้อ | ระดับ |
|---------|--------|----------|-------|
| 1 | Data Pipeline Fundamentals | 6 ข้อ (Q1-Q6) | 🟢 Basic - 🟡 Intermediate |
| 2 | Airflow Fundamentals | 6 ข้อ (Q7-Q12) | 🟢 Basic - 🟡 Intermediate |
| 3 | Execution & Scheduling | 5 ข้อ (Q13-Q17) | 🟡 Intermediate - 🔴 Advanced |
| 4 | Connections, Variables & Templating | 4 ข้อ (Q18-Q21) | 🟡 Intermediate |
| 5 | Open-ended / Scenario | 3 ข้อ (Q22-Q24) | 🔴 Advanced |

**รวม: 24 ข้อ** — ผสมระหว่าง Multiple Choice, Matching, และ Open-ended
