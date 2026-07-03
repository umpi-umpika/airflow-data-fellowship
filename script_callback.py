import random
import time

def calculate_backoff_with_jitter(attempt, base_delay=1, max_delay=32):
    """
    คำนวณเวลารอโดยใช้สูตร Exponential Backoff + Full Jitter
    """
    # 1. คำนวณแบบ Exponential Backoff นิ่ง ๆ (เช่น 1s -> 2s -> 4s -> 8s)
    exponential_backoff = min(max_delay, base_delay * (2 ** attempt))
    
    # 2. ใส่ Jitter (โรยเกลือสุ่มค่าตั้งแต่ 0 จนถึงเพดานของ Exponential รอบนั้น)
    actual_delay = random.uniform(0, exponential_backoff)
    
    return actual_delay

# จำลองสถานการณ์: มี Workers 5 ตัวพังพร้อมกันในรอบที่ 3 (Attempt 3)
print("=== Exponential Backoff VS Full Jitter (Attempt 3) ===")
raw_backoff = min(32, 1 * (2 ** 3))
print(f"เดฟสายทฤษฎี (Pure Exponential Backoff) ทุกตัวต้องรอเท่ากันเป๊ะ: {raw_backoff} วินาที\n")

print("เดฟสายโปรดักชัน (Workers สุ่มตื่นด้วย Jitter):")
for worker_id in range(1, 6):
    delay = calculate_backoff_with_jitter(attempt=3)
    print(f" 🤖 Worker #{worker_id} สุ่มได้เวลารอจริง -> {delay:.2f} วินาที")