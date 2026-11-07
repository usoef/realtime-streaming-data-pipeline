import json
import random
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

TOPIC_NAME = 'transactions'
print(f"🚀 Producer aktif mengirim data ke topik: {TOPIC_NAME}")

recent_events = []

def generate_transaction():
    choice = random.random()
    
    # 1. Simulasi Duplicate Event
    if choice < 0.05 and recent_events:
        return random.choice(recent_events)

    user_id = f"U{random.randint(10000, 99999)}"
    
    # 2. Simulasi Amount Invalid (Negatif / Terlalu Besar)
    if choice < 0.12:
        amount = random.choice([-25000, 15000000])
    else:
        amount = random.randint(50000, 5000000)

    # 3. Simulasi Late Event (> 3 menit) atau Invalid Timestamp
    now = datetime.now(timezone.utc)
    if choice < 0.20:
        event_time = (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z")
    elif choice < 0.25:
        event_time = "INVALID_TIME_FORMAT"
    else:
        event_time = now.isoformat().replace("+00:00", "Z")

    # 4. Simulasi Source Tidak Dikenal
    if choice < 0.32:
        source = "smartwatch"
    else:
        source = random.choice(["mobile", "web", "pos"])

    event = {
        "user_id": user_id if choice > 0.38 else None,
        "amount": amount,
        "timestamp": event_time,
        "source": source
    }

    if len(recent_events) > 10:
        recent_events.pop(0)
    recent_events.append(event)
    return event

if __name__ == "__main__":
    try:
        while True:
            data = generate_transaction()
            producer.send(TOPIC_NAME, value=data)
            producer.flush()
            print(f"Sent: {data}")
            time.sleep(random.uniform(1.0, 2.0))
    except KeyboardInterrupt:
        producer.close()
        print("Producer dihentikan.")