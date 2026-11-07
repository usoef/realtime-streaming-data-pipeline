"""Kafka producer that emits synthetic transaction events.

A configurable share of events is deliberately faulty (duplicates, invalid
amounts, late or malformed timestamps, unknown sources, missing user IDs) to
exercise the validation, DLQ, and watermark logic downstream.
"""

import json
import random
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

KAFKA_SERVERS = ["localhost:9092"]
TOPIC_NAME = "transactions"
VALID_SOURCES = ("mobile", "web", "pos")

# Probability of each simulated fault, applied independently per event
P_DUPLICATE = 0.05
P_INVALID_AMOUNT = 0.07
P_LATE_EVENT = 0.08
P_INVALID_TIMESTAMP = 0.05
P_UNKNOWN_SOURCE = 0.07
P_MISSING_USER_ID = 0.06

LATE_EVENT_DELAY = timedelta(minutes=5)  # exceeds the 3-minute watermark
INVALID_AMOUNTS = (-25_000, 15_000_000)

recent_events = deque(maxlen=10)


def chance(probability: float) -> bool:
    return random.random() < probability


def iso_utc(delay: timedelta = timedelta(0)) -> str:
    return (datetime.now(timezone.utc) - delay).isoformat().replace("+00:00", "Z")


def generate_transaction() -> dict:
    if recent_events and chance(P_DUPLICATE):
        return random.choice(recent_events)

    if chance(P_INVALID_AMOUNT):
        amount = random.choice(INVALID_AMOUNTS)
    else:
        amount = random.randint(50_000, 5_000_000)

    if chance(P_INVALID_TIMESTAMP):
        timestamp = "INVALID_TIME_FORMAT"
    elif chance(P_LATE_EVENT):
        timestamp = iso_utc(LATE_EVENT_DELAY)
    else:
        timestamp = iso_utc()

    if chance(P_UNKNOWN_SOURCE):
        source = "smartwatch"
    else:
        source = random.choice(VALID_SOURCES)

    event = {
        "user_id": None if chance(P_MISSING_USER_ID) else f"U{random.randint(10000, 99999)}",
        "amount": amount,
        "timestamp": timestamp,
        "source": source,
    }
    recent_events.append(event)
    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )
    print(f"Producer started, sending events to topic: {TOPIC_NAME}")

    try:
        while True:
            event = generate_transaction()
            producer.send(TOPIC_NAME, value=event)
            print(f"Sent: {event}")
            time.sleep(random.uniform(1.0, 2.0))
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()