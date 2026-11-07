"""Streamlit dashboard that monitors the valid and DLQ Kafka topics in real time."""

import json
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from threading import Lock, Thread

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

KAFKA_SERVERS = ["localhost:9092"]
TOPIC_VALID = "transactions_valid"
TOPIC_DLQ = "transactions_dlq"
CONSUMER_GROUP = "dashboard-consumer-group"

MAX_LOG_SIZE = 500
TABLE_ROWS = 15
REFRESH_SECONDS = 2
DISPLAY_COLUMNS = ["received_at", "topic", "user_id", "amount", "source", "timestamp", "error_reason"]


@dataclass
class EventStore:
    """State shared between the Kafka consumer thread and the Streamlit UI."""

    lock: Lock = field(default_factory=Lock)
    log: deque = field(default_factory=lambda: deque(maxlen=MAX_LOG_SIZE))
    counts: Counter = field(default_factory=Counter)
    error: str | None = None


@st.cache_resource
def get_store() -> EventStore:
    return EventStore()


def consume_kafka(store: EventStore) -> None:
    # Runs in a background thread: do not call st.* functions here.
    try:
        consumer = KafkaConsumer(
            TOPIC_VALID,
            TOPIC_DLQ,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=CONSUMER_GROUP,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )
        for message in consumer:
            record = message.value
            record["topic"] = message.topic
            record["received_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
            with store.lock:
                store.log.append(record)
                store.counts[message.topic] += 1
    except Exception as exc:
        store.error = str(exc)
        print(f"Kafka consumer error: {exc}")


@st.cache_resource
def start_consumer() -> Thread:
    thread = Thread(target=consume_kafka, args=(get_store(),), daemon=True)
    thread.start()
    return thread


def share(count: int, total: int) -> str | None:
    return f"{count / total:.0%} of total" if total else None


@st.fragment(run_every=REFRESH_SECONDS)
def render_live_view() -> None:
    store = get_store()

    if store.error:
        st.error(f"Kafka consumer stopped: {store.error}")

    with store.lock:
        records = list(store.log)
        valid = store.counts[TOPIC_VALID]
        dlq = store.counts[TOPIC_DLQ]
    total = valid + dlq

    col_total, col_valid, col_dlq = st.columns(3)
    col_total.metric("Total Events Processed", total)
    col_valid.metric("Valid Transactions", valid, delta=share(valid, total))
    col_dlq.metric("DLQ (Invalid Events)", dlq, delta=share(dlq, total), delta_color="inverse")

    st.subheader("📥 Live Stream Events Log")
    if not records:
        st.info("Waiting for data from Kafka. Make sure the producer and the Spark job are running.")
        return

    latest = pd.DataFrame(records).tail(TABLE_ROWS).iloc[::-1]
    st.dataframe(latest.reindex(columns=DISPLAY_COLUMNS), hide_index=True)


st.set_page_config(page_title="Real-Time Streaming Dashboard", page_icon="📊", layout="wide")
st.title("📊 Real-Time Streaming Data Pipeline Dashboard")
st.markdown(
    "Monitor transaction flow, validation results, and the *Dead Letter Queue (DLQ)* "
    "in real time from Apache Kafka."
)

start_consumer()
render_live_view()