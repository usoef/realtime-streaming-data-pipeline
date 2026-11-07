import json
import time
from collections import deque
from threading import Thread, Lock

import pandas as pd
import streamlit as st
from kafka import KafkaConsumer

st.set_page_config(page_title="Real-Time Streaming Dashboard", page_icon="📊", layout="wide")

st.title("📊 Real-Time Streaming Data Pipeline Dashboard")
st.markdown("Memantau arus transaksi data, validasi, dan *Dead Letter Queue (DLQ)* secara *real-time* dari Apache Kafka.")

col1, col2, col3 = st.columns(3)


@st.cache_resource
def get_store():
    # Penyimpanan bersama antara thread consumer dan halaman Streamlit
    return {"lock": Lock(), "log": deque(maxlen=500), "error": None}


def consume_kafka(store):
    # Jangan panggil st.* di sini, thread ini tidak punya konteks Streamlit
    try:
        consumer = KafkaConsumer(
            'transactions_valid',
            'transactions_dlq',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='dashboard-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        for message in consumer:
            record = message.value
            record['topic'] = message.topic
            record['received_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
            with store["lock"]:
                store["log"].append(record)
    except Exception as e:
        store["error"] = str(e)
        print(f"Kafka consumer error: {e}")


@st.cache_resource
def start_background_consumer():
    store = get_store()
    t = Thread(target=consume_kafka, args=(store,), daemon=True)
    t.start()
    return t


store = get_store()
start_background_consumer()

st.subheader("📥 Live Stream Events Log")

if store["error"]:
    st.error(f"Kafka consumer berhenti: {store['error']}")

with store["lock"]:
    records = list(store["log"])

if records:
    df = pd.DataFrame(records)

    valid_count = len(df[df['topic'] == 'transactions_valid'])
    dlq_count = len(df[df['topic'] == 'transactions_dlq'])

    with col1:
        st.metric("Total Events Processed", len(df))
    with col2:
        st.metric("Valid Transactions", valid_count, delta="Success")
    with col3:
        st.metric("DLQ (Invalid/Errors)", dlq_count, delta="-Errors", delta_color="inverse")

    st.dataframe(df.tail(15), use_container_width=True)
else:
    st.info("Menunggu data masuk dari Kafka... Pastikan Producer dan Spark Job sedang berjalan.")

time.sleep(2)
st.rerun()