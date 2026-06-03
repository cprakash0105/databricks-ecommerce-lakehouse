"""
E-Commerce Kafka Streaming Producer
Sends clickstream + transaction events to Confluent Cloud Kafka.
"""

import json
import random
import time
import uuid
import os
from datetime import datetime

from confluent_kafka import Producer

# ─── Config (set these as environment variables) ──────────────────────────────
BOOTSTRAP_SERVER = os.environ.get("KAFKA_BOOTSTRAP", "pkc-619z3.us-east1.gcp.confluent.cloud:9092")
API_KEY = os.environ.get("KAFKA_API_KEY", "")
API_SECRET = os.environ.get("KAFKA_API_SECRET", "")

CLICKSTREAM_TOPIC = "ecommerce.clickstream"
TRANSACTIONS_TOPIC = "ecommerce.transactions"

# ─── Kafka Producer Config ────────────────────────────────────────────────────
conf = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET,
}

producer = Producer(conf)

# ─── Sample Data ──────────────────────────────────────────────────────────────
CUSTOMER_IDS = [str(uuid.uuid4()) for _ in range(100)]
PRODUCTS = [f"PROD-{i:04d}" for i in range(1, 201)]
EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "checkout", "search"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "apple_pay"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/search", "/account"]


def generate_clickstream_event():
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(CUSTOMER_IDS),
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "page_url": random.choice(PAGES) + "/" + random.choice(PRODUCTS),
        "product_id": random.choice(PRODUCTS) if random.random() > 0.3 else None,
        "timestamp": datetime.utcnow().isoformat(),
    }


def generate_transaction_event():
    return {
        "txn_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(CUSTOMER_IDS),
        "amount": round(random.uniform(10.0, 2000.0), 2),
        "currency": random.choice(["GBP", "USD", "EUR"]),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(["success", "failed", "pending"]),
        "timestamp": datetime.utcnow().isoformat(),
    }


def delivery_report(err, msg):
    if err:
        print(f"  ERROR: {err}")


def stream_events(duration_seconds=120, rate_per_sec=5):
    start = time.time()
    count = 0

    print(f"Streaming to Kafka ({BOOTSTRAP_SERVER})...")
    print(f"  Duration: {duration_seconds}s, Rate: {rate_per_sec} events/sec")
    print(f"  Topics: {CLICKSTREAM_TOPIC}, {TRANSACTIONS_TOPIC}")

    while time.time() - start < duration_seconds:
        for _ in range(rate_per_sec):
            if random.random() < 0.7:
                event = generate_clickstream_event()
                producer.produce(
                    CLICKSTREAM_TOPIC,
                    key=event["user_id"],
                    value=json.dumps(event),
                    callback=delivery_report
                )
            else:
                event = generate_transaction_event()
                producer.produce(
                    TRANSACTIONS_TOPIC,
                    key=event["customer_id"],
                    value=json.dumps(event),
                    callback=delivery_report
                )
            count += 1

        producer.poll(0)
        time.sleep(1)

        if count % 50 == 0:
            print(f"  Sent {count} events...")

    producer.flush()
    print(f"Done. Total events sent: {count}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka Streaming Producer")
    parser.add_argument("--duration", type=int, default=120, help="Duration in seconds")
    parser.add_argument("--rate", type=int, default=5, help="Events per second")
    args = parser.parse_args()

    if not API_KEY or not API_SECRET:
        print("ERROR: Set KAFKA_API_KEY and KAFKA_API_SECRET environment variables")
        exit(1)

    stream_events(args.duration, args.rate)
