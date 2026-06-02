"""
E-Commerce Synthetic Data Generator
Produces:
  - Batch: customer CSV, order JSON files → GCS landing zone
  - Streaming: clickstream + transaction events → Pub/Sub
"""

import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path

from google.cloud import pubsub_v1, storage
from faker import Faker

fake = Faker(["en_GB", "en_US", "de_DE", "fr_FR"])

# Config
PROJECT_ID = "cp-ecomm-demo"
LANDING_BUCKET = f"{PROJECT_ID}-landing-dev"
CLICKSTREAM_TOPIC = f"projects/{PROJECT_ID}/topics/ecommerce-clickstream-dev"
TRANSACTIONS_TOPIC = f"projects/{PROJECT_ID}/topics/ecommerce-transactions-dev"

PRODUCTS = [f"PROD-{i:04d}" for i in range(1, 201)]
CATEGORIES = ["electronics", "clothing", "home", "sports", "books", "food"]
COUNTRIES = ["UK", "US", "DE", "FR"]
SEGMENTS = ["high_value", "medium_value", "low_value", "new"]
EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "checkout", "search"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "apple_pay"]
ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]


def generate_customers(n: int = 1000) -> list[dict]:
    customers = []
    for _ in range(n):
        country = random.choice(COUNTRIES)
        customers.append({
            "customer_id": str(uuid.uuid4()),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "city": fake.city(),
            "country": country,
            "segment": random.choice(SEGMENTS),
            "signup_date": fake.date_between(start_date="-3y").isoformat(),
        })
    return customers


def generate_orders(customers: list[dict], n: int = 5000) -> list[dict]:
    orders = []
    for _ in range(n):
        customer = random.choice(customers)
        qty = random.randint(1, 5)
        price = round(random.uniform(5.0, 500.0), 2)
        orders.append({
            "order_id": str(uuid.uuid4()),
            "customer_id": customer["customer_id"],
            "product_id": random.choice(PRODUCTS),
            "quantity": qty,
            "total_amount": round(qty * price, 2),
            "status": random.choice(ORDER_STATUSES),
            "order_date": fake.date_time_between(start_date="-90d").isoformat(),
        })
    return orders


def generate_clickstream_event(customers: list[dict]) -> dict:
    return {
        "user_id": random.choice(customers)["customer_id"],
        "session_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "page_url": f"/products/{random.choice(PRODUCTS)}",
        "product_id": random.choice(PRODUCTS) if random.random() > 0.3 else None,
    }


def generate_transaction_event(customers: list[dict]) -> dict:
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": random.choice(customers)["customer_id"],
        "amount": round(random.uniform(10.0, 2000.0), 2),
        "currency": random.choice(["GBP", "USD", "EUR"]),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(["success", "failed", "pending"]),
    }


# ─── Batch: Upload to GCS ────────────────────────────────────────────────────

def upload_batch_data():
    storage_client = storage.Client()
    bucket = storage_client.bucket(LANDING_BUCKET)

    print("Generating customers...")
    customers = generate_customers(1000)
    blob = bucket.blob(f"customers/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
    blob.upload_from_string(
        "\n".join(json.dumps(c) for c in customers),
        content_type="application/json"
    )
    print(f"  Uploaded {len(customers)} customers")

    print("Generating orders...")
    orders = generate_orders(customers, 5000)
    blob = bucket.blob(f"orders/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json")
    blob.upload_from_string(
        "\n".join(json.dumps(o) for o in orders),
        content_type="application/json"
    )
    print(f"  Uploaded {len(orders)} orders")

    return customers


# ─── Streaming: Publish to Pub/Sub ────────────────────────────────────────────

def stream_events(customers: list[dict], duration_seconds: int = 300, rate_per_sec: int = 10):
    publisher = pubsub_v1.PublisherClient()
    start = time.time()
    count = 0

    print(f"Streaming events for {duration_seconds}s at ~{rate_per_sec} events/sec...")

    while time.time() - start < duration_seconds:
        for _ in range(rate_per_sec):
            # 70% clickstream, 30% transactions
            if random.random() < 0.7:
                event = generate_clickstream_event(customers)
                publisher.publish(CLICKSTREAM_TOPIC, json.dumps(event).encode())
            else:
                event = generate_transaction_event(customers)
                publisher.publish(TRANSACTIONS_TOPIC, json.dumps(event).encode())
            count += 1

        time.sleep(1)

    print(f"  Published {count} events")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="E-Commerce Data Generator")
    parser.add_argument("--mode", choices=["batch", "stream", "both"], default="both")
    parser.add_argument("--stream-duration", type=int, default=300)
    parser.add_argument("--stream-rate", type=int, default=10)
    args = parser.parse_args()

    customers = []
    if args.mode in ("batch", "both"):
        customers = upload_batch_data()

    if args.mode in ("stream", "both"):
        if not customers:
            customers = generate_customers(1000)
        stream_events(customers, args.stream_duration, args.stream_rate)
