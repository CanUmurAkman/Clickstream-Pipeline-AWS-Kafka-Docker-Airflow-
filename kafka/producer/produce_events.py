import os
import json
import time
import uuid
import random
import datetime as dt
from confluent_kafka import Producer

# Environment variables
TOPIC = os.getenv("KAFKA_TOPIC", os.getenv("TOPIC", "clickstream.events"))

FF_START_DATE = os.getenv("FF_START_DATE")           # e.g., "2025-09-01"
FF_DAYS = int(os.getenv("FF_DAYS", "0"))
FF_EVENTS_TARGET = int(os.getenv("FF_EVENTS_TARGET", "0"))
FF_RATE = float(os.getenv("FF_RATE", "20"))

UTC = dt.timezone.utc

conf = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))}
producer = Producer(conf)


USERS = [f"u_{i}" for i in range(1, 2001)]
PRODUCTS = [f"sku_{i}" for i in range(1, 301)]
PAGES = ["/", "/search", "/product", "/cart", "/checkout"]
REFERRERS = ["google", "email", "direct", "ads"]

def make_event(ts: dt.datetime | None = None):
    etype = random.choices(
        ["page_view", "add_to_cart", "checkout", "purchase"],
        weights=[0.75, 0.15, 0.07, 0.03],
    )[0]
    price = round(random.uniform(5, 120), 2) if etype == "purchase" else None
    return {
        "event_ts": (ts or dt.datetime.now(UTC)).isoformat().replace("+00:00", "Z"),
        "user_id": random.choice(USERS),
        "session_id": str(uuid.uuid4()),
        "event_type": etype,
        "page": random.choice(PAGES),
        "product_id": random.choice(PRODUCTS),
        "price": price,
        "currency": "USD",
        "referrer": random.choice(REFERRERS),
        "user_agent": "Mozilla/5.0",
    }


def sim_timestamps():
    if FF_START_DATE and FF_DAYS > 0:
        # Parse start date (allow YYYY-MM-DD or full ISO)
        try:
            start = dt.datetime.fromisoformat(FF_START_DATE)
        except ValueError:
            start = dt.datetime.strptime(FF_START_DATE, "%Y-%m-%d")
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)

        end = start + dt.timedelta(days=FF_DAYS)
        duration = end - start

        if FF_EVENTS_TARGET > 0:
            step = duration / FF_EVENTS_TARGET
        else:
            step = dt.timedelta(seconds=1.0 / (FF_RATE if FF_RATE > 0 else 1.0))

        t = start
        while t < end:
            yield t
            t += step
        return

    # fallback: realtime
    while True:
        yield dt.datetime.now(UTC)


def produce_all():
    fast_forward = bool(FF_START_DATE and FF_DAYS > 0)
    try:
        for ts in sim_timestamps():
            evt = make_event(ts)
            payload = json.dumps(evt).encode("utf-8")

            # Backpressure-safe produce
            while True:
                try:
                    producer.produce(TOPIC, payload)
                    break
                except BufferError:
                    # Queue full: give broker a chance to drain
                    producer.poll(0.5)
                    producer.flush(0.5)

            # Drive delivery callbacks
            producer.poll(0)

            if not fast_forward:
                time.sleep(0.05)

        if fast_forward:
            producer.flush()

    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()


if __name__ == "__main__":
    produce_all()
