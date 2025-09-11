import os, json, time, random, uuid, datetime as dt
from confluent_kafka import Producer
import datetime as dt

# Py3.12 has dt.UTC; older versions use dt.timezone.utc
try:
    UTC = dt.UTC
except AttributeError:  # Py<=3.11
    UTC = dt.timezone.utc

FF_EVENTS_TARGET = int(os.getenv("FF_EVENTS_TARGET", "0"))

def now_utc_z() -> str:
    # RFC3339/ISO-8601 with trailing Z
    return dt.datetime.now(UTC).isoformat().replace("+00:00", "Z")


bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream.events")
FF_START_DATE = os.getenv("FF_START_DATE")  # e.g., "2025-09-01"
FF_DAYS = int(os.getenv("FF_DAYS", "0"))
FF_RATE = float(os.getenv("FF_RATE", "20"))  # simulated events per second
producer = Producer({"bootstrap.servers": bootstrap})

users = [f"u_{i}" for i in range(1, 2001)]
products = [f"sku_{i}" for i in range(1, 301)]
pages = ["/", "/search", "/product", "/cart", "/checkout"]
referrers = ["google", "email", "direct", "ads"]

def make_event(ts: dt.datetime | None = None):
    etype = random.choices(
        ["page_view","add_to_cart","checkout","purchase"],
        weights=[0.75,0.15,0.07,0.03]
    )[0]
    price = round(random.uniform(5, 120), 2) if etype=="purchase" else None
    return {
        "event_time": now_utc_z(),
        "user_id": random.choice(users),
        "session_id": str(uuid.uuid4()),
        "event_type": etype,
        "page": random.choice(pages),
        "product_id": random.choice(products),
        "price": price,
        "currency": "USD",
        "referrer": random.choice(referrers),
        "user_agent": "Mozilla/5.0",
        "event_ts": (ts or dt.datetime.now(UTC)).isoformat().replace("+00:00", "Z")
    }

def sim_timestamps():
    """
    Yields event timestamps.
      - Fast-forward mode (if FF_START_DATE and FF_DAYS > 0):
          * If FF_EVENTS_TARGET > 0: emit exactly ~FF_EVENTS_TARGET timestamps
            evenly spaced over [start, start+days).
          * Else: emit at a simulated rate of FF_RATE events/second (no sleeps).
      - Realtime mode (fallback): yield now() forever.
    """
    if FF_START_DATE and FF_DAYS > 0:
        # Parse start date (allow "YYYY-MM-DD" or full ISO)
        try:
            start = dt.datetime.fromisoformat(FF_START_DATE)
        except ValueError:
            start = dt.datetime.strptime(FF_START_DATE, "%Y-%m-%d")

        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)

        end = start + dt.timedelta(days=FF_DAYS)
        duration = end - start

        if FF_EVENTS_TARGET > 0:
            # Evenly distribute a fixed number of events across the window
            step = duration / FF_EVENTS_TARGET
        else:
            # Use a per-second simulated rate
            rate = FF_RATE if FF_RATE > 0 else 1.0
            step = dt.timedelta(seconds=1.0 / rate)

        t = start
        while t < end:
            yield t
            t += step
        return  # fast-forward ends cleanly

    # Realtime fallback (infinite stream)
    while True:
        yield dt.datetime.now(UTC)

def produce_all(producer):
    """
    Produce events to TOPIC using sim_timestamps() and make_event().
      - Fast-forward mode (FF_START_DATE set and FF_DAYS > 0):
          Generates a finite stream, does NOT sleep, and flushes at the end.
      - Realtime mode (fallback):
          Infinite loop with a tiny sleep to avoid busy-waiting.
    """
    fast_forward = bool(FF_START_DATE and FF_DAYS > 0)

    try:
        for ts in sim_timestamps():
            evt = make_event(ts)
            payload = json.dumps(evt).encode("utf-8")

            # Be resilient to temporary queue pressure
            try:
                producer.produce(TOPIC, payload)
            except BufferError:
                # Let the internal queue drain a bit, then retry once
                producer.poll(0.1)
                producer.produce(TOPIC, payload)

            # Drive delivery callbacks and keep the queue flowing
            producer.poll(0)

            # Only throttle in realtime mode (fast-forward should blast through)
            if not fast_forward:
                time.sleep(0.05)

        # If we ever exit the loop (finite fast-forward), ensure everything is delivered
        if fast_forward:
            producer.flush()

    except KeyboardInterrupt:
        # Graceful stop on Ctrl+C
        pass
    finally:
        # In case of any early exit, make a final attempt to deliver queued messages
        if fast_forward:
            producer.flush()


