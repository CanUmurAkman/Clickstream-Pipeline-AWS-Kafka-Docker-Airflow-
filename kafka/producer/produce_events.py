import os, json, time, random, uuid, datetime as dt
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "clickstream.events")
producer = Producer({"bootstrap.servers": BOOTSTRAP})

users = [f"u_{i}" for i in range(1, 2001)]
products = [f"sku_{i}" for i in range(1, 301)]
pages = ["/", "/search", "/product", "/cart", "/checkout"]
referrers = ["google", "email", "direct", "ads"]

def make_event():
    etype = random.choices(
        ["page_view","add_to_cart","checkout","purchase"],
        weights=[0.75,0.15,0.07,0.03]
    )[0]
    price = round(random.uniform(5, 120), 2) if etype=="purchase" else None
    return {
        "event_time": dt.datetime.utcnow().isoformat()+"Z",
        "user_id": random.choice(users),
        "session_id": str(uuid.uuid4()),
        "event_type": etype,
        "page": random.choice(pages),
        "product_id": random.choice(products),
        "price": price,
        "currency": "USD",
        "referrer": random.choice(referrers),
        "user_agent": "Mozilla/5.0"
    }

while True:
    evt = make_event()
    producer.produce(TOPIC, json.dumps(evt).encode("utf-8"))
    producer.poll(0)
    time.sleep(0.05)
