import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent: {msg.value().decode()}")


def generate_telemetry():
    truck_id = f"TRUCK-{random.randint(1, 100):03d}"

    return {
        "truck_id": truck_id,
        "temperature": round(random.uniform(-10, 45), 2),
        "speed": round(random.uniform(0, 100), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    print("StreamForge telemetry producer started.")

    while True:
        telemetry = generate_telemetry()

        producer.produce(
            KAFKA_TOPIC,
            key=telemetry["truck_id"],
            value=json.dumps(telemetry).encode(),
            callback=delivery_report,
        )

        producer.poll(0)
        time.sleep(1)


if __name__ == "__main__":
    main()
