import json
from datetime import datetime

from confluent_kafka import Consumer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
)


consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})


def process_message(message):
    data = json.loads(message.value().decode("utf-8"))

    event_time = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    print(
        f"Processed | {data['truck_id']} | "
        f"Temp: {data['temperature']}°C | "
        f"Speed: {data['speed']} km/h | "
        f"Time: {event_time}"
    )


def main():
    consumer.subscribe([KAFKA_TOPIC])

    print("StreamForge processor started.")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print(f"Kafka error: {message.error()}")
                continue

            process_message(message)

    except KeyboardInterrupt:
        print("\nProcessor stopped.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()

