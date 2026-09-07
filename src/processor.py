import json
from collections import defaultdict, deque
from datetime import datetime, timedelta

from confluent_kafka import Consumer

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    TEMPERATURE_MIN,
    WINDOW_SECONDS,
)

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})

# Store recent events separately for each truck
truck_events = defaultdict(deque)


def process_message(message):
    data = json.loads(message.value().decode("utf-8"))

    # Filter
    if data["temperature"] <= TEMPERATURE_MIN:
        return

    event_time = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    truck_id = data["truck_id"]
    temperature = data["temperature"]

    events = truck_events[truck_id]
    events.append((event_time, temperature))

    # Keep only the latest 5 minutes
    window_start = event_time - timedelta(seconds=WINDOW_SECONDS)

    while events and events[0][0] < window_start:
        events.popleft()

    # Calculate rolling average
    average_temperature = sum(
        temp for _, temp in events
    ) / len(events)

    print(
        f"Processed | {truck_id} | "
        f"Temp: {temperature}°C | "
        f"Speed: {data['speed']} km/h | "
        f"5-min Avg Temp: {average_temperature:.2f}°C | "
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
