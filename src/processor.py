import json
from datetime import datetime
from confluent_kafka import Consumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID, TEMPERATURE_MIN

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
    "auto.offset.reset": "earliest",
})

def process_message(message):
    data = json.loads(message.value().decode("utf-8"))

    if data["temperature"] <= TEMPERATURE_MIN:
        return

    processed_event = {
        "truck_id": data["truck_id"],
        "temperature": data["temperature"],
        "speed": data["speed"],
        "timestamp": data["timestamp"],
    }

    event_time = datetime.fromisoformat(
        processed_event["timestamp"].replace("Z", "+00:00")
    )

    print(
        f"Processed | {processed_event['truck_id']} | "
        f"Temp: {processed_event['temperature']}°C | "
        f"Speed: {processed_event['speed']} km/h | "
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
