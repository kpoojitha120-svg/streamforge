import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer
from prometheus_client import Counter, Gauge
from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_GROUP_ID,
    TEMPERATURE_MIN,
    WINDOW_SECONDS,
)
events_processed = Counter("streamforge_events_processed_total", "Total processed events")
events_per_second_metric = Gauge("streamforge_events_per_second", "Current events per second")
processing_lag_metric = Gauge("streamforge_processing_lag_seconds", "Current processing lag in seconds")
worker_status_metric = Gauge("streamforge_worker_status", "Worker status: 1=running, 0=stopped")
consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": "streamforge-worker-final",
    "auto.offset.reset": "earliest",
})

truck_events = defaultdict(deque)
processed_times = deque()

history = {
    "timestamps": [],
    "temperatures": [],
    "rolling_averages": [],
    "events_per_second": [],
    "processing_lag": [],
}

STATUS_FILE = "/home/lenovoc/StreamForge/data/latest_status.json"


def write_status(data):
    os.makedirs("/home/lenovoc/StreamForge/data", exist_ok=True)

    history["timestamps"].append(data["timestamp"])
    history["temperatures"].append(data["temperature"])
    history["rolling_averages"].append(data["rolling_average"])
    history["events_per_second"].append(data["events_per_second"])
    history["processing_lag"].append(data["processing_lag"])

    for key in history:
        history[key] = history[key][-60:]

    status = {
        "temperature": data["temperature"],
        "speed": data["speed"],
        "rolling_average": data["rolling_average"],
        "events_per_second": data["events_per_second"],
        "processing_lag": data["processing_lag"],
        "worker_status": "RUNNING",
        "worker_id": "worker-1",
        "updated_at": data["timestamp"],
        "history": history,
    }

    with open(STATUS_FILE, "w", encoding="utf-8") as file:
        json.dump(status, file, indent=2)


def process_message(message):
    data = json.loads(message.value().decode("utf-8"))

    if data["temperature"] <= TEMPERATURE_MIN:
        return

    event_time = datetime.fromisoformat(
        data["timestamp"].replace("Z", "+00:00")
    )

    truck_id = data["truck_id"]
    temperature = data["temperature"]

    events = truck_events[truck_id]
    events.append((event_time, temperature))

    window_start = event_time - timedelta(seconds=WINDOW_SECONDS)

    while events and events[0][0] < window_start:
        events.popleft()

    average_temperature = sum(
        temp for _, temp in events
    ) / len(events)

    now = time.time()
    processed_times.append(now)

    while processed_times and processed_times[0] < now - 10:
        processed_times.popleft()

    events_per_second = len(processed_times) / 10

    current_time = datetime.now(timezone.utc)

    processing_lag = max(
        0,
        (current_time - event_time).total_seconds()
    )
    events_processed.inc()
    events_per_second_metric.set(events_per_second)
    processing_lag_metric.set(processing_lag)


    dashboard_data = {
        "temperature": temperature,
        "speed": data["speed"],
        "rolling_average": round(average_temperature, 2),
        "events_per_second": round(events_per_second, 2),
        "processing_lag": round(processing_lag, 3),
        "timestamp": event_time.isoformat(),
    }

    write_status(dashboard_data)

    print(
        f"Processed | {truck_id} | "
        f"Temp: {temperature}°C | "
        f"Speed: {data['speed']} km/h | "
        f"5-min Avg Temp: {average_temperature:.2f}°C | "
        f"Events/sec: {events_per_second:.2f} | "
        f"Lag: {processing_lag:.3f}s | "
        f"Time: {event_time}"
    )


def main():
    consumer.subscribe([KAFKA_TOPIC])

    print("StreamForge processor started.")
    print(f"Dashboard status file: {STATUS_FILE}")

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
