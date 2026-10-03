import json
import os
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from confluent_kafka import Consumer, Producer
from prometheus_client import Counter, Gauge
from rocksdict import Rdict

from config.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_STATE_CHANGELOG_TOPIC,
    KAFKA_GROUP_ID,
    WORKER_ID,
    TEMPERATURE_MIN,
    WINDOW_SECONDS,
)

events_processed = Counter(
    "streamforge_events_processed_total",
    "Total processed events",
)
events_filtered_metric = Counter(
    "streamforge_events_filtered_total",
    "Total filtered events",
)
events_late_metric = Counter(
    "streamforge_late_events_total",
    "Total late-arriving events",
)
events_per_second_metric = Gauge(
    "streamforge_events_per_second",
    "Current events per second",
)
events_total_metric = Gauge(
    "streamforge_events_total",
    "Current processed event count",
)
processing_lag_metric = Gauge(
    "streamforge_processing_lag_seconds",
    "Current processing lag in seconds",
)
processing_latency_metric = Gauge(
    "streamforge_processing_latency_seconds",
    "Actual event processing latency in seconds",
)
worker_status_metric = Gauge(
    "streamforge_worker_status",
    "Worker status: 1=running, 0=stopped",
)
worker_uptime_metric = Gauge(
    "streamforge_worker_uptime_seconds",
    "Worker uptime in seconds",
)
worker_last_poll_metric = Gauge(
    "streamforge_worker_last_poll_timestamp",
    "Last Kafka poll timestamp",
)
messages_polled_metric = Counter(
    "streamforge_messages_polled_total",
    "Total Kafka messages polled",
)
kafka_errors_metric = Counter(
    "streamforge_kafka_errors_total",
    "Total Kafka consumer errors",
)
state_persisted_metric = Gauge(
    "streamforge_state_persisted_trucks",
    "Number of truck states persisted in RocksDB",
)

changelog_producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
})

consumer = Consumer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": KAFKA_GROUP_ID,
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
STATE_DIR = "/home/lenovoc/StreamForge/data/rocksdb"
STATE_TRUCK_IDS_KEY = "__truck_ids__"

assigned_partitions = []

os.makedirs(STATE_DIR, exist_ok=True)
state_db = Rdict(STATE_DIR)


def load_persistent_state():
    """Restore truck event windows from RocksDB."""
    try:
        raw_ids = state_db.get(STATE_TRUCK_IDS_KEY)

        if not raw_ids:
            print("RocksDB state: no previous truck state found.")
            return

        truck_ids = json.loads(raw_ids)

        restored = 0

        for truck_id in truck_ids:
            raw_events = state_db.get(f"truck:{truck_id}")

            if not raw_events:
                continue

            events = json.loads(raw_events)

            truck_events[truck_id] = deque(
                (
                    datetime.fromisoformat(timestamp),
                    temperature,
                )
                for timestamp, temperature in events
            )

            restored += 1

        state_persisted_metric.set(restored)
        print(f"RocksDB state restored: {restored} truck(s)")

    except Exception as exc:
        print(f"RocksDB state restore warning: {exc}")


def persist_truck_state(truck_id):
    """Persist one truck's current rolling-window state."""
    events = truck_events[truck_id]

    serialized_events = [
        [event_time.isoformat(), temperature]
        for event_time, temperature in events
    ]

    state_db[f"truck:{truck_id}"] = json.dumps(serialized_events)

    truck_ids = sorted(truck_events.keys())
    state_db[STATE_TRUCK_IDS_KEY] = json.dumps(truck_ids)

    state_db.flush()

    changelog_producer.produce(
        KAFKA_STATE_CHANGELOG_TOPIC,
        key=truck_id,
        value=json.dumps({
            "truck_id": truck_id,
            "events": serialized_events,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }).encode("utf-8"),
    )
    changelog_producer.poll(0)

    state_persisted_metric.set(len(truck_ids))


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
        "worker_id": WORKER_ID,
        "assigned_partitions": assigned_partitions,
        "updated_at": data["timestamp"],
        "history": history,
    }

    with open(STATUS_FILE, "w", encoding="utf-8") as file:
        json.dump(status, file, indent=2)


def on_assign(consumer, partitions):
    global assigned_partitions

    assigned_partitions = [p.partition for p in partitions]

    print(
        f"Worker {WORKER_ID} assigned partitions: "
        f"{assigned_partitions}"
    )

    consumer.assign(partitions)


def on_revoke(consumer, partitions):
    print(
        f"Worker {WORKER_ID} revoked partitions: "
        f"{[p.partition for p in partitions]}"
    )


def process_message(message):
    processing_start = time.time()
    try:
        data = json.loads(message.value().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"Invalid telemetry JSON: {exc}")
        kafka_errors_metric.inc()
        return

    required_fields = ("truck_id", "temperature", "speed", "timestamp")
    if any(field not in data for field in required_fields):
        print("Invalid telemetry: missing required field")
        kafka_errors_metric.inc()
        return

    try:
        temperature = float(data["temperature"])
        speed = float(data["speed"])
        event_time = datetime.fromisoformat(
            str(data["timestamp"]).replace("Z", "+00:00")
        )
        truck_id = str(data["truck_id"]).strip()
        if not truck_id:
            raise ValueError("empty truck_id")
    except (TypeError, ValueError) as exc:
        print(f"Invalid telemetry values: {exc}")
        kafka_errors_metric.inc()
        return

    if temperature <= TEMPERATURE_MIN:
        events_filtered_metric.inc()
        return

    events = truck_events[truck_id]
    events.append((event_time, temperature))

    window_start = event_time - timedelta(seconds=WINDOW_SECONDS)

    while events and events[0][0] < window_start:
        events.popleft()

    average_temperature = sum(
        temp for _, temp in events
    ) / len(events)

    persist_truck_state(truck_id)

    now = time.time()
    processed_times.append(now)

    while processed_times and processed_times[0] < now - 10:
        processed_times.popleft()

    events_per_second = len(processed_times) / 10

    current_time = datetime.now(timezone.utc)

    processing_lag = max(
        0,
        (current_time - event_time).total_seconds(),
    )

    if processing_lag > WINDOW_SECONDS:
        events_late_metric.inc()

    events_processed.inc()
    events_total_metric.inc()
    events_per_second_metric.set(events_per_second)
    processing_lag_metric.set(processing_lag)
    processing_latency_metric.set(max(0, time.time() - processing_start))

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
    load_persistent_state()

    consumer.subscribe(
        [KAFKA_TOPIC],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )

    print(
        f"StreamForge processor started | Worker: {WORKER_ID}"
    )
    print(f"Worker ID: {WORKER_ID}")
    print(f"Dashboard status file: {STATUS_FILE}")
    print(f"RocksDB state directory: {STATE_DIR}")

    worker_status_metric.set(1)
    worker_start_time = time.time()

    try:
        while True:
            worker_uptime_metric.set(
                time.time() - worker_start_time
            )
            worker_last_poll_metric.set(time.time())

            message = consumer.poll(1.0)

            if message is None:
                continue

            messages_polled_metric.inc()

            if message.error():
                kafka_errors_metric.inc()
                print(f"Kafka error: {message.error()}")
                continue

            process_message(message)

    except KeyboardInterrupt:
        print("\nProcessor stopped.")

    finally:
        worker_status_metric.set(0)
        state_db.flush()
        state_db.close()
        consumer.close()


if __name__ == "__main__":
    main()
