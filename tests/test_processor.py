import json
import sys
import types
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

# Mock external services before importing the real processor module.
confluent_kafka = types.ModuleType("confluent_kafka")
confluent_kafka.Consumer = MagicMock
confluent_kafka.Producer = MagicMock
sys.modules["confluent_kafka"] = confluent_kafka

prometheus_client = types.ModuleType("prometheus_client")


class FakeMetric:
    def __init__(self, *args, **kwargs):
        self.value = 0

    def inc(self, amount=1):
        self.value += amount

    def set(self, value):
        self.value = value


prometheus_client.Counter = FakeMetric
prometheus_client.Gauge = FakeMetric
sys.modules["prometheus_client"] = prometheus_client

rocksdict = types.ModuleType("rocksdict")


class FakeRdict(dict):
    def __init__(self, *args, **kwargs):
        super().__init__()
    def flush(self):
        pass

    def close(self):
        pass


rocksdict.Rdict = FakeRdict
sys.modules["rocksdict"] = rocksdict

from src import processor


class FakeMessage:
    def __init__(self, payload):
        self._payload = json.dumps(payload).encode("utf-8")

    def value(self):
        return self._payload


def setup_function():
    processor.truck_events.clear()
    processor.processed_times.clear()

    processor.persist_truck_state = MagicMock()
    processor.write_status = MagicMock()


def test_valid_event_is_processed():
    event = {
        "truck_id": "TRUCK-001",
        "temperature": 25,
        "speed": 50,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    processor.process_message(FakeMessage(event))

    assert "TRUCK-001" in processor.truck_events
    assert len(processor.truck_events["TRUCK-001"]) == 1
    assert processor.events_processed.value == 1


def test_zero_temperature_is_filtered():
    event = {
        "truck_id": "TRUCK-002",
        "temperature": 0,
        "speed": 40,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    processor.process_message(FakeMessage(event))

    assert "TRUCK-002" not in processor.truck_events
    assert processor.events_filtered_metric.value == 1


def test_negative_temperature_is_filtered():
    event = {
        "truck_id": "TRUCK-003",
        "temperature": -5,
        "speed": 40,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    processor.process_message(FakeMessage(event))

    assert "TRUCK-003" not in processor.truck_events
    assert processor.events_filtered_metric.value >= 1


def test_missing_required_field_is_rejected():
    event = {
        "truck_id": "TRUCK-004",
        "temperature": 20,
        "speed": 40,
    }

    processor.process_message(FakeMessage(event))

    assert "TRUCK-004" not in processor.truck_events


def test_invalid_temperature_is_rejected():
    event = {
        "truck_id": "TRUCK-005",
        "temperature": "invalid",
        "speed": 40,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    processor.process_message(FakeMessage(event))

    assert "TRUCK-005" not in processor.truck_events


def test_multiple_trucks_are_tracked_separately():
    timestamp = datetime.now(timezone.utc).isoformat()

    for truck_id in ("TRUCK-010", "TRUCK-011", "TRUCK-012"):
        event = {
            "truck_id": truck_id,
            "temperature": 20,
            "speed": 30,
            "timestamp": timestamp,
        }
        processor.process_message(FakeMessage(event))

    assert set(processor.truck_events.keys()) == {
        "TRUCK-010",
        "TRUCK-011",
        "TRUCK-012",
    }


def test_five_minute_window_removes_old_events():
    now = datetime.now(timezone.utc)

    old_event = {
        "truck_id": "TRUCK-020",
        "temperature": 20,
        "speed": 30,
        "timestamp": (now - timedelta(seconds=301)).isoformat(),
    }

    current_event = {
        "truck_id": "TRUCK-020",
        "temperature": 30,
        "speed": 30,
        "timestamp": now.isoformat(),
    }

    processor.process_message(FakeMessage(old_event))
    processor.process_message(FakeMessage(current_event))

    events = processor.truck_events["TRUCK-020"]

    assert len(events) == 1
    assert events[0][1] == 30
