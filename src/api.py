import json
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import generate_latest, Gauge

API_VERSION = "1.0"

app = FastAPI(title="StreamForge API", version=API_VERSION, description="Live telemetry processing API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

STATUS_FILE = "/home/lenovoc/StreamForge/data/latest_status.json"

streamforge_events_per_second = Gauge("streamforge_events_per_second", "Actual events processed per second")
streamforge_processing_lag = Gauge("streamforge_processing_lag_seconds", "Actual processing lag in seconds")
streamforge_rolling_average = Gauge("streamforge_rolling_average_temperature", "Actual rolling average temperature")
streamforge_temperature = Gauge("streamforge_temperature", "Latest actual temperature")
streamforge_worker_status = Gauge("streamforge_worker_status", "Worker status: 1=running, 0=not running")
streamforge_processing_latency = Gauge(
    "streamforge_processing_latency_seconds",
    "Actual event processing latency in seconds",
)


@app.get("/")
def root():
    return {
        "project": "StreamForge",
        "status": "API running"
    }


@app.get("/api/info")
def project_info():
    return {"project": "StreamForge", "version": API_VERSION, "service": "Distributed Event Processor"}


@app.get("/ready")
def readiness_check():
    return {"ready": True, "service": "StreamForge API"}


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "StreamForge API", "uptime": "active"}


@app.get("/api/worker")
def get_worker():
    status = get_status()
    return {
        "worker_id": status["worker_id"],
        "worker_status": status["worker_status"],
        "assigned_partitions": status.get("assigned_partitions", []),
        "events_per_second": status["events_per_second"],
        "processing_lag": status["processing_lag"],
        "updated_at": status["updated_at"],
    }


@app.get("/api/partitions")
def get_partitions():
    status = get_status()
    partitions = status.get("assigned_partitions", [])
    return {
        "worker_id": status["worker_id"],
        "partition_count": len(partitions),
        "partitions": partitions,
    }


@app.get("/api/worker/summary")
def worker_summary():
    status = get_status()
    return {
        "worker_id": status["worker_id"],
        "status": status["worker_status"],
        "partitions": status.get("assigned_partitions", []),
        "events_per_second": status["events_per_second"],
        "processing_lag": status["processing_lag"],
        "rolling_average": status["rolling_average"],
        "last_updated": status["updated_at"],
    }


@app.get("/api/status", response_model=None)
def get_status():
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as file:
                return json.load(file)
        except (json.JSONDecodeError, OSError):
            pass

    return {
        "temperature": 0,
        "speed": 0,
        "rolling_average": 0,
        "events_per_second": 0,
        "processing_lag": 0,
        "worker_status": "WAITING FOR DATA",
        "worker_id": "worker-1",
        "updated_at": None,
        "history": {
            "timestamps": [],
            "temperatures": [],
            "rolling_averages": [],
            "events_per_second": [],
            "processing_lag": []
        }
    }


@app.get("/api/metrics-summary")
def metrics_summary():
    status = get_status()
    return {"events_per_second": status["events_per_second"], "processing_lag": status["processing_lag"], "rolling_average": status["rolling_average"]}


@app.get("/api/topology")
def get_topology():
    status = get_status()
    return {
        "project": "StreamForge",
        "nodes": [
            {"id": "kafka", "type": "source", "label": "Kafka"},
            {"id": "producer", "type": "source", "label": "Telemetry Producer"},
            {"id": "processor", "type": "processor", "label": "Python Processor"},
            {"id": "filter", "type": "processor", "label": "Temperature Filter"},
            {"id": "mapping", "type": "processor", "label": "Event Mapping"},
            {"id": "window", "type": "processor", "label": "5-Minute Window"},
            {"id": "state", "type": "state", "label": "RocksDB State"},
            {"id": "metrics", "type": "monitoring", "label": "Prometheus"},
            {"id": "api", "type": "api", "label": "FastAPI"},
            {"id": "dashboard", "type": "dashboard", "label": "Dashboard"},
        ],
        "edges": [
            ["producer", "kafka"],
            ["kafka", "processor"],
            ["processor", "filter"],
            ["filter", "mapping"],
            ["mapping", "window"],
            ["window", "state"],
            ["state", "metrics"],
            ["metrics", "api"],
            ["api", "dashboard"],
        ],
        "worker": {
            "id": status["worker_id"],
            "status": status["worker_status"],
            "partitions": status.get("assigned_partitions", []),
        },
    }


@app.get("/api/dashboard/metrics")
def dashboard_metrics():
    status = get_status()
    return {
        "events_per_second": status["events_per_second"],
        "processing_lag": status["processing_lag"],
        "rolling_average": status["rolling_average"],
        "temperature": status["temperature"],
        "speed": status["speed"],
        "updated_at": status["updated_at"],
    }


@app.get("/metrics")
def metrics():
    status = get_status()
    streamforge_events_per_second.set(status.get("events_per_second", 0))
    streamforge_processing_lag.set(status.get("processing_lag", 0))
    streamforge_rolling_average.set(status.get("rolling_average", 0))
    streamforge_temperature.set(status.get("temperature", 0))
    streamforge_worker_status.set(1 if status.get("worker_status") == "RUNNING" else 0)
    streamforge_processing_latency.set(status.get("processing_latency", 0))
    return Response(generate_latest(), media_type="text/plain")