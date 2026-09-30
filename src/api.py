import json
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import generate_latest

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
        "pipeline": [
            "Kafka",
            "Telemetry Producer",
            "Python Processor",
            "Temperature Filter",
            "Event Mapping",
            "5-Minute Window",
            "RocksDB State",
            "Prometheus",
            "FastAPI",
            "Dashboard",
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
    return Response(
        generate_latest(),
        media_type="text/plain"
    )