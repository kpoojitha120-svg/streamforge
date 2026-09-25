import json
import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from prometheus_client import generate_latest

app = FastAPI(title="StreamForge API")

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


@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "StreamForge API"}


@app.get("/api/status")
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


@app.get("/metrics")
def metrics():
    return Response(
        generate_latest(),
        media_type="text/plain"
    )