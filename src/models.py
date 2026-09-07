from dataclasses import dataclass
from datetime import datetime


@dataclass
class TruckTelemetry:
    truck_id: str
    temperature: float
    speed: float
    timestamp: datetime
