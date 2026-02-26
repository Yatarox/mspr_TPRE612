import psutil
import logging
import json
from pathlib import Path
import pandas as pd
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def log_memory(prefix: str = ""):
    try:
        rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
        logger.info(f"{prefix}Memory usage: {rss_mb:.1f} MB")
    except Exception:
        pass

def latest_version_dir(dataset_dir: Path) -> Optional[Path]:
    versions = [p for p in dataset_dir.iterdir() if p.is_dir()]
    if versions:
        versions.sort(key=lambda p: p.name)
        return versions[-1]
    required = ["stops.txt", "routes.txt", "trips.txt", "stop_times.txt", "agency.txt"]
    if any((dataset_dir / f).exists() for f in required):
        return dataset_dir
    return None

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        logger.warning(f"File not found: {path}")
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8", encoding_errors="replace")

def read_metadata(dataset_dir: Path) -> Dict[str, str]:
    metadata_path = dataset_dir / "metadata.json"
    if metadata_path.exists():
        with open(metadata_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def is_valid_numeric(value: str) -> bool:
    if not value or not isinstance(value, str):
        return False
    value = value.strip()
    if "/" in value or (value.count("-") > 1):
        return False
    try:
        float(value)
        return True
    except ValueError:
        return False
    
def get_transport_type(route_type_code: str) -> str:
    route_type_map = {
        "0": "Tram", "1": "Metro", "2": "Rail", "3": "Bus", "4": "Ferry",
        "5": "Cable tram", "6": "Aerial lift", "7": "Funicular",
        "100": "Railway", "101": "High Speed Rail", "102": "Long Distance Train",
        "103": "Inter Regional Rail", "105": "Sleeper Rail", "106": "Regional Rail",
        "107": "Suburban Railway", "109": "Suburban Railway", "200": "Coach",
        "400": "Urban Railway", "401": "Metro", "402": "Underground",
        "700": "Bus", "900": "Tram", "1000": "Water Transport", "1500": "Taxi",
    }
    return route_type_map.get(str(route_type_code), f"Type {route_type_code}")

