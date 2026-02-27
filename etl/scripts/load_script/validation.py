

from typing import Any, Tuple, Dict
import pandas as pd


def validate_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    required = [
        "trip_id",
        "agency_name",
        "route_name",
        "origin_stop_name",
        "destination_stop_name"]
    for field in required:
        val = row.get(field)
        if val is None or str(val).strip() == "" or str(val) == "ERROR":
            return False, f"Missing or invalid {field}"
    try:
        if pd.notna(row.get("distance_km")):
            d = float(row["distance_km"])
            if d < 0 or d > 20_000:
                return False, f"distance_km out of range: {d}"
    except Exception:
        return False, "distance_km not numeric"
    return True, ""