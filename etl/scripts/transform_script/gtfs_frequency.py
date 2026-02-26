
import pandas as pd
from typing import Dict, Tuple

def build_frequency_map(trips: pd.DataFrame, first: pd.DataFrame, last: pd.DataFrame) -> Dict[Tuple[str, str, str, str], int]:
    freq_df = trips[["trip_id", "route_id", "service_id"]].copy()
    freq_df["origin_stop_id"] = freq_df["trip_id"].map(first["stop_id"])
    freq_df["destination_stop_id"] = freq_df["trip_id"].map(last["stop_id"])
    freq_df = freq_df.dropna(subset=["origin_stop_id", "destination_stop_id"])
    freq_df["key"] = list(zip(
        freq_df["route_id"].astype(str),
        freq_df["service_id"].astype(str),
        freq_df["origin_stop_id"].astype(str),
        freq_df["destination_stop_id"].astype(str),
    ))
    return freq_df.groupby("key").size().to_dict()

def compute_frequency(days_active: int, key: Tuple[str, str, str, str], freq_map: Dict[Tuple[str, str, str, str], int]) -> int:
    trips_per_day = max(1, freq_map.get(key, 1))
    trips_per_day = min(trips_per_day, 20)
    return days_active * trips_per_day

def calculate_frequency_per_week_intermediate(service_days_str: str, key: Tuple[str, str, str, str], freq_map: Dict[Tuple[str, str, str, str], int]) -> int:
    if not service_days_str or service_days_str == "Tous les jours":
        days_active = 7
    else:
        days_active = len(service_days_str.split(","))
    return compute_frequency(days_active, key, freq_map)