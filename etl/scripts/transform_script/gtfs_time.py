from typing import Optional
import pandas as pd

# ============================================================
# Time & classification
# ============================================================

def parse_gtfs_time_to_sec(t: str) -> Optional[int]:
    if not isinstance(t, str) or not t.strip():
        return None
    parts = t.strip().split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return None

def classifier_train(departure_time: str) -> str:
    try:
        h = int(departure_time.split(":")[0])
        return "NUIT" if h >= 22 or h < 6 else "JOUR"
    except Exception:
        return "INCONNU"


# ============================================================
# Core computations
# ============================================================

def compute_durations(stop_times: pd.DataFrame) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)
    st = stop_times[["trip_id", "arrival_time", "departure_time", "stop_sequence"]].copy()
    st["arr_sec"] = st["arrival_time"].apply(parse_gtfs_time_to_sec)
    st["dep_sec"] = st["departure_time"].apply(parse_gtfs_time_to_sec)
    st = st.sort_values(["trip_id", "stop_sequence"])
    g = st.groupby("trip_id")
    first_dep = g["dep_sec"].first()
    last_arr = g["arr_sec"].last()
    dur_sec = last_arr - first_dep

    def minmax_span(group: pd.DataFrame) -> float:
        vals = pd.concat([group["arr_sec"], group["dep_sec"]]).dropna()
        if len(vals) < 2:
            return 0.0
        return float(vals.max() - vals.min())

    alt = g.apply(minmax_span)
    dur_sec = dur_sec.where(dur_sec >= 0, alt).fillna(0)
    return (dur_sec / 60.0).round(2)