import math
from pathlib import Path
from typing import List, Dict, Optional
import pandas as pd
import numpy as np

# ============================================================
# Helpers généraux
# ============================================================

def latest_version_dir(dataset_dir: Path) -> Optional[Path]:
    versions = [p for p in dataset_dir.iterdir() if p.is_dir()]
    if not versions:
        return None
    versions.sort(key=lambda p: p.name)
    return versions[-1]

def read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path, dtype=str, encoding="utf-8", encoding_errors="replace")

# ============================================================
# Temps & classification
# ============================================================

def parse_gtfs_time_to_sec(t: str) -> Optional[int]:
    if not isinstance(t, str) or not t.strip():
        return None
    parts = t.strip().split(":")
    try:
        h = int(parts[0]); m = int(parts[1]) if len(parts) > 1 else 0; s = int(parts[2]) if len(parts) > 2 else 0
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
# Distance
# ============================================================

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1 = np.radians(lat1); lon1 = np.radians(lon1)
    lat2 = np.radians(lat2); lon2 = np.radians(lon2)
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))

# ============================================================
# Core
# ============================================================

def compute_durations(stop_times: pd.DataFrame) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)
    st = stop_times.copy()
    st["arr_sec"] = st["arrival_time"].apply(parse_gtfs_time_to_sec)
    st["dep_sec"] = st["departure_time"].apply(parse_gtfs_time_to_sec)
    st = st.sort_values(["trip_id", "stop_sequence"])
    g = st.groupby("trip_id")
    first_dep = g["dep_sec"].first()
    last_arr = g["arr_sec"].last()
    dur_sec = last_arr - first_dep

    def minmax_span(group: pd.DataFrame) -> float:
        vals = pd.concat([group["arr_sec"], group["dep_sec"]])
        vals = vals[pd.notna(vals)]
        if len(vals) < 2:
            return 0.0
        return float(vals.max() - vals.min())

    alt = g.apply(minmax_span)
    dur_sec = dur_sec.where(dur_sec >= 0, alt)
    return (dur_sec / 60.0).round(2)

def compute_distances(stop_times: pd.DataFrame, stops: pd.DataFrame) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)
    st = stop_times.copy()
    st["shape_dist_traveled"] = pd.to_numeric(st.get("shape_dist_traveled"), errors="coerce")
    st = st.sort_values(["trip_id", "stop_sequence"])
    # If shape_dist_traveled available
    if st["shape_dist_traveled"].notna().any():
        g = st.groupby("trip_id")["shape_dist_traveled"]
        raw = (g.max() - g.min())
        raw = raw.where(raw <= 1000, raw / 1000.0)
        return raw.fillna(0).round(3)
    # Else compute haversine segment-wise
    stops_idx = stops.set_index("stop_id")[["stop_lat", "stop_lon"]].apply(pd.to_numeric, errors="coerce")
    st = st.join(stops_idx, on="stop_id")
    st[["stop_lat", "stop_lon"]] = st[["stop_lat", "stop_lon"]].astype(float)
    st[["lat_prev", "lon_prev"]] = st.groupby("trip_id")[["stop_lat", "stop_lon"]].shift(1)
    seg = st.dropna(subset=["lat_prev", "lon_prev", "stop_lat", "stop_lon"])
    seg["seg_km"] = haversine_km(seg["lat_prev"], seg["lon_prev"], seg["stop_lat"], seg["stop_lon"])
    return seg.groupby("trip_id")["seg_km"].sum().round(3)

def build_trips_summary_for_dataset(staging_dir: str, dataset_id: str) -> List[Dict]:
    ds_dir = Path(staging_dir) / dataset_id
    latest = latest_version_dir(ds_dir)
    if not latest:
        return []

    agency_df = read_csv(latest / "agency.txt")
    routes_df = read_csv(latest / "routes.txt")
    trips_df = read_csv(latest / "trips.txt")
    stops_df = read_csv(latest / "stops.txt")
    stop_times_df = read_csv(latest / "stop_times.txt")

    if trips_df.empty or stop_times_df.empty:
        return []

    # clean / types
    stop_times_df["stop_sequence"] = pd.to_numeric(stop_times_df.get("stop_sequence"), errors="coerce").fillna(0).astype(int)
    stop_times_df["trip_id"] = stop_times_df["trip_id"].fillna("").astype(str)
    stop_times_df = stop_times_df[stop_times_df["trip_id"] != ""]

    durations_min = compute_durations(stop_times_df)
    distances_km = compute_distances(stop_times_df, stops_df)

    # prepare stops for names
    stops_name = stops_df.set_index("stop_id")["stop_name"].fillna("")

    # per trip first/last stop/time
    st_sorted = stop_times_df.sort_values(["trip_id", "stop_sequence"])
    first = st_sorted.groupby("trip_id").first()
    last = st_sorted.groupby("trip_id").last()

    trips = trips_df.copy()
    trips["trip_id"] = trips["trip_id"].fillna("")
    trips = trips[trips["trip_id"] != ""]

    # join route + agency
    routes_df = routes_df.rename(columns={"agency_id": "route_agency_id"})
    trips = trips.merge(routes_df, on="route_id", how="left")
    trips = trips.merge(agency_df.rename(columns={"agency_id": "route_agency_id"}), on="route_agency_id", how="left", suffixes=("", "_agency"))

    rows: List[Dict] = []
    for _, tr in trips.iterrows():
        tid = tr["trip_id"]
        if tid not in first.index or tid not in last.index:
            continue
        dep = str(first.loc[tid].get("departure_time") or "")
        arr = str(last.loc[tid].get("arrival_time") or "")
        rows.append({
            "dataset_id": dataset_id,
            "trip_id": tid,
            "route_id": tr.get("route_id", "") or "",
            "route_name": _route_title(tr),
            "agency_id": tr.get("route_agency_id", "") or "",
            "agency_name": tr.get("agency_name", "") or "",
            "service_type": classifier_train(dep) if dep else "INCONNU",
            "origin_stop_name": stops_name.get(first.loc[tid].get("stop_id"), ""),
            "destination_stop_name": stops_name.get(last.loc[tid].get("stop_id"), ""),
            "departure_time": dep,
            "arrival_time": arr,
            "distance_km": float(distances_km.get(tid, 0.0)),
            "duration_h": round(float(durations_min.get(tid, 0.0)) / 60.0, 2),
        })
    return rows

def _route_title(route_row: Dict) -> str:
    short = str(route_row.get("route_short_name") or "").strip()
    long = str(route_row.get("route_long_name") or "").strip()
    if short and long:
        return f"{short} - {long}"
    return long or short or ""

# ============================================================
# Entrée principale
# ============================================================

def _write_csv(rows: List[Dict], out_csv: Path) -> None:
    if not rows:
        return
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_csv, index=False, encoding="utf-8")

def transform_gtfs(staging_dir: str, processed_dir: str) -> List[str]:
    written: List[str] = []
    staging = Path(staging_dir)
    for ds in [p for p in staging.iterdir() if p.is_dir()]:
        rows = build_trips_summary_for_dataset(staging_dir, ds.name)
        if not rows:
            continue
        out_csv = Path(processed_dir) / ds.name / "trips_summary.csv"
        _write_csv(rows, out_csv)
        written.append(str(out_csv))
    return written