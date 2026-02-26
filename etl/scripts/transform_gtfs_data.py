from pathlib import Path
from typing import List, Dict, Optional, Tuple
import pandas as pd
import numpy as np
import json
import logging
import warnings
import gc
import psutil
from concurrent.futures import ProcessPoolExecutor, as_completed

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ORDERED_COLUMNS = [
    "trip_id",
    "agency_name",
    "route_name",
    "train_type",
    "service_type",
    "origin_stop_name",
    "origin_country",
    "destination_stop_name",
    "destination_country",
    "departure_time",
    "arrival_time",
    "distance_km",
    "duration_h",
    "emission_gco2e_pkm",
    "total_emission_kgco2e",
    "frequency_per_week",
    "source_dataset",
    "traction",
]

# ============================================================
# Helpers
# ============================================================

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

# ============================================================
# Country detection from GPS coordinates
# ============================================================

def build_stop_country_map(stops_df: pd.DataFrame) -> Dict[str, Optional[str]]:
    if stops_df.empty or "stop_lat" not in stops_df.columns or "stop_lon" not in stops_df.columns:
        logger.warning("No lat/lon in stops.txt, returning empty country map")
        return {}
    country_boxes = {
        "FR": (41.0, 51.5, -5.5, 10.0),
        "DE": (47.0, 55.5, 5.5, 15.5),
        "IT": (36.0, 47.5, 6.5, 19.0),
        "ES": (35.5, 44.0, -10.0, 5.0),
        "CH": (45.5, 48.0, 5.5, 11.0),
        "BE": (49.5, 51.5, 2.5, 6.5),
        "NL": (50.5, 54.0, 3.0, 7.5),
        "AT": (46.0, 49.5, 9.0, 17.5),
        "LU": (49.4, 50.2, 5.7, 6.6),
        "GB": (49.5, 61.0, -8.5, 2.0),
        "IE": (51.0, 55.5, -11.0, -5.5),
        "PT": (36.5, 42.5, -10.0, -6.0),
        "PL": (49.0, 55.0, 14.0, 24.5),
        "CZ": (48.5, 51.0, 12.0, 19.0),
        "SK": (47.5, 49.5, 16.5, 22.5),
        "HU": (45.5, 48.5, 16.0, 23.0),
        "RO": (43.5, 48.5, 20.0, 30.0),
        "BG": (41.0, 44.5, 22.0, 29.0),
        "GR": (34.5, 42.0, 19.0, 29.0),
        "HR": (42.0, 46.5, 13.0, 20.0),
        "SI": (45.0, 47.0, 13.0, 17.0),
        "SE": (55.0, 69.5, 10.5, 24.5),
        "NO": (57.5, 71.5, 4.0, 31.5),
        "DK": (54.5, 58.0, 8.0, 15.5),
        "FI": (59.5, 70.5, 19.0, 32.0),
    }
    stop_country_map = {}
    stops_clean = stops_df.copy()
    stops_clean["stop_lat"] = pd.to_numeric(stops_clean["stop_lat"], errors="coerce")
    stops_clean["stop_lon"] = pd.to_numeric(stops_clean["stop_lon"], errors="coerce")
    stops_clean = stops_clean.dropna(subset=["stop_lat", "stop_lon", "stop_id"])
    logger.info(f"🌍 Building country map for {len(stops_clean)} stops with valid coordinates...")
    for _, stop in stops_clean.iterrows():
        stop_id = str(stop["stop_id"])
        lat = float(stop["stop_lat"])
        lon = float(stop["stop_lon"])
        matches = []
        for country, (lat_min, lat_max, lon_min, lon_max) in country_boxes.items():
            if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
                matches.append(country)
        if len(matches) == 1:
            stop_country_map[stop_id] = matches[0]
        elif len(matches) > 1:
            stop_country_map[stop_id] = matches[0]
            logger.debug(f"Stop {stop_id} at ({lat:.4f}, {lon:.4f}) in multiple countries {matches}, using {matches[0]}")
        else:
            stop_country_map[stop_id] = None
            logger.debug(f"Stop {stop_id} at ({lat:.4f}, {lon:.4f}) outside known countries")
    found_count = sum(1 for v in stop_country_map.values() if v is not None)
    logger.info(f"✓ Country map built: {found_count}/{len(stops_clean)} stops mapped ({found_count/len(stops_clean)*100:.1f}%)")
    country_counts = {}
    for country in stop_country_map.values():
        if country:
            country_counts[country] = country_counts.get(country, 0) + 1
    logger.info(f"📊 Stops per country: {dict(sorted(country_counts.items(), key=lambda x: x[1], reverse=True)[:10])}")
    return stop_country_map

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
# Distance
# ============================================================

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1 = np.radians(lat1)
    lon1 = np.radians(lon1)
    lat2 = np.radians(lat2)
    lon2 = np.radians(lon2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
    return 2 * R * np.arcsin(np.sqrt(a))

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

def compute_distances(stop_times: pd.DataFrame, stops: pd.DataFrame) -> pd.Series:
    if stop_times.empty:
        return pd.Series(dtype=float)
    st = stop_times[["trip_id", "stop_id", "stop_sequence"]].copy()

    if "shape_dist_traveled" in stop_times.columns:
        st["shape_dist_traveled"] = pd.to_numeric(stop_times["shape_dist_traveled"], errors="coerce")
        st = st.sort_values(["trip_id", "stop_sequence"])
        if st["shape_dist_traveled"].notna().any():
            g = st.groupby("trip_id")["shape_dist_traveled"]
            raw = (g.max() - g.min())
            raw = raw.where(raw <= 1000, raw / 1000.0)
            return raw.fillna(0).round(3)

    st = st.sort_values(["trip_id", "stop_sequence"])
    if "stop_lat" not in stops.columns or "stop_lon" not in stops.columns:
        logger.warning("stop_lat ou stop_lon not found in stops.txt, returning 0 distances")
        return pd.Series(dtype=float)

    stops_idx = stops[["stop_id", "stop_lat", "stop_lon"]].copy()
    stops_idx["stop_lat"] = pd.to_numeric(stops_idx["stop_lat"], errors="coerce")
    stops_idx["stop_lon"] = pd.to_numeric(stops_idx["stop_lon"], errors="coerce")
    stops_idx = stops_idx.set_index("stop_id")

    st = st.join(stops_idx, on="stop_id", how="left")
    st.loc[:, "lat_prev"] = st.groupby("trip_id")["stop_lat"].shift(1)
    st.loc[:, "lon_prev"] = st.groupby("trip_id")["stop_lon"].shift(1)

    seg = st.dropna(subset=["lat_prev", "lon_prev", "stop_lat", "stop_lon"])
    if seg.empty:
        logger.warning("No valid segments for distance calculation")
        return pd.Series(dtype=float)

    seg = seg.copy()
    seg["seg_km"] = haversine_km(
        seg["lat_prev"].astype(float),
        seg["lon_prev"].astype(float),
        seg["stop_lat"].astype(float),
        seg["stop_lon"].astype(float),
    )
    result = seg.groupby("trip_id")["seg_km"].sum().round(3)
    return result.fillna(0)

def classify_train_service(route_type: str, route_name: str, agency_name: str, distance_km: float, duration_h: float) -> str:
    route_type_map = {
        "101": "Grande vitesse",
        "102": "Intercité",
        "103": "Inter-régional",
        "106": "Régional",
        "107": "Suburban",
        "2": "Rail",
    }
    if route_type in route_type_map:
        return route_type_map[route_type]
    route_upper = route_name.upper()
    agency_upper = agency_name.upper()
    if any(x in route_upper or x in agency_upper for x in ["TGV", "ICE", "AVE", "EUROSTAR", "THALYS", "FRECCIAROSSA"]):
        return "Grande vitesse"
    if any(x in route_upper or x in agency_upper for x in ["INTERCITÉ", "INTERCITY", "IC ", "INTER CITY"]):
        return "Intercité"
    if any(x in route_upper or x in agency_upper for x in ["TER", "REGIONAL", "REGIO", "RE ", "RB "]):
        return "Régional"
    if any(x in route_upper or x in agency_upper for x in ["EN ", "NJ ", "NIGHTJET", "INTERNATIONAL"]):
        return "International"
    if distance_km > 800 or duration_h > 6:
        return "Grande ligne"
    elif distance_km > 200 or duration_h > 2:
        return "Intercité"
    elif distance_km > 0:
        return "Régional"
    return "Inconnu"

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

def extract_country_from_stop_name(stop_name: str) -> Optional[str]:
    stop_upper = stop_name.upper()
    country_patterns = {
        "FR": ["PARIS", "LYON", "MARSEILLE", "LILLE", "STRASBOURG", "BORDEAUX", "NANTES", "TOULOUSE", "NICE", "MONTPELLIER"],
        "DE": ["BERLIN", "MÜNCHEN", "MUNICH", "FRANKFURT", "HAMBURG", "KÖLN", "COLOGNE", "STUTTGART"],
        "IT": ["ROMA", "ROME", "MILANO", "MILAN", "VENEZIA", "VENICE", "FIRENZE", "FLORENCE", "NAPOLI"],
        "ES": ["MADRID", "BARCELONA", "VALENCIA", "SEVILLA", "SEVILLE", "ZARAGOZA"],
        "CH": ["ZÜRICH", "ZURICH", "GENÈVE", "GENEVA", "BERN", "BERNE", "BASEL"],
        "BE": ["BRUXELLES", "BRUSSELS", "BRUSSEL", "ANTWERPEN", "ANVERS", "GENT"],
        "NL": ["AMSTERDAM", "ROTTERDAM", "UTRECHT", "EINDHOVEN", "TILBURG"],
        "AT": ["WIEN", "VIENNA", "GRAZ", "LINZ", "SALZBURG"],
        "GB": ["LONDON", "MANCHESTER", "BIRMINGHAM", "LEEDS", "GLASGOW"],
        "CZ": ["PRAHA", "PRAGUE", "BRNO", "OSTRAVA"],
        "PL": ["WARSZAWA", "WARSAW", "KRAKÓW", "CRACOW", "ŁÓDŹ"],
        "LU": ["LUXEMBOURG"],
        "PT": ["LISBOA", "LISBON", "PORTO"],
    }
    for country, cities in country_patterns.items():
        if any(city in stop_upper for city in cities):
            return country
    return None

def estimate_traction(route_type: str, route_name: str, agency_name: str, train_service: str) -> str:
    route_upper = route_name.upper()
    agency_upper = agency_name.upper()
    electric_keywords = ["TGV", "ICE", "EUROSTAR", "THALYS", "AVE", "FRECCIAROSSA", "TER", "INTERCITÉ"]
    if any(kw in route_upper or kw in agency_upper for kw in electric_keywords):
        return "électrique"
    if train_service in ["Grande vitesse", "Intercité"]:
        return "électrique"
    diesel_keywords = ["DIESEL", "AUTORAIL"]
    if any(kw in route_upper or kw in agency_upper for kw in diesel_keywords):
        return "diesel"
    if train_service == "Régional":
        return "électrique"
    return "mixte"

def calculate_emissions(distance_km: float, traction: str, train_service: str) -> tuple:
    emission_factors = {
        ("Grande vitesse", "électrique"): 3.2,
        ("Intercité", "électrique"): 8.1,
        ("Régional", "électrique"): 29.9,
        ("Inter-régional", "électrique"): 20.0,
        ("International", "électrique"): 5.0,
        ("Grande ligne", "électrique"): 6.0,
        ("Grande vitesse", "diesel"): 15.0,
        ("Intercité", "diesel"): 35.0,
        ("Régional", "diesel"): 45.0,
        ("Inter-régional", "diesel"): 40.0,
    }
    key = (train_service, traction)
    emission_gco2e_pkm = emission_factors.get(key, 25.0)
    if traction == "mixte":
        elec_key = (train_service, "électrique")
        diesel_key = (train_service, "diesel")
        emission_gco2e_pkm = (emission_factors.get(elec_key, 20.0) + emission_factors.get(diesel_key, 40.0)) / 2
    total_emission_kgco2e = round((emission_gco2e_pkm * distance_km) / 1000, 3)
    return emission_gco2e_pkm, total_emission_kgco2e

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

# ============================================================
# Frequency helpers
# ============================================================

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

# ============================================================
# Processing
# ============================================================

def _route_title(route_row: Dict, origin_stop_name: str, destination_stop_name: str) -> str:
    short = str(route_row.get("route_short_name") or "").strip()
    long = str(route_row.get("route_long_name") or "").strip()
    if (
        (short.startswith("---/---") and long.startswith("---/---"))
        or (short == "---/---" and long == "---/---")
        or (short == "" and long.startswith("---/---"))
        or (long == "" and short.startswith("---/---"))
        or (not short and not long)
    ):
        return f"{origin_stop_name} - {destination_stop_name}"
    if short and long:
        return f"{short}"
    return long or short or "ERROR"

def split_by_agency(trips: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    if "agency_id" not in trips.columns:
        return {"all": trips}
    split = {}
    for agency_id in trips["agency_id"].unique():
        if pd.isna(agency_id) or agency_id == "":
            continue
        subset = trips[trips["agency_id"] == agency_id]
        if not subset.empty:
            split[str(agency_id)] = subset
    return split if split else {"all": trips}

def _process_trips_chunk(trips_chunk: pd.DataFrame, first, last, stops_name, 
                         stop_country_map: Dict[str, Optional[str]],
                         distances_km, durations_min, dataset_id_meta: str, processed_dir: str,
                         freq_map: Dict[Tuple[str, str, str, str], int],
                         all_rows: List[Dict]) -> int:
    skipped_invalid = 0

    for idx, tr in enumerate(trips_chunk.iterrows()):
        _, tr = tr
        tid = str(tr.get("trip_id", ""))
        if not tid or tid not in first.index or tid not in last.index:
            continue

        dep = str(first.loc[tid].get("departure_time") or "")
        arr = str(last.loc[tid].get("arrival_time") or "")
        agency_name = str(tr.get("agency_name", "ERROR") or "ERROR")

        dist = float(distances_km.get(tid, 0.0)) if tid in distances_km.index else 0.0
        dur = float(durations_min.get(tid, 0.0) or 0.0) / 60.0 if tid in durations_min.index else 0.0

        route_type_code = str(tr.get("route_type", ""))
        origin_stop_id = first.loc[tid].get("stop_id")
        destination_stop_id = last.loc[tid].get("stop_id")
        origin_stop_name = stops_name.get(origin_stop_id, "ERROR")
        destination_stop_name = stops_name.get(destination_stop_id, "ERROR")

        # Utilise la nouvelle version de _route_title
        route_name = _route_title(tr, origin_stop_name, destination_stop_name)

        train_service = classify_train_service(
            route_type_code, route_name, agency_name, dist, dur
        )

        service_days = []
        for day, abbr in [
            ("monday", "Mon"), ("tuesday", "Tue"), ("wednesday", "Wed"),
            ("thursday", "Thu"), ("friday", "Fri"), ("saturday", "Sat"), ("sunday", "Sun")
        ]:
            if str(tr.get(day, "0")) == "1":
                service_days.append(abbr)
        service_days_str = ",".join(service_days) if service_days else "Tous les jours"

        origin_country = stop_country_map.get(str(origin_stop_id))
        destination_country = stop_country_map.get(str(destination_stop_id))

        if origin_country is None:
            origin_country = extract_country_from_stop_name(origin_stop_name)
        if destination_country is None:
            destination_country = extract_country_from_stop_name(destination_stop_name)

        freq_key = (
            str(tr.get("route_id", "")),
            str(tr.get("service_id", "")),
            str(origin_stop_id),
            str(destination_stop_id),
        )
        frequency_per_week = calculate_frequency_per_week_intermediate(
            service_days_str, freq_key, freq_map
        )

        traction = estimate_traction(
            route_type_code, route_name, agency_name, train_service
        )
        emission_gco2e_pkm, total_emission_kgco2e = calculate_emissions(
            dist, traction, train_service
        )

        if not is_valid_numeric(str(emission_gco2e_pkm)):
            logger.warning(
                f"⚠️ Dataset {dataset_id_meta} Trip {tid}: emission_gco2e_pkm invalide : "
                f"'{emission_gco2e_pkm}' → skipped"
            )
            skipped_invalid += 1
            continue

        all_rows.append({
            "trip_id": tid,
            "agency_name": agency_name,
            "route_name": route_name,
            "train_type": train_service,
            "service_type": classifier_train(dep) if dep else "INCONNU",
            "origin_stop_name": origin_stop_name,
            "origin_country": origin_country,
            "destination_stop_name": destination_stop_name,
            "destination_country": destination_country,
            "departure_time": dep,
            "arrival_time": arr,
            "distance_km": round(dist, 3),
            "duration_h": round(dur, 2),
            "emission_gco2e_pkm": round(emission_gco2e_pkm, 2),
            "total_emission_kgco2e": round(total_emission_kgco2e, 3),
            "frequency_per_week": frequency_per_week,
            "source_dataset": dataset_id_meta,
            "traction": traction,
        })

    return len(all_rows) - skipped_invalid

# ============================================================
# Build dataset
# ============================================================

def build_trips_summary_for_dataset(staging_dir: str, dataset_id: str, processed_dir: str) -> int:
    ds_dir = Path(staging_dir) / dataset_id
    latest = latest_version_dir(ds_dir)
    if not latest:
        logger.error(f"ERROR: No version directory found for dataset {dataset_id}")
        return 0

    metadata = read_metadata(latest)
    dataset_id_meta = str(metadata.get("dataset_id", dataset_id))

    out_csv = Path(processed_dir) / dataset_id_meta / f"trips_summary_{dataset_id_meta}.csv"
    if out_csv.exists():
        out_csv.unlink()

    agency_df = read_csv(latest / "agency.txt")
    routes_df = read_csv(latest / "routes.txt")
    stops_df = read_csv(latest / "stops.txt")
    calendar_df = read_csv(latest / "calendar.txt")
    trips_df = read_csv(latest / "trips.txt")
    stop_times_df = read_csv(latest / "stop_times.txt")

    logger.info(f"Processing dataset: {dataset_id}")
    logger.info(f"Agency rows: {len(agency_df)}, Routes rows: {len(routes_df)}, Trips rows: {len(trips_df)}")

    if trips_df.empty or stop_times_df.empty:
        logger.error(f"ERROR: Empty trips or stop_times for dataset {dataset_id}")
        return 0

    stop_country_map = build_stop_country_map(stops_df)

    if len(trips_df) > 1_000_000:
        logger.warning(f"⚠️ Large dataset ({len(trips_df)} trips), skipping distance/duration calculation")
        distances_km = pd.Series(dtype=float)
        durations_min = pd.Series(dtype=float)
    else:
        stop_times_df["stop_sequence"] = pd.to_numeric(stop_times_df.get("stop_sequence"), errors="coerce").fillna(0).astype(int)
        stop_times_df["trip_id"] = stop_times_df["trip_id"].fillna("").astype(str)
        stop_times_df = stop_times_df[stop_times_df["trip_id"] != ""]
        logger.info("Computing durations and distances...")
        distances_km = compute_distances(stop_times_df, stops_df)
        durations_min = compute_durations(stop_times_df)
        log_memory("After distance/duration - ")
        del stop_times_df
        gc.collect()

    st_for_times = read_csv(latest / "stop_times.txt")
    st_for_times["stop_sequence"] = pd.to_numeric(st_for_times.get("stop_sequence"), errors="coerce").fillna(0).astype(int)
    st_for_times["trip_id"] = st_for_times["trip_id"].fillna("").astype(str)
    st_for_times = st_for_times[st_for_times["trip_id"] != ""]
    st_for_times = st_for_times.sort_values(["trip_id", "stop_sequence"])
    first = st_for_times.groupby("trip_id").first()
    last = st_for_times.groupby("trip_id").last()
    del st_for_times
    gc.collect()

    stops_name = stops_df.set_index("stop_id")["stop_name"].fillna("")
    del stops_df
    gc.collect()

    trips = trips_df.copy()
    trips["trip_id"] = trips["trip_id"].fillna("").astype(str)
    trips = trips[trips["trip_id"] != ""]
    del trips_df
    gc.collect()

    trips = trips.merge(routes_df, on="route_id", how="left", suffixes=("", "_route"))
    trips["route_agency_id"] = trips.get("agency_id", "ERROR")
    del routes_df
    gc.collect()

    if not agency_df.empty and "agency_id" in agency_df.columns:
        trips = trips.merge(
            agency_df[["agency_id", "agency_name"]],
            left_on="route_agency_id",
            right_on="agency_id",
            how="left",
            suffixes=("", "_agency"),
        )
    else:
        trips["agency_name"] = "ERROR"
    del agency_df
    gc.collect()

    if not calendar_df.empty and "service_id" in calendar_df.columns:
        trips = trips.merge(calendar_df, on="service_id", how="left", suffixes=("", "_cal"))
    del calendar_df
    gc.collect()

    freq_map = build_frequency_map(trips, first, last)
    log_memory("After freq_map - ")

    all_rows: List[Dict] = []

    if len(trips) > 500_000:
        logger.info("Large dataset, splitting by agency...")
        agencies_split = split_by_agency(trips)
        for agency_id, trips_chunk in agencies_split.items():
            logger.info(f"Processing agency {agency_id} with {len(trips_chunk)} trips...")
            _process_trips_chunk(
                trips_chunk, first, last, stops_name, stop_country_map,
                distances_km, durations_min,
                dataset_id_meta, processed_dir, freq_map, all_rows
            )
            del trips_chunk
            gc.collect()
    else:
        CHUNK_SIZE = 5000
        for chunk_start in range(0, len(trips), CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, len(trips))
            chunk = trips.iloc[chunk_start:chunk_end].copy()
            logger.info(f"Processing chunk {chunk_start}-{chunk_end} of {len(trips)}")
            _process_trips_chunk(
                chunk, first, last, stops_name, stop_country_map,
                distances_km, durations_min,
                dataset_id_meta, processed_dir, freq_map, all_rows
            )
            del chunk
            gc.collect()

    if all_rows:
        _write_csv(all_rows, out_csv)
        logger.info(f"✓ Generated {len(all_rows)} trip summaries for dataset {dataset_id}")
        return len(all_rows)
    
    logger.warning(f"No valid rows for dataset {dataset_id}")
    return 0

# ============================================================
# IO
# ============================================================

def _write_csv(rows: List[Dict], out_csv: Path) -> None:
    if not rows:
        logger.warning("No rows to write")
        return
    
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    
    logger.info(f"📋 Writing {len(df)} rows to {out_csv}")
    
    for col in ORDERED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan
    
    df = df[ORDERED_COLUMNS]
    df.to_csv(out_csv, mode='w', header=True, index=False, encoding="utf-8")
    logger.info(f"✓ Written {len(df)} rows to {out_csv}")

# ============================================================
# Entrée principale
# ============================================================

def transform_gtfs(staging_dir: str, processed_dir: str, max_workers: int = 1) -> List[str]:
    written: List[str] = []
    staging = Path(staging_dir)
    if not staging.exists():
        logger.error(f"Staging directory not found: {staging}")
        return written

    datasets = sorted([p for p in staging.iterdir() if p.is_dir()])
    if not datasets:
        logger.warning("No datasets to transform")
        return written

    if max_workers > 1 and len(datasets) > 1:
        logger.info(f"Parallel transform with {min(max_workers, len(datasets))} workers")
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(build_trips_summary_for_dataset, staging_dir, ds.name, processed_dir): ds.name
                for ds in datasets
            }
            for future in as_completed(futures):
                ds_name = futures[future]
                try:
                    count = future.result()
                    if count > 0:
                        out_csv = Path(processed_dir) / ds_name / f"trips_summary_{ds_name}.csv"
                        written.append(str(out_csv))
                    logger.info(f"✓ Dataset {ds_name} completed with {count} rows")
                except Exception as e:
                    logger.error(f"Error processing dataset {ds_name}: {e}", exc_info=True)
    else:
        for ds in datasets:
            try:
                logger.info(f"🚀 Starting transform for dataset {ds.name}")
                count = build_trips_summary_for_dataset(staging_dir, ds.name, processed_dir)
                if count > 0:
                    out_csv = Path(processed_dir) / ds.name / f"trips_summary_{ds.name}.csv"
                    written.append(str(out_csv))
                gc.collect()
            except Exception as e:
                logger.error(f"Error processing dataset {ds.name}: {e}", exc_info=True)
                continue

    logger.info(f"✓ Transform completed - {len(written)} files written")
    return written