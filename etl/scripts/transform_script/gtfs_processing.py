from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
from .gtfs_helpers import is_valid_numeric
from .gtfs_emission import calculate_emissions, estimate_traction
from .gtfs_frequency import calculate_frequency_per_week_intermediate
from .gtfs_geo import extract_country_from_stop_name
from .gtfs_time import classifier_train
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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


def _process_trips_chunk(
    trips_chunk: pd.DataFrame,
    first: pd.DataFrame,
    last: pd.DataFrame,
    stops_name: pd.Series,
    stop_country_map: Dict[str, Optional[str]],
    distances_km: pd.Series,
    durations_min: pd.Series,
    dataset_id_meta: str,
    processed_dir: str,
    freq_map: Dict[Tuple[str, str, str, str], int],
    all_rows: List[Dict],
) -> int:
    """
    Traitement vectorisé du chunk de trips.
    On évite iterrows() en travaillant colonne par colonne sur le DataFrame entier.
    """
    chunk = trips_chunk.copy()
    chunk["trip_id"] = chunk["trip_id"].astype(str)

    # Filtrer les trips sans first/last
    valid_mask = chunk["trip_id"].isin(first.index) & chunk["trip_id"].isin(last.index)
    chunk = chunk[valid_mask].copy()
    if chunk.empty:
        return 0

    tids = chunk["trip_id"].values

    # --- Stops origine / destination ---
    origin_stop_ids = first.loc[tids, "stop_id"].values
    destination_stop_ids = last.loc[tids, "stop_id"].values

    origin_stop_names = np.array([stops_name.get(sid, "ERROR") for sid in origin_stop_ids])
    destination_stop_names = np.array([stops_name.get(sid, "ERROR") for sid in destination_stop_ids])

    # --- Distances et durées ---
    dists = np.array([
        float(distances_km.get(tid, 0.0)) if tid in distances_km.index else 0.0
        for tid in tids
    ])
    durs = np.array([
        float(durations_min.get(tid, 0.0) or 0.0) / 60.0 if tid in durations_min.index else 0.0
        for tid in tids
    ])

    # --- Départ / arrivée ---
    dep_times = first.loc[tids, "departure_time"].fillna("").astype(str).values
    arr_times = last.loc[tids, "arrival_time"].fillna("").astype(str).values

    # --- Route names ---
    route_type_codes = chunk.get("route_type", pd.Series([""] * len(chunk))).fillna("").astype(str).values
    agency_names = chunk.get("agency_name", pd.Series(["ERROR"] * len(chunk))).fillna("ERROR").astype(str).values

    route_names = np.array([
        _route_title(row, orig, dest)
        for row, orig, dest in zip(chunk.to_dict("records"), origin_stop_names, destination_stop_names)
    ])

    # --- Service days ---
    day_cols = [("monday", "Mon"), ("tuesday", "Tue"), ("wednesday", "Wed"),
                ("thursday", "Thu"), ("friday", "Fri"), ("saturday", "Sat"), ("sunday", "Sun")]

    def _service_days_str(row: Dict) -> str:
        days = [abbr for col, abbr in day_cols if str(row.get(col, "0")) == "1"]
        return ",".join(days) if days else "Tous les jours"

    service_days_strs = [_service_days_str(row) for row in chunk.to_dict("records")]

    # --- Pays ---
    origin_countries = [
        stop_country_map.get(str(sid)) or extract_country_from_stop_name(name)
        for sid, name in zip(origin_stop_ids, origin_stop_names)
    ]
    destination_countries = [
        stop_country_map.get(str(sid)) or extract_country_from_stop_name(name)
        for sid, name in zip(destination_stop_ids, destination_stop_names)
    ]

    # --- Train service & traction & emissions ---
    route_ids = chunk.get("route_id", pd.Series([""] * len(chunk))).fillna("").astype(str).values
    service_ids = chunk.get("service_id", pd.Series([""] * len(chunk))).fillna("").astype(str).values

    skipped_invalid = 0
    initial_len = len(all_rows)

    for i in range(len(chunk)):
        tid = tids[i]
        dist = dists[i]
        dur = durs[i]
        route_type_code = route_type_codes[i]
        route_name = route_names[i]
        agency_name = agency_names[i]

        train_service = classify_train_service(route_type_code, route_name, agency_name, dist, dur)
        traction = estimate_traction(route_type_code, route_name, agency_name, train_service)
        emission_gco2e_pkm, total_emission_kgco2e = calculate_emissions(dist, traction, train_service)

        if not is_valid_numeric(str(emission_gco2e_pkm)):
            logger.warning(
                f"⚠️ Dataset {dataset_id_meta} Trip {tid}: emission_gco2e_pkm invalide : "
                f"'{emission_gco2e_pkm}' → skipped"
            )
            skipped_invalid += 1
            continue

        freq_key = (
            route_ids[i],
            service_ids[i],
            str(origin_stop_ids[i]),
            str(destination_stop_ids[i]),
        )
        frequency_per_week = calculate_frequency_per_week_intermediate(
            service_days_strs[i], freq_key, freq_map
        )

        dep = dep_times[i]
        all_rows.append({
            "trip_id": tid,
            "agency_name": agency_name,
            "route_name": route_name,
            "train_type": train_service,
            "service_type": classifier_train(dep) if dep else "INCONNU",
            "origin_stop_name": origin_stop_names[i],
            "origin_country": origin_countries[i],
            "destination_stop_name": destination_stop_names[i],
            "destination_country": destination_countries[i],
            "departure_time": dep,
            "arrival_time": arr_times[i],
            "distance_km": round(dist, 3),
            "duration_h": round(dur, 2),
            "emission_gco2e_pkm": round(emission_gco2e_pkm, 2),
            "total_emission_kgco2e": round(total_emission_kgco2e, 3),
            "frequency_per_week": frequency_per_week,
            "source_dataset": dataset_id_meta,
            "traction": traction,
        })

    return len(all_rows) - initial_len - skipped_invalid


def classify_train_service(
    route_type: str, route_name: str, agency_name: str, distance_km: float, duration_h: float
) -> str:
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
