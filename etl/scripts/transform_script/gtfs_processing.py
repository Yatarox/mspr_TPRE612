from typing import Dict, List, Optional, Tuple
import pandas as pd
from pathlib import Path
from .gtfs_helpers import is_valid_numeric
from .gtfs_emission import calculate_emissions, estimate_traction
from .gtfs_frequency import calculate_frequency_per_week_intermediate
from .gtfs_geo import extract_country_from_stop_name
from .gtfs_time import classifier_train
import logging  

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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