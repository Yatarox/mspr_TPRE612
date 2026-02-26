import os
from pathlib import Path
from typing import List, Dict
import pandas as pd
import numpy as np
import logging
import warnings
import gc
from concurrent.futures import ProcessPoolExecutor, as_completed
import sys
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from transform_script.gtfs_helpers import read_csv, read_metadata, latest_version_dir,log_memory
from transform_script.gtfs_frequency import build_frequency_map
from transform_script.gtfs_geo import compute_distances, build_stop_country_map
from transform_script.gtfs_processing import split_by_agency, _process_trips_chunk
from transform_script.gtfs_time import compute_durations


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