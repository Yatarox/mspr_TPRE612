import os
import sys
import gc
import logging
import warnings
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed, TimeoutError as FuturesTimeoutError
from concurrent.futures.process import BrokenProcessPool

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from transform_script.gtfs_helpers import read_csv, read_metadata, latest_version_dir, log_memory
from transform_script.gtfs_frequency import build_frequency_map
from transform_script.gtfs_geo import compute_distances, build_stop_country_map
from transform_script.gtfs_processing import split_by_agency, _process_trips_chunk
from transform_script.gtfs_time import compute_durations

warnings.filterwarnings("ignore", category=pd.errors.SettingWithCopyWarning)

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


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _empty_trip_frame() -> pd.DataFrame:
    return pd.DataFrame()


def _prepare_stop_times_df(stop_times_df: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    stop_times_df = _normalize_columns(stop_times_df).copy()

    if "trip_id" not in stop_times_df.columns:
        logger.warning(f"⚠️ [{dataset_id}] stop_times.txt missing trip_id")
        return pd.DataFrame()

    if "stop_sequence" not in stop_times_df.columns:
        stop_times_df["stop_sequence"] = 0

    stop_times_df["stop_sequence"] = pd.to_numeric(
        stop_times_df["stop_sequence"], errors="coerce"
    ).fillna(0).astype(int)

    stop_times_df["trip_id"] = stop_times_df["trip_id"].fillna("").astype(str).str.strip()
    stop_times_df = stop_times_df[stop_times_df["trip_id"] != ""].copy()

    if stop_times_df.empty:
        return stop_times_df

    stop_times_df = stop_times_df.sort_values(["trip_id", "stop_sequence"])

    if "shape_dist_traveled" in stop_times_df.columns:
        stop_times_df["shape_dist_traveled"] = pd.to_numeric(
            stop_times_df["shape_dist_traveled"], errors="coerce"
        )
        stop_times_df["_prev_dist"] = stop_times_df.groupby("trip_id")["shape_dist_traveled"].shift(1)

        bad_mask = (
            stop_times_df["shape_dist_traveled"].notna()
            & stop_times_df["_prev_dist"].notna()
            & (stop_times_df["shape_dist_traveled"] < stop_times_df["_prev_dist"])
        )
        if bad_mask.any():
            logger.warning(
                f"⚠️ [{dataset_id}] Dropping {bad_mask.sum()} stop_times rows "
                f"with strictly decreasing shape_dist_traveled"
            )
            stop_times_df = stop_times_df.loc[~bad_mask].copy()

        stop_times_df = stop_times_df.drop(columns=["_prev_dist"], errors="ignore")

    return stop_times_df


def _sanitize_dataframe(df: pd.DataFrame, dataset_id: str) -> pd.DataFrame:
    initial_count = len(df)
    df = df.copy()

    if "trip_id" in df.columns:
        mask = df["trip_id"].isna() | (df["trip_id"].astype(str).str.strip() == "")
        if mask.any():
            logger.warning(f"⚠️ [{dataset_id}] Dropping {mask.sum()} rows with missing trip_id")
            df = df.loc[~mask].copy()

    if "duration_h" in df.columns:
        df["duration_h"] = pd.to_numeric(df["duration_h"], errors="coerce")
        mask = df["duration_h"].isna() | (df["duration_h"] <= 0)
        if mask.any():
            logger.warning(f"⚠️ [{dataset_id}] Dropping {mask.sum()} rows with invalid duration (<=0 or NaN)")
            df = df.loc[~mask].copy()

    if "distance_km" in df.columns:
        df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
        mask = df["distance_km"].isna() | (df["distance_km"] < 0)
        if mask.any():
            logger.warning(f"⚠️ [{dataset_id}] Dropping {mask.sum()} rows with invalid distance (<0 or NaN)")
            df = df.loc[~mask].copy()

    for col in ["emission_gco2e_pkm", "total_emission_kgco2e"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            neg = df[col] < 0
            if neg.any():
                logger.warning(f"⚠️ [{dataset_id}] Setting {neg.sum()} negative values in {col} to NaN")
                df.loc[neg, col] = np.nan

    if "origin_stop_name" in df.columns and "destination_stop_name" in df.columns:
        same = df["origin_stop_name"] == df["destination_stop_name"]
        if same.any():
            logger.warning(f"⚠️ [{dataset_id}] Dropping {same.sum()} rows where origin == destination")
            df = df.loc[~same].copy()

    if "trip_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["trip_id"], keep="first")
        dropped = before - len(df)
        if dropped > 0:
            logger.warning(f"⚠️ [{dataset_id}] Dropping {dropped} duplicate trip_id rows")

    removed = initial_count - len(df)
    if removed > 0:
        logger.info(f"🧹 [{dataset_id}] Sanitized: {removed} rows removed, {len(df)} rows remaining")
    else:
        logger.info(f"✅ [{dataset_id}] No issues found during sanitization")

    return df


def _write_csv(rows: List[Dict], out_csv: Path) -> None:
    if not rows:
        logger.warning("No rows to write")
        return

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)

    for col in ORDERED_COLUMNS:
        if col not in df.columns:
            df[col] = np.nan

    df = df[ORDERED_COLUMNS]
    logger.info(f"📋 Writing {len(df)} rows to {out_csv}")
    df.to_csv(out_csv, mode="w", header=True, index=False, encoding="utf-8")
    logger.info(f"✓ Written {len(df)} rows to {out_csv}")


def _resolve_dataset_output_id(staging_dir: str, dataset_id: str) -> str:
    try:
        ds_dir = Path(staging_dir) / dataset_id
        latest = latest_version_dir(ds_dir)
        if not latest:
            return dataset_id
        metadata = read_metadata(latest)
        return str(metadata.get("dataset_id", dataset_id))
    except Exception:
        return dataset_id


def build_trips_summary_for_dataset(staging_dir: str, dataset_id: str, processed_dir: str) -> Tuple[int, str]:
    try:
        ds_dir = Path(staging_dir) / dataset_id
        latest = latest_version_dir(ds_dir)
        if not latest:
            logger.error(f"ERROR: No version directory found for dataset {dataset_id}")
            return 0, ""

        metadata = read_metadata(latest)
        dataset_id_meta = str(metadata.get("dataset_id", dataset_id))
        out_csv = Path(processed_dir) / dataset_id_meta / f"trips_summary_{dataset_id_meta}.csv"

        agency_df = _normalize_columns(read_csv(latest / "agency.txt"))
        routes_df = _normalize_columns(read_csv(latest / "routes.txt"))
        stops_df = _normalize_columns(read_csv(latest / "stops.txt"))

        calendar_path = latest / "calendar.txt"
        if calendar_path.exists():
            calendar_df = _normalize_columns(read_csv(calendar_path))
            logger.info(f"calendar.txt loaded: {len(calendar_df)} rows")
        else:
            logger.warning(f"⚠️ [{dataset_id}] calendar.txt missing -> continue without calendar")
            calendar_df = pd.DataFrame()

        trips_df = _normalize_columns(read_csv(latest / "trips.txt"))
        stop_times_df = _normalize_columns(read_csv(latest / "stop_times.txt"))

        logger.info(f"Processing dataset: {dataset_id}")
        logger.info(f"Agency rows: {len(agency_df)}, Routes: {len(routes_df)}, Trips: {len(trips_df)}")

        if trips_df.empty or stop_times_df.empty:
            logger.error(f"ERROR: Empty trips or stop_times for dataset {dataset_id}")
            return 0, ""

        if len(trips_df) > 1_000_000:
            logger.warning(f"⚠️ [{dataset_id}] Dataset too large ({len(trips_df)} trips) -> skipping")
            return 0, ""

        stop_country_map = build_stop_country_map(stops_df)

        if "stop_id" in stops_df.columns and "stop_name" in stops_df.columns:
            stops_name = stops_df.set_index("stop_id")["stop_name"].fillna("")
        else:
            logger.warning(f"⚠️ [{dataset_id}] stops.txt missing stop_id/stop_name")
            stops_name = pd.Series(dtype=str)

        stop_times_df = _prepare_stop_times_df(stop_times_df, dataset_id)
        if stop_times_df.empty:
            logger.warning(f"⚠️ [{dataset_id}] No usable stop_times after preparation")
            return 0, ""

        logger.info(f"stop_times columns: {list(stop_times_df.columns)}")
        logger.info("Computing distances and durations...")
        distances_km = compute_distances(stop_times_df, stops_df)
        durations_min = compute_durations(stop_times_df)

        first = stop_times_df.groupby("trip_id").first()
        last = stop_times_df.groupby("trip_id").last()

        log_memory(f"After distance/duration for {dataset_id} - ")
        del stop_times_df
        gc.collect()

        trips = trips_df.copy()
        if "trip_id" not in trips.columns:
            logger.error(f"ERROR: trips.txt missing trip_id for dataset {dataset_id}")
            return 0, ""

        trips["trip_id"] = trips["trip_id"].fillna("").astype(str).str.strip()
        trips = trips[trips["trip_id"] != ""].copy()
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
        log_memory(f"After freq_map for {dataset_id} - ")

        all_rows: List[Dict] = []

        if len(trips) > 500_000:
            logger.info("Large dataset, splitting by agency...")
            agencies_split = split_by_agency(trips)
            for agency_id, trips_chunk in agencies_split.items():
                logger.info(f"Processing agency {agency_id} with {len(trips_chunk)} trips...")
                _process_trips_chunk(
                    trips_chunk,
                    first,
                    last,
                    stops_name,
                    stop_country_map,
                    distances_km,
                    durations_min,
                    dataset_id_meta,
                    processed_dir,
                    freq_map,
                    all_rows,
                )
                del trips_chunk
                gc.collect()
        else:
            chunk_size = 5000
            for chunk_start in range(0, len(trips), chunk_size):
                chunk_end = min(chunk_start + chunk_size, len(trips))
                chunk = trips.iloc[chunk_start:chunk_end].copy()
                logger.info(f"Processing chunk {chunk_start}-{chunk_end} of {len(trips)}")
                _process_trips_chunk(
                    chunk,
                    first,
                    last,
                    stops_name,
                    stop_country_map,
                    distances_km,
                    durations_min,
                    dataset_id_meta,
                    processed_dir,
                    freq_map,
                    all_rows,
                )
                del chunk
                gc.collect()

        if not all_rows:
            logger.warning(f"⚠️ No valid rows for dataset {dataset_id}")
            return 0, ""

        df_final = pd.DataFrame(all_rows)
        df_final = _sanitize_dataframe(df_final, dataset_id)

        if df_final.empty:
            logger.warning(f"⚠️ No valid rows after sanitization for dataset {dataset_id}")
            return 0, ""

        if out_csv.exists():
            out_csv.unlink()
        _write_csv(df_final.to_dict("records"), out_csv)

        logger.info(f"✓ Generated {len(df_final)} trip summaries for dataset {dataset_id}")
        return len(df_final), str(out_csv)

    except Exception as e:
        logger.error(f"✗ Exception in build_trips_summary_for_dataset({dataset_id}): {e}", exc_info=True)
        return 0, ""


def transform_gtfs(staging_dir: str, processed_dir: str, max_workers: int = 4, skip_existing: bool = True) -> List[str]:
    written: List[str] = []
    staging = Path(staging_dir)

    if not staging.exists():
        logger.error(f"ERROR: Staging directory not found: {staging}")
        return written

    datasets = sorted([p for p in staging.iterdir() if p.is_dir()])
    if not datasets:
        logger.warning("No datasets to transform")
        return written

    if skip_existing:
        filtered = []
        skipped = 0
        for ds in datasets:
            out_id = _resolve_dataset_output_id(staging_dir, ds.name)
            out_path = Path(processed_dir) / out_id / f"trips_summary_{out_id}.csv"
            if out_path.exists():
                skipped += 1
            else:
                filtered.append(ds)
        datasets = filtered
        if skipped:
            logger.info(f"⏭️ Skipping {skipped} already processed datasets")

    if not datasets:
        logger.info("✅ All datasets already processed")
        return [str(p) for p in Path(processed_dir).glob("*/trips_summary_*.csv")]

    safe_workers = min(max_workers, 2)
    total = len(datasets)
    logger.info(f"🚀 Starting transform for {total} datasets (max_workers={safe_workers})")

    try:
        with ProcessPoolExecutor(max_workers=safe_workers) as executor:
            futures = {
                executor.submit(build_trips_summary_for_dataset, staging_dir, ds.name, processed_dir): ds.name
                for ds in datasets
            }

            done_count = 0
            for future in as_completed(futures):
                ds_name = futures[future]
                done_count += 1
                try:
                    count, out_csv_path = future.result(timeout=1200)
                    if count > 0 and out_csv_path:
                        written.append(out_csv_path)
                        logger.info(f"[{done_count}/{total}] ✓ Dataset {ds_name}: {count} rows -> {out_csv_path}")
                    else:
                        logger.warning(f"[{done_count}/{total}] ⚠️ Dataset {ds_name}: no rows generated")
                except FuturesTimeoutError:
                    logger.error(f"[{done_count}/{total}] ✗ Dataset {ds_name}: TIMEOUT")
                except Exception as e:
                    logger.error(f"[{done_count}/{total}] ✗ Dataset {ds_name}: {e}")
                    gc.collect()

    except BrokenProcessPool:
        logger.error("✗ ProcessPool cassé, fallback séquentiel")
        for i, ds in enumerate(datasets, start=1):
            try:
                count, out_csv_path = build_trips_summary_for_dataset(staging_dir, ds.name, processed_dir)
                if count > 0 and out_csv_path:
                    written.append(out_csv_path)
                    logger.info(f"[fallback {i}/{total}] ✓ Dataset {ds.name}: {count} rows -> {out_csv_path}")
                else:
                    logger.warning(f"[fallback {i}/{total}] ⚠️ Dataset {ds.name}: no rows generated")
            except Exception as e:
                logger.error(f"[fallback {i}/{total}] ✗ Dataset {ds.name}: {e}")
                gc.collect()

    logger.info(f"✓✓✓ Transform completed - {len(written)} files written")
    return written