from typing import Any, Dict, List, Tuple
from pathlib import Path
from datetime import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from load_script.validation import validate_row
from load_script.helpers import sanitize_country_for_staging
import logging
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ============================================================
# Staging
# ============================================================


def load_staging_table(
    hook: MySqlHook,
    csv_path: Path,
    load_id: int,
    dataset_id: int,
    origin_max_len: int,
    dest_max_len: int
) -> int:
    if not csv_path.exists():
        logger.error(f"CSV not found: {csv_path}")
        return 0

    try:
        hook.run("TRUNCATE TABLE stg_trips_summary")
        logger.info("✓ Staging table truncated")
    except Exception as e:
        logger.warning(f"Could not truncate staging table: {e}")

    loaded_count = 0
    row_num = 0
    try:
        for chunk_idx, chunk in enumerate(
                pd.read_csv(csv_path, dtype=str, chunksize=5000)):
            batch_values: List[Tuple] = []
            diagnostics: List[str] = []
            batch_debug: List[Dict[str, Any]] = []

            for _, row in chunk.iterrows():
                row_num += 1
                row_dict = row.to_dict()
                valid, err = validate_row(row_dict)
                if not valid:
                    logger.debug(f"Skip row {row_num}: {err}")
                    continue

                route_full = str(row.get("route_name", ""))
                route_id = route_full.split(
                    " - ")[0] if " - " in route_full else route_full

                origin_country = sanitize_country_for_staging(
                    row.get("origin_country"), origin_max_len, f"origin_country row {row_num}")
                destination_country = sanitize_country_for_staging(
                    row.get("destination_country"), dest_max_len, f"destination_country row {row_num}")

                if origin_country and len(origin_country) > origin_max_len:
                    diagnostics.append(
                        f"row {row_num} origin_country '{origin_country}' len={
                            len(origin_country)} > {origin_max_len}")
                    origin_country = origin_country[:origin_max_len]
                if destination_country and len(
                        destination_country) > dest_max_len:
                    diagnostics.append(
                        f"row {row_num} destination_country '{destination_country}' len={
                            len(destination_country)} > {dest_max_len}")
                    destination_country = destination_country[:dest_max_len]

                batch_values.append((
                    load_id, datetime.now(), dataset_id,
                    str(row.get("trip_id", "")),
                    route_id,
                    route_full,
                    str(row.get("agency_name", "")).split(":")[
                        0] if "agency_name" in row else "",
                    str(row.get("agency_name", "")),
                    str(row.get("service_type", "")),
                    str(row.get("origin_stop_name", "")),
                    origin_country,
                    str(row.get("destination_stop_name", "")),
                    destination_country,
                    str(row.get("departure_time", "")),
                    str(row.get("arrival_time", "")),
                    float(
                        row.get(
                            "distance_km",
                            0)) if pd.notna(
                        row.get("distance_km")) else None,
                    float(
                        row.get(
                            "duration_h",
                            0)) if pd.notna(
                        row.get("duration_h")) else None,
                    str(row.get("train_type", "")),
                    str(row.get("traction", "")),
                    "",  # transport_type placeholder
                    float(row.get("emission_gco2e_pkm", 0)) if pd.notna(
                        row.get("emission_gco2e_pkm")) else None,
                    float(row.get("total_emission_kgco2e", 0)) if pd.notna(
                        row.get("total_emission_kgco2e")) else None,
                    int(row.get("frequency_per_week", 0)) if pd.notna(
                        row.get("frequency_per_week")) else None,
                ))

                if len(batch_debug) < 5:
                    batch_debug.append({
                        "batch_index": len(batch_values),
                        "csv_row_num": row_num,
                        "trip_id": str(row.get("trip_id", "")),
                        "route_id": route_id,
                        "agency_id": str(row.get("agency_name", "")).split(":")[0] if "agency_name" in row else "",
                        "origin_country": origin_country,
                        "origin_len": 0 if origin_country is None else len(origin_country),
                        "destination_country": destination_country,
                        "dest_len": 0 if destination_country is None else len(destination_country),
                    })

            if not batch_values:
                continue

            placeholders = ", ".join(
                ["(" + ",".join(["%s"] * 23) + ")"] * len(batch_values))
            flat_params: List[Any] = [v for tup in batch_values for v in tup]

            try:
                hook.run(
                    f"""INSERT INTO stg_trips_summary
                        (load_id, loaded_at, dataset_id, trip_id, route_id, route_name,
                         agency_id, agency_name, service_type, origin_stop_name, origin_country,
                         destination_stop_name, destination_country, departure_time, arrival_time,
                         distance_km, duration_h, train_type, traction, transport_type,
                         emission_gco2e_pkm, total_emission_kgco2e, frequency_per_week)
                        VALUES {placeholders}
                    """,
                    parameters=flat_params
                )
            except Exception as sql_error:
                start_row = row_num - len(batch_values) + 1
                end_row = row_num
                logger.error(
                    f"🔴 SQL INSERT FAILED at chunk {chunk_idx}, rows {start_row}-{end_row}")
                logger.error(f"🔴 Error: {sql_error}")
                logger.error(f"🔴 Batch size: {len(batch_values)} rows")

                try:
                    cols = [
                        "load_id",
                        "loaded_at",
                        "dataset_id",
                        "trip_id",
                        "route_id",
                        "route_name",
                        "agency_id",
                        "agency_name",
                        "service_type",
                        "origin_stop_name",
                        "origin_country",
                        "destination_stop_name",
                        "destination_country",
                        "departure_time",
                        "arrival_time",
                        "distance_km",
                        "duration_h",
                        "train_type",
                        "traction",
                        "transport_type",
                        "emission_gco2e_pkm",
                        "total_emission_kgco2e",
                        "frequency_per_week"]
                    df = pd.DataFrame(batch_values, columns=cols)
                    dump_dir = Path("/opt/airflow/logs/staging_dumps")
                    dump_dir.mkdir(parents=True, exist_ok=True)
                    dump_path = dump_dir / \
                        f"stg_batch_fail_chunk{chunk_idx}_load{load_id}_rows{start_row}-{end_row}.csv"
                    df.to_csv(dump_path, index=False, encoding="utf-8")
                    logger.error(f"  🔎 Batch dump saved: {dump_path}")
                except Exception as dump_err:
                    logger.error(f"  ⚠️ Could not dump batch CSV: {dump_err}")

                offenders = []
                for dbg in batch_debug:
                    if (dbg["origin_country"] and dbg["origin_len"] > origin_max_len) or (
                            dbg["destination_country"] and dbg["dest_len"] > dest_max_len):
                        offenders.append(dbg)
                for dbg in offenders[:10]:
                    logger.error(
                        f"  offender batch_idx={
                            dbg['batch_index']} csv_row={
                            dbg['csv_row_num']} " f"origin='{
                            dbg['origin_country']}'(len={
                            dbg['origin_len']}, max={origin_max_len}) " f"dest='{
                            dbg['destination_country']}'(len={
                            dbg['dest_len']}, max={dest_max_len})")

                for d in diagnostics[:20]:
                    logger.error(f"  diag: {d}")

                raise

            if diagnostics:
                logger.warning(
                    f"Chunk {chunk_idx}: {len(diagnostics)} country diagnostics. Examples: {diagnostics[:3]}")

            loaded_count += len(batch_values)
            if chunk_idx % 10 == 0:
                logger.info(
                    f"  Chunk {chunk_idx}: +{len(batch_values)} rows (total {loaded_count})")
                for dbg in batch_debug:
                    logger.info(
                        f"[PRE-INSERT] chunk={chunk_idx} batch_idx={
                            dbg['batch_index']} csv_row={
                            dbg['csv_row_num']} " f"trip={
                            dbg['trip_id']} route={
                            dbg['route_id']} " f"origin='{
                            dbg['origin_country']}'(len={
                            dbg['origin_len']}) " f"dest='{
                                dbg['destination_country']}'(len={
                                    dbg['dest_len']})")

        logger.info(
            f"✓ Loaded {loaded_count} rows into stg_trips_summary (processed {row_num} total rows)")
        return loaded_count

    except Exception as e:
        logger.error(
            f"Staging load failed at row {row_num}: {e}",
            exc_info=True)