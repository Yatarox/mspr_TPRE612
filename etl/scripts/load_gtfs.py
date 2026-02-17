import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Tuple, Any, List, Optional
import re

import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "load_gtfs.py v3.0 (auto-create dim_country, set-based fact load)"
logger.info(f"[BOOT] {VERSION}")

# ============================================================
# Dimension Loaders (avec cache et stats)
# ============================================================

class DimensionCache:
    def __init__(self, max_size: int = 10_000):
        self.cache: Dict[str, Any] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key):
        val = self.cache.get(key)
        if val is not None:
            self.hits += 1
        else:
            self.misses += 1
        return val

    def set(self, key, value):
        if len(self.cache) >= self.max_size:
            oldest_key = next(iter(self.cache))
            self.cache.pop(oldest_key, None)
        self.cache[key] = value

    def stats(self):
        total = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total else 0
        logger.info(f"Cache stats - hits:{self.hits} misses:{self.misses} hit_rate:{hit_rate:.1f}% size:{len(self.cache)}")

    def clear(self):
        self.stats()
        self.cache.clear()
        self.hits = 0
        self.misses = 0

dim_cache = DimensionCache()

# ============================================================
# Helpers
# ============================================================

def get_column_max_length(hook: MySqlHook, table: str, column: str) -> Optional[int]:
    try:
        row = hook.get_first(
            """
            SELECT CHARACTER_MAXIMUM_LENGTH
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME = %s
              AND COLUMN_NAME = %s
            """,
            parameters=(table, column),
        )
        if row and row[0]:
            return int(row[0])
    except Exception:
        pass
    return None

def get_staging_country_limits(hook: MySqlHook) -> Tuple[int, int]:
    default_len = 30
    o_len = get_column_max_length(hook, "stg_trips_summary", "origin_country") or default_len
    d_len = get_column_max_length(hook, "stg_trips_summary", "destination_country") or default_len
    logger.info(f"Staging country column widths -> origin_country:{o_len}, destination_country:{d_len}")
    return o_len, d_len

def sanitize_country_for_staging(value: Any, max_len: int, field_name: str) -> Optional[str]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if s == "":
        return None

    up = s.upper()

    if up in {"UNKNOWN", "UNKN", "UNK", "NA", "N/A", "NONE", "NULL"}:
        return None

    if re.match(r"^\d{4}-\d{2}-\d{2}", up) or re.match(r"^\d{2}/\d{2}/\d{4}", up):
        logger.warning(f"[country] Date-like detected in {field_name}: '{s}' -> NULL")
        return None

    up = re.sub(r"[^A-Z]", "", up)
    if up == "":
        return None

    if len(up) > max_len:
        logger.warning(f"[country] Too long for {field_name} (len={len(up)}, max={max_len}) -> truncated: '{up[:max_len]}'")
        up = up[:max_len]

    return up

# ============================================================
# Dimensions (row-wise fallback, conservées pour compat)
# ============================================================

def load_dim_dataset(hook, dataset_id: int) -> int:
    key = f"dataset_{dataset_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT dataset_sk FROM dim_dataset WHERE dataset_id = %s", parameters=(dataset_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run("INSERT INTO dim_dataset (dataset_id) VALUES (%s) ON DUPLICATE KEY UPDATE dataset_id=VALUES(dataset_id)", parameters=(dataset_id,))
    row = hook.get_first("SELECT dataset_sk FROM dim_dataset WHERE dataset_id = %s", parameters=(dataset_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    raise AirflowException(f"Cannot load dataset dimension for {dataset_id}")

def load_dim_trip(hook, trip_id: str) -> int:
    key = f"trip_{trip_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT trip_sk FROM dim_trip WHERE trip_id = %s", parameters=(trip_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run("INSERT INTO dim_trip (trip_id) VALUES (%s) ON DUPLICATE KEY UPDATE trip_id=VALUES(trip_id)", parameters=(trip_id,))
    row = hook.get_first("SELECT trip_sk FROM dim_trip WHERE trip_id = %s", parameters=(trip_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    raise AirflowException(f"Cannot load trip dimension for {trip_id}")

def load_dim_route(hook, route_id: str, route_name: str) -> int:
    key = f"route_{route_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT route_sk FROM dim_route WHERE route_id = %s", parameters=(route_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run(
        "INSERT INTO dim_route (route_id, route_name) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE route_name=VALUES(route_name)",
        parameters=(route_id, route_name)
    )
    row = hook.get_first("SELECT route_sk FROM dim_route WHERE route_id = %s", parameters=(route_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    raise AirflowException(f"Cannot load route dimension for {route_id}")

def load_dim_agency(hook, agency_id: str, agency_name: str) -> int:
    key = f"agency_{agency_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT agency_sk FROM dim_agency WHERE agency_id = %s", parameters=(agency_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run(
        "INSERT INTO dim_agency (agency_id, agency_name) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE agency_name=VALUES(agency_name)",
        parameters=(agency_id, agency_name)
    )
    row = hook.get_first("SELECT agency_sk FROM dim_agency WHERE agency_id = %s", parameters=(agency_id,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    raise AirflowException(f"Cannot load agency dimension for {agency_id}")

def load_dim_service_type(hook, service_type: str) -> int:
    key = f"service_{service_type}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT service_sk FROM dim_service_type WHERE service_type = %s", parameters=(service_type,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run("INSERT INTO dim_service_type (service_type) VALUES (%s) ON DUPLICATE KEY UPDATE service_type=VALUES(service_type)", parameters=(service_type,))
    row = hook.get_first("SELECT service_sk FROM dim_service_type WHERE service_type = %s", parameters=(service_type,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    raise AirflowException(f"Cannot load service_type dimension for {service_type}")

def load_dim_train_type(hook, train_type: Optional[str]) -> Optional[int]:
    if not train_type:
        return None
    key = f"train_type_{train_type}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT train_type_sk FROM dim_train_type WHERE train_type = %s", parameters=(train_type,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run("INSERT INTO dim_train_type (train_type) VALUES (%s) ON DUPLICATE KEY UPDATE train_type=VALUES(train_type)", parameters=(train_type,))
    row = hook.get_first("SELECT train_type_sk FROM dim_train_type WHERE train_type = %s", parameters=(train_type,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    return None

def load_dim_traction(hook, traction: Optional[str]) -> Optional[int]:
    if not traction:
        return None
    key = f"traction_{traction}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT traction_sk FROM dim_traction WHERE traction = %s", parameters=(traction,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run("INSERT INTO dim_traction (traction) VALUES (%s) ON DUPLICATE KEY UPDATE traction=VALUES(traction)", parameters=(traction,))
    row = hook.get_first("SELECT traction_sk FROM dim_traction WHERE traction = %s", parameters=(traction,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    return None

def load_dim_country(hook, country_code: Optional[str]) -> Optional[int]:
    if not country_code:
        return None
    key = f"country_{country_code}"
    cached = dim_cache.get(key)
    if cached:
        return cached

    row = hook.get_first("SELECT country_sk FROM dim_country WHERE country_code = %s", parameters=(country_code,))
    if row:
        dim_cache.set(key, row[0]); return row[0]

    # Auto-crée le pays si absent, puis relit
    try:
        hook.run(
            "INSERT INTO dim_country (country_code) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE country_code=VALUES(country_code)",
            parameters=(country_code,)
        )
        row = hook.get_first("SELECT country_sk FROM dim_country WHERE country_code = %s", parameters=(country_code,))
        if row:
            dim_cache.set(key, row[0]); return row[0]
    except Exception as e:
        logger.warning(f"Could not insert country '{country_code}' into dim_country: {e}")

    return None

def load_dim_location(hook, stop_name: Optional[str], country_code: Optional[str] = None) -> Optional[int]:
    if not stop_name:
        return None

    if country_code:
        exists = hook.get_first("SELECT 1 FROM dim_country WHERE country_code = %s", parameters=(country_code,))
        if not exists:
            logger.warning(f"[FK] country_code '{country_code}' not in dim_country -> set NULL for location '{stop_name}'")
            country_code = None

    key = f"location_{stop_name}_{country_code}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT location_sk FROM dim_location WHERE stop_name = %s", parameters=(stop_name,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    hook.run(
        "INSERT INTO dim_location (stop_name, country_code) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE stop_name=VALUES(stop_name), country_code=VALUES(country_code)",
        parameters=(stop_name, country_code)
    )
    row = hook.get_first("SELECT location_sk FROM dim_location WHERE stop_name = %s", parameters=(stop_name,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    return None

def load_dim_time(hook, time_str: Optional[str]) -> Optional[int]:
    if not time_str:
        return None
    key = f"time_{time_str}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first("SELECT time_sk FROM dim_time WHERE time_value = %s", parameters=(time_str,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    try:
        parts = time_str.split(":")
        hour = int(parts[0]) % 24
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
    except Exception:
        hour, minute, second = 0, 0, 0
    hook.run(
        "INSERT INTO dim_time (time_value, hour, minute, second) VALUES (%s, %s, %s, %s) "
        "ON DUPLICATE KEY UPDATE time_value=VALUES(time_value)",
        parameters=(time_str, hour, minute, second)
    )
    row = hook.get_first("SELECT time_sk FROM dim_time WHERE time_value = %s", parameters=(time_str,))
    if row:
        dim_cache.set(key, row[0]); return row[0]
    return None

# ============================================================
# Validation
# ============================================================

def validate_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    required = ["trip_id", "agency_name", "route_name", "origin_stop_name", "destination_stop_name"]
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
        for chunk_idx, chunk in enumerate(pd.read_csv(csv_path, dtype=str, chunksize=5000)):
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
                route_id = route_full.split(" - ")[0] if " - " in route_full else route_full

                origin_country = sanitize_country_for_staging(row.get("origin_country"), origin_max_len, f"origin_country row {row_num}")
                destination_country = sanitize_country_for_staging(row.get("destination_country"), dest_max_len, f"destination_country row {row_num}")

                if origin_country and len(origin_country) > origin_max_len:
                    diagnostics.append(f"row {row_num} origin_country '{origin_country}' len={len(origin_country)} > {origin_max_len}")
                    origin_country = origin_country[:origin_max_len]
                if destination_country and len(destination_country) > dest_max_len:
                    diagnostics.append(f"row {row_num} destination_country '{destination_country}' len={len(destination_country)} > {dest_max_len}")
                    destination_country = destination_country[:dest_max_len]

                batch_values.append((
                    load_id, datetime.now(), dataset_id,
                    str(row.get("trip_id", "")),
                    route_id,
                    route_full,
                    str(row.get("agency_name", "")).split(":")[0] if "agency_name" in row else "",
                    str(row.get("agency_name", "")),
                    str(row.get("service_type", "")),
                    str(row.get("origin_stop_name", "")),
                    origin_country,
                    str(row.get("destination_stop_name", "")),
                    destination_country,
                    str(row.get("departure_time", "")),
                    str(row.get("arrival_time", "")),
                    float(row.get("distance_km", 0)) if pd.notna(row.get("distance_km")) else None,
                    float(row.get("duration_h", 0)) if pd.notna(row.get("duration_h")) else None,
                    str(row.get("train_type", "")),
                    str(row.get("traction", "")),
                    "",  # transport_type placeholder
                    float(row.get("emission_gco2e_pkm", 0)) if pd.notna(row.get("emission_gco2e_pkm")) else None,
                    float(row.get("total_emission_kgco2e", 0)) if pd.notna(row.get("total_emission_kgco2e")) else None,
                    int(row.get("frequency_per_week", 0)) if pd.notna(row.get("frequency_per_week")) else None,
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

            placeholders = ", ".join(["(" + ",".join(["%s"] * 23) + ")"] * len(batch_values))
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
                logger.error(f"🔴 SQL INSERT FAILED at chunk {chunk_idx}, rows {start_row}-{end_row}")
                logger.error(f"🔴 Error: {sql_error}")
                logger.error(f"🔴 Batch size: {len(batch_values)} rows")

                try:
                    cols = [
                        "load_id","loaded_at","dataset_id","trip_id","route_id","route_name",
                        "agency_id","agency_name","service_type","origin_stop_name","origin_country",
                        "destination_stop_name","destination_country","departure_time","arrival_time",
                        "distance_km","duration_h","train_type","traction","transport_type",
                        "emission_gco2e_pkm","total_emission_kgco2e","frequency_per_week"
                    ]
                    df = pd.DataFrame(batch_values, columns=cols)
                    dump_dir = Path("/opt/airflow/logs/staging_dumps")
                    dump_dir.mkdir(parents=True, exist_ok=True)
                    dump_path = dump_dir / f"stg_batch_fail_chunk{chunk_idx}_load{load_id}_rows{start_row}-{end_row}.csv"
                    df.to_csv(dump_path, index=False, encoding="utf-8")
                    logger.error(f"  🔎 Batch dump saved: {dump_path}")
                except Exception as dump_err:
                    logger.error(f"  ⚠️ Could not dump batch CSV: {dump_err}")

                offenders = []
                for dbg in batch_debug:
                    if (dbg["origin_country"] and dbg["origin_len"] > origin_max_len) or (dbg["destination_country"] and dbg["dest_len"] > dest_max_len):
                        offenders.append(dbg)
                for dbg in offenders[:10]:
                    logger.error(f"  offender batch_idx={dbg['batch_index']} csv_row={dbg['csv_row_num']} "
                                 f"origin='{dbg['origin_country']}'(len={dbg['origin_len']}, max={origin_max_len}) "
                                 f"dest='{dbg['destination_country']}'(len={dbg['dest_len']}, max={dest_max_len})")

                for d in diagnostics[:20]:
                    logger.error(f"  diag: {d}")

                raise

            if diagnostics:
                logger.warning(f"Chunk {chunk_idx}: {len(diagnostics)} country diagnostics. Examples: {diagnostics[:3]}")

            loaded_count += len(batch_values)
            if chunk_idx % 10 == 0:
                logger.info(f"  Chunk {chunk_idx}: +{len(batch_values)} rows (total {loaded_count})")
                for dbg in batch_debug:
                    logger.info(
                        f"[PRE-INSERT] chunk={chunk_idx} batch_idx={dbg['batch_index']} csv_row={dbg['csv_row_num']} "
                        f"trip={dbg['trip_id']} route={dbg['route_id']} "
                        f"origin='{dbg['origin_country']}'(len={dbg['origin_len']}) "
                        f"dest='{dbg['destination_country']}'(len={dbg['dest_len']})"
                    )

        logger.info(f"✓ Loaded {loaded_count} rows into stg_trips_summary (processed {row_num} total rows)")
        return loaded_count

    except Exception as e:
        logger.error(f"Staging load failed at row {row_num}: {e}", exc_info=True)
        raise

# ============================================================
# Set-based dimension upsert + fact load (rapide)
# ============================================================

def upsert_dimensions_from_staging(hook: MySqlHook, load_id: int) -> None:
    # Countries (origin + destination)
    hook.run("""
        INSERT INTO dim_country (country_code)
        SELECT c FROM (
            SELECT DISTINCT origin_country AS c
            FROM stg_trips_summary WHERE load_id=%s AND origin_country IS NOT NULL
            UNION
            SELECT DISTINCT destination_country AS c
            FROM stg_trips_summary WHERE load_id=%s AND destination_country IS NOT NULL
        ) t
        ON DUPLICATE KEY UPDATE country_code=VALUES(country_code)
    """, parameters=(load_id, load_id))

    # Times
    hook.run("""
        INSERT INTO dim_time (time_value, hour, minute, second)
        SELECT t, 
               CAST(SUBSTRING_INDEX(t, ':', 1) AS UNSIGNED) %% 24,
               CAST(SUBSTRING_INDEX(SUBSTRING_INDEX(t, ':', 2), ':', -1) AS UNSIGNED),
               CAST(SUBSTRING_INDEX(t, ':', -1) AS UNSIGNED)
        FROM (
            SELECT DISTINCT departure_time AS t FROM stg_trips_summary WHERE load_id=%s AND departure_time IS NOT NULL
            UNION
            SELECT DISTINCT arrival_time AS t   FROM stg_trips_summary WHERE load_id=%s AND arrival_time   IS NOT NULL
        ) x
        ON DUPLICATE KEY UPDATE time_value=VALUES(time_value)
    """, parameters=(load_id, load_id))

    # Trips
    hook.run("""
        INSERT INTO dim_trip (trip_id)
        SELECT DISTINCT trip_id
        FROM stg_trips_summary WHERE load_id=%s
        ON DUPLICATE KEY UPDATE trip_id=VALUES(trip_id)
    """, parameters=(load_id,))

    # Dataset
    hook.run("""
        INSERT INTO dim_dataset (dataset_id)
        SELECT DISTINCT dataset_id
        FROM stg_trips_summary WHERE load_id=%s
        ON DUPLICATE KEY UPDATE dataset_id=VALUES(dataset_id)
    """, parameters=(load_id,))

    # Routes
    hook.run("""
        INSERT INTO dim_route (route_id, route_name)
        SELECT DISTINCT route_id, route_name
        FROM stg_trips_summary WHERE load_id=%s
        ON DUPLICATE KEY UPDATE route_name=VALUES(route_name)
    """, parameters=(load_id,))

    # Agencies
    hook.run("""
        INSERT INTO dim_agency (agency_id, agency_name)
        SELECT DISTINCT agency_id, agency_name
        FROM stg_trips_summary WHERE load_id=%s
        ON DUPLICATE KEY UPDATE agency_name=VALUES(agency_name)
    """, parameters=(load_id,))

    # Service type
    hook.run("""
        INSERT INTO dim_service_type (service_type)
        SELECT DISTINCT service_type
        FROM stg_trips_summary WHERE load_id=%s AND service_type IS NOT NULL
        ON DUPLICATE KEY UPDATE service_type=VALUES(service_type)
    """, parameters=(load_id,))

    # Train type
    hook.run("""
        INSERT INTO dim_train_type (train_type)
        SELECT DISTINCT train_type
        FROM stg_trips_summary WHERE load_id=%s AND train_type IS NOT NULL
        ON DUPLICATE KEY UPDATE train_type=VALUES(train_type)
    """, parameters=(load_id,))

    # Traction
    hook.run("""
        INSERT INTO dim_traction (traction)
        SELECT DISTINCT traction
        FROM stg_trips_summary WHERE load_id=%s AND traction IS NOT NULL
        ON DUPLICATE KEY UPDATE traction=VALUES(traction)
    """, parameters=(load_id,))

    # Locations (origin + destination)
    hook.run("""
        INSERT INTO dim_location (stop_name, country_code)
        SELECT stop_name, country_code
        FROM (
            SELECT DISTINCT origin_stop_name AS stop_name, origin_country AS country_code
            FROM stg_trips_summary WHERE load_id=%s
            UNION
            SELECT DISTINCT destination_stop_name, destination_country
            FROM stg_trips_summary WHERE load_id=%s
        ) t
        ON DUPLICATE KEY UPDATE 
            stop_name=VALUES(stop_name),
            country_code=VALUES(country_code)
    """, parameters=(load_id, load_id))

def load_fact_table(hook, load_id: int) -> int:
    # Upsert dimensions en bloc
    upsert_dimensions_from_staging(hook, load_id)

    # Insert/Update des faits en un seul SQL
    hook.run("""
        INSERT INTO fact_trip_summary
        (trip_sk, dataset_sk, route_sk, agency_sk, service_sk, train_type_sk, traction_sk,
         origin_location_sk, origin_country_sk, destination_location_sk, destination_country_sk,
         departure_time_sk, arrival_time_sk,
         distance_km, duration_h, emission_gco2e_pkm, total_emission_kgco2e, frequency_per_week,
         last_load_id, last_loaded_at)
        SELECT
          dt.trip_sk, dd.dataset_sk, dr.route_sk, da.agency_sk, ds.service_sk, dtt.train_type_sk, dtr.traction_sk,
          dlo.location_sk, dco.country_sk, dld.location_sk, dcd.country_sk,
          tdep.time_sk, tarr.time_sk,
          s.distance_km, s.duration_h, s.emission_gco2e_pkm, s.total_emission_kgco2e, s.frequency_per_week,
          %s, NOW()
        FROM stg_trips_summary s
        JOIN dim_dataset      dd  ON dd.dataset_id    = s.dataset_id
        JOIN dim_trip         dt  ON dt.trip_id       = s.trip_id
        LEFT JOIN dim_route   dr  ON dr.route_id      = s.route_id
        LEFT JOIN dim_agency  da  ON da.agency_id     = s.agency_id
        LEFT JOIN dim_service_type ds  ON ds.service_type = s.service_type
        LEFT JOIN dim_train_type  dtt ON dtt.train_type  = s.train_type
        LEFT JOIN dim_traction    dtr ON dtr.traction    = s.traction
        LEFT JOIN dim_country dco ON dco.country_code = s.origin_country
        LEFT JOIN dim_country dcd ON dcd.country_code = s.destination_country
        LEFT JOIN dim_location dlo ON dlo.stop_name = s.origin_stop_name 
                                   AND (dlo.country_code <=> s.origin_country)
        LEFT JOIN dim_location dld ON dld.stop_name = s.destination_stop_name 
                                   AND (dld.country_code <=> s.destination_country)
        LEFT JOIN dim_time tdep ON tdep.time_value = s.departure_time
        LEFT JOIN dim_time tarr ON tarr.time_value = s.arrival_time
        WHERE s.load_id = %s
        ON DUPLICATE KEY UPDATE
          dataset_sk = VALUES(dataset_sk),
          route_sk = VALUES(route_sk),
          agency_sk = VALUES(agency_sk),
          service_sk = VALUES(service_sk),
          train_type_sk = VALUES(train_type_sk),
          traction_sk = VALUES(traction_sk),
          origin_location_sk = VALUES(origin_location_sk),
          origin_country_sk = VALUES(origin_country_sk),
          destination_location_sk = VALUES(destination_location_sk),
          destination_country_sk = VALUES(destination_country_sk),
          departure_time_sk = VALUES(departure_time_sk),
          arrival_time_sk = VALUES(arrival_time_sk),
          distance_km = VALUES(distance_km),
          duration_h = VALUES(duration_h),
          emission_gco2e_pkm = VALUES(emission_gco2e_pkm),
          total_emission_kgco2e = VALUES(total_emission_kgco2e),
          frequency_per_week = VALUES(frequency_per_week),
          last_load_id = VALUES(last_load_id),
          last_loaded_at = VALUES(last_loaded_at)
    """, parameters=(load_id, load_id))

    # Compte fiable: nombre de faits marqués par ce load_id
    row = hook.get_first("SELECT COUNT(*) FROM fact_trip_summary WHERE last_load_id = %s", parameters=(load_id,))
    return int(row[0]) if row and row[0] is not None else 0

# ============================================================
# Entrée principale
# ============================================================

def load_gtfs(processed_dir: str, conn_id: str = "mysql_default") -> Dict[str, Any]:
    hook = MySqlHook(mysql_conn_id=conn_id)
    processed = Path(processed_dir)

    if not processed.exists():
        raise AirflowException(f"Directory not found: {processed}")

    origin_max_len, dest_max_len = get_staging_country_limits(hook)

    total_loaded = 0
    datasets_done = 0

    for dataset_dir in sorted([p for p in processed.iterdir() if p.is_dir()]):
        try:
            dataset_id = int(dataset_dir.name)
        except ValueError:
            logger.warning(f"Skipping invalid directory: {dataset_dir.name}")
            continue

        load_id = int(datetime.now().timestamp() * 1000)

        csv_path = dataset_dir / f"trips_summary_{dataset_id}.csv"
        if not csv_path.exists():
            csv_path = dataset_dir / "trips_summary.csv"

        logger.info(f"Loading dataset {dataset_id}...")
        logger.info(f"  Looking for CSV at: {csv_path}")
        logger.info(f"  CSV exists: {csv_path.exists()}")

        if not csv_path.exists():
            logger.error(f"Available files in {dataset_dir}:")
            for f in dataset_dir.glob("*"):
                logger.error(f"  - {f.name}")

        loaded = load_staging_table(hook, csv_path, load_id, dataset_id, origin_max_len, dest_max_len)
        if loaded == 0:
            logger.warning(f"No data for dataset {dataset_id}")
            dim_cache.clear()
            continue

        processed_count = load_fact_table(hook, load_id)
        total_loaded += processed_count
        datasets_done += 1
        logger.info(f"✓ Dataset {dataset_id}: {processed_count} facts loaded")

        dim_cache.clear()

    if total_loaded == 0:
        raise AirflowException("No data loaded")

    logger.info(f"✓✓✓ SUCCESS: {total_loaded} total facts loaded across {datasets_done} datasets")
    return {"total_rows": total_loaded, "datasets": datasets_done}