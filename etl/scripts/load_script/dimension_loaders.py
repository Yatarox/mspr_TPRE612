from load_script.dimension_cache import dim_cache
from airflow.exceptions import AirflowException
from typing import Optional
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# Dimensions (row-wise fallback, conservées pour compat)
# ============================================================


def load_dim_dataset(hook, dataset_id: int) -> int:
    key = f"dataset_{dataset_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT dataset_sk FROM dim_dataset WHERE dataset_id = %s",
        parameters=(
            dataset_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_dataset (dataset_id) VALUES (%s) ON DUPLICATE KEY UPDATE dataset_id=VALUES(dataset_id)",
        parameters=(
            dataset_id,
        ))
    row = hook.get_first(
        "SELECT dataset_sk FROM dim_dataset WHERE dataset_id = %s",
        parameters=(
            dataset_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    raise AirflowException(f"Cannot load dataset dimension for {dataset_id}")


def load_dim_trip(hook, trip_id: str) -> int:
    key = f"trip_{trip_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT trip_sk FROM dim_trip WHERE trip_id = %s",
        parameters=(
            trip_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_trip (trip_id) VALUES (%s) ON DUPLICATE KEY UPDATE trip_id=VALUES(trip_id)",
        parameters=(
            trip_id,
        ))
    row = hook.get_first(
        "SELECT trip_sk FROM dim_trip WHERE trip_id = %s",
        parameters=(
            trip_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    raise AirflowException(f"Cannot load trip dimension for {trip_id}")


def load_dim_route(hook, route_id: str, route_name: str) -> int:
    key = f"route_{route_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT route_sk FROM dim_route WHERE route_id = %s",
        parameters=(
            route_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_route (route_id, route_name) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE route_name=VALUES(route_name)",
        parameters=(route_id, route_name)
    )
    row = hook.get_first(
        "SELECT route_sk FROM dim_route WHERE route_id = %s",
        parameters=(
            route_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    raise AirflowException(f"Cannot load route dimension for {route_id}")


def load_dim_agency(hook, agency_id: str, agency_name: str) -> int:
    key = f"agency_{agency_id}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT agency_sk FROM dim_agency WHERE agency_id = %s",
        parameters=(
            agency_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_agency (agency_id, agency_name) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE agency_name=VALUES(agency_name)",
        parameters=(agency_id, agency_name)
    )
    row = hook.get_first(
        "SELECT agency_sk FROM dim_agency WHERE agency_id = %s",
        parameters=(
            agency_id,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    raise AirflowException(f"Cannot load agency dimension for {agency_id}")


def load_dim_service_type(hook, service_type: str) -> int:
    key = f"service_{service_type}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT service_sk FROM dim_service_type WHERE service_type = %s",
        parameters=(
            service_type,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_service_type (service_type) VALUES (%s) ON DUPLICATE KEY UPDATE service_type=VALUES(service_type)",
        parameters=(
            service_type,
        ))
    row = hook.get_first(
        "SELECT service_sk FROM dim_service_type WHERE service_type = %s",
        parameters=(
            service_type,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    raise AirflowException(
        f"Cannot load service_type dimension for {service_type}")


def load_dim_train_type(hook, train_type: Optional[str]) -> Optional[int]:
    if not train_type:
        return None
    key = f"train_type_{train_type}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT train_type_sk FROM dim_train_type WHERE train_type = %s",
        parameters=(
            train_type,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_train_type (train_type) VALUES (%s) ON DUPLICATE KEY UPDATE train_type=VALUES(train_type)",
        parameters=(
            train_type,
        ))
    row = hook.get_first(
        "SELECT train_type_sk FROM dim_train_type WHERE train_type = %s",
        parameters=(
            train_type,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    return None


def load_dim_traction(hook, traction: Optional[str]) -> Optional[int]:
    if not traction:
        return None
    key = f"traction_{traction}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT traction_sk FROM dim_traction WHERE traction = %s",
        parameters=(
            traction,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_traction (traction) VALUES (%s) ON DUPLICATE KEY UPDATE traction=VALUES(traction)",
        parameters=(
            traction,
        ))
    row = hook.get_first(
        "SELECT traction_sk FROM dim_traction WHERE traction = %s",
        parameters=(
            traction,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    return None


def load_dim_country(hook, country_code: Optional[str]) -> Optional[int]:
    if not country_code:
        return None
    key = f"country_{country_code}"
    cached = dim_cache.get(key)
    if cached:
        return cached

    row = hook.get_first(
        "SELECT country_sk FROM dim_country WHERE country_code = %s",
        parameters=(
            country_code,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]

    # Auto-crée le pays si absent, puis relit
    try:
        hook.run(
            "INSERT INTO dim_country (country_code) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE country_code=VALUES(country_code)",
            parameters=(country_code,)
        )
        row = hook.get_first(
            "SELECT country_sk FROM dim_country WHERE country_code = %s",
            parameters=(
                country_code,
            ))
        if row:
            dim_cache.set(key, row[0])
            return row[0]
    except Exception as e:
        logger.warning(
            f"Could not insert country '{country_code}' into dim_country: {e}")

    return None


def load_dim_location(
        hook,
        stop_name: Optional[str],
        country_code: Optional[str] = None) -> Optional[int]:
    if not stop_name:
        return None

    if country_code:
        exists = hook.get_first(
            "SELECT 1 FROM dim_country WHERE country_code = %s",
            parameters=(
                country_code,
            ))
        if not exists:
            logger.warning(
                f"[FK] country_code '{country_code}' not in dim_country -> set NULL for location '{stop_name}'")
            country_code = None

    key = f"location_{stop_name}_{country_code}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT location_sk FROM dim_location WHERE stop_name = %s",
        parameters=(
            stop_name,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    hook.run(
        "INSERT INTO dim_location (stop_name, country_code) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE stop_name=VALUES(stop_name), country_code=VALUES(country_code)",
        parameters=(
            stop_name,
            country_code))
    row = hook.get_first(
        "SELECT location_sk FROM dim_location WHERE stop_name = %s",
        parameters=(
            stop_name,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    return None


def load_dim_time(hook, time_str: Optional[str]) -> Optional[int]:
    if not time_str:
        return None
    key = f"time_{time_str}"
    cached = dim_cache.get(key)
    if cached:
        return cached
    row = hook.get_first(
        "SELECT time_sk FROM dim_time WHERE time_value = %s",
        parameters=(
            time_str,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
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
        parameters=(
            time_str,
            hour,
            minute,
            second))
    row = hook.get_first(
        "SELECT time_sk FROM dim_time WHERE time_value = %s",
        parameters=(
            time_str,
        ))
    if row:
        dim_cache.set(key, row[0])
        return row[0]
    return None
