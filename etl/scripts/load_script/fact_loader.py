from airflow.providers.mysql.hooks.mysql import MySqlHook

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
    row = hook.get_first(
        "SELECT COUNT(*) FROM fact_trip_summary WHERE last_load_id = %s",
        parameters=(
            load_id,
        ))
    return int(row[0]) if row and row[0] is not None else 0