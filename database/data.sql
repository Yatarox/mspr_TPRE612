/* =========================================================
   6) IMPORT
   =========================================================
   Importer le CSV dans stg_trips_summary en renseignant load_id.
*/

-- Choisis le lot à traiter
SET @LOAD_ID = 1;

INSERT INTO stg_trips_summary (
  load_id,
  dataset_id, trip_id, route_id, route_name,
  agency_id, agency_name, service_type,
  origin_stop_name, destination_stop_name,
  departure_time, arrival_time, distance_km, duration_h
)
SELECT
  @LOAD_ID,
  dataset_id, trip_id, route_id, route_name,
  agency_id, agency_name, service_type,
  origin_stop_name, destination_stop_name,
  departure_time, arrival_time, distance_km, duration_h
FROM stg_trips_summary_raw;


/* =========================================================
   7) ETL INCREMENTAL (pour un lot donné)
   ========================================================= */

-- 7.1 Dimensions
INSERT IGNORE INTO dim_dataset (dataset_id)
SELECT DISTINCT dataset_id
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_trip (trip_id)
SELECT DISTINCT trip_id
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_route (route_id, route_name)
SELECT DISTINCT route_id, route_name
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_agency (agency_id, agency_name)
SELECT DISTINCT agency_id, agency_name
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_service_type (service_type)
SELECT DISTINCT service_type
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_location (stop_name)
SELECT DISTINCT origin_stop_name
FROM stg_trips_summary
WHERE load_id = @LOAD_ID
UNION
SELECT DISTINCT destination_stop_name
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

INSERT IGNORE INTO dim_time (time_value, hour, minute, second)
SELECT DISTINCT
  CAST(departure_time AS TIME) AS time_value,
  HOUR(CAST(departure_time AS TIME)) AS hour,
  MINUTE(CAST(departure_time AS TIME)) AS minute,
  SECOND(CAST(departure_time AS TIME)) AS second
FROM stg_trips_summary
WHERE load_id = @LOAD_ID
UNION DISTINCT
SELECT DISTINCT
  CAST(arrival_time AS TIME) AS time_value,
  HOUR(CAST(arrival_time AS TIME)) AS hour,
  MINUTE(CAST(arrival_time AS TIME)) AS minute,
  SECOND(CAST(arrival_time AS TIME)) AS second
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

-- 7.2 Fact (UPSERT = dernier état)
INSERT INTO fact_trip_summary (
  dataset_sk, trip_sk, route_sk, agency_sk, service_sk,
  origin_location_sk, destination_location_sk,
  departure_time_sk, arrival_time_sk,
  distance_km, duration_h,
  last_load_id, last_loaded_at
)
SELECT
  ds.dataset_sk,
  tr.trip_sk,
  ro.route_sk,
  ag.agency_sk,
  sv.service_sk,
  lo.location_sk,
  ld.location_sk,
  td.time_sk,
  ta.time_sk,
  st.distance_km,
  st.duration_h,
  st.load_id,
  st.loaded_at
FROM stg_trips_summary st
JOIN dim_dataset ds ON ds.dataset_id = st.dataset_id
JOIN dim_trip tr ON tr.trip_id = st.trip_id
JOIN dim_route ro ON ro.route_id = st.route_id
JOIN dim_agency ag ON ag.agency_id = st.agency_id
JOIN dim_service_type sv ON sv.service_type = st.service_type
JOIN dim_location lo ON lo.stop_name = st.origin_stop_name
JOIN dim_location ld ON ld.stop_name = st.destination_stop_name
JOIN dim_time td ON td.time_value = CAST(st.departure_time AS TIME)
JOIN dim_time ta ON ta.time_value = CAST(st.arrival_time AS TIME)
WHERE st.load_id = @LOAD_ID
ON DUPLICATE KEY UPDATE
  dataset_sk = VALUES(dataset_sk),
  route_sk = VALUES(route_sk),
  agency_sk = VALUES(agency_sk),
  service_sk = VALUES(service_sk),
  origin_location_sk = VALUES(origin_location_sk),
  destination_location_sk = VALUES(destination_location_sk),
  departure_time_sk = VALUES(departure_time_sk),
  arrival_time_sk = VALUES(arrival_time_sk),
  distance_km = VALUES(distance_km),
  duration_h = VALUES(duration_h),
  last_load_id = VALUES(last_load_id),
  last_loaded_at = VALUES(last_loaded_at);

/* =========================================================
   8) CHECKS
   ========================================================= */
-- Combien de lignes dans le lot ?
SELECT @LOAD_ID AS load_id, COUNT(*) AS rows_in_batch
FROM stg_trips_summary
WHERE load_id = @LOAD_ID;

-- Total facts
SELECT COUNT(*) AS fact_rows FROM fact_trip_summary;
