/* =========================================================
   1) Database
   ========================================================= */
CREATE DATABASE IF NOT EXISTS rail_dw
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE rail_dw;

DROP TABLE IF EXISTS stg_trips_summary_raw;

/* =========================================================
   2) STAGING (table brute)
   ========================================================= */
CREATE TABLE stg_trips_summary_raw (
  dataset_id INT NOT NULL,
  trip_id VARCHAR(64) NOT NULL,
  route_id VARCHAR(32) NOT NULL,
  route_name VARCHAR(128) NOT NULL,
  agency_id VARCHAR(32) NOT NULL,
  agency_name VARCHAR(128) NOT NULL,
  service_type VARCHAR(16) NOT NULL,
  origin_stop_name VARCHAR(128) NOT NULL,
  destination_stop_name VARCHAR(128) NOT NULL,
  departure_time VARCHAR(16) NOT NULL,
  arrival_time VARCHAR(16) NOT NULL,
  distance_km DECIMAL(10,3) NULL,
  duration_h DECIMAL(6,2) NULL
) ENGINE=InnoDB;


/* =========================================================
   3) STAGING (table brute + gestion par lot)
   ========================================================= */
DROP TABLE IF EXISTS stg_trips_summary;

CREATE TABLE stg_trips_summary (
  -- Gestion des imports par lots
  load_id BIGINT NOT NULL,
  loaded_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

  -- Colonnes du CSV
  dataset_id INT NOT NULL,
  trip_id VARCHAR(64) NOT NULL,
  route_id VARCHAR(32) NOT NULL,
  route_name VARCHAR(128) NOT NULL,
  agency_id VARCHAR(32) NOT NULL,
  agency_name VARCHAR(128) NOT NULL,
  service_type VARCHAR(16) NOT NULL,
  origin_stop_name VARCHAR(128) NOT NULL,
  destination_stop_name VARCHAR(128) NOT NULL,
  departure_time VARCHAR(16) NOT NULL,  -- "HH:MM:SS"
  arrival_time VARCHAR(16) NOT NULL,    -- "HH:MM:SS"
  distance_km DECIMAL(10,3) NULL,
  duration_h DECIMAL(6,2) NULL,

  -- Index
  KEY idx_stg_load_id (load_id),
  KEY idx_stg_trip (trip_id),
  KEY idx_stg_route (route_id),
  KEY idx_stg_agency (agency_id),

  -- Empêche les doublons dans un même lot
  UNIQUE KEY uq_stg_load_trip (load_id, trip_id)
) ENGINE=InnoDB;

/* =========================================================
   4) DIMENSIONS
   ========================================================= */

DROP TABLE IF EXISTS dim_dataset;
CREATE TABLE dim_dataset (
  dataset_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id INT NOT NULL,
  UNIQUE KEY uq_dim_dataset_id (dataset_id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_trip;
CREATE TABLE dim_trip (
  trip_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  trip_id VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_dim_trip_id (trip_id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_route;
CREATE TABLE dim_route (
  route_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  route_id VARCHAR(32) NOT NULL,
  route_name VARCHAR(128) NOT NULL,
  UNIQUE KEY uq_dim_route_id (route_id),
  KEY idx_dim_route_name (route_name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_agency;
CREATE TABLE dim_agency (
  agency_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  agency_id VARCHAR(32) NOT NULL,
  agency_name VARCHAR(128) NOT NULL,
  UNIQUE KEY uq_dim_agency_id (agency_id),
  KEY idx_dim_agency_name (agency_name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_service_type;
CREATE TABLE dim_service_type (
  service_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  service_type VARCHAR(16) NOT NULL,
  UNIQUE KEY uq_dim_service_type (service_type)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
  location_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  stop_name VARCHAR(128) NOT NULL,
  UNIQUE KEY uq_dim_stop_name (stop_name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
  time_sk INT AUTO_INCREMENT PRIMARY KEY,
  time_value TIME NOT NULL,
  hour TINYINT NOT NULL,
  minute TINYINT NOT NULL,
  second TINYINT NOT NULL,
  UNIQUE KEY uq_dim_time_value (time_value)
) ENGINE=InnoDB;

/* =========================================================
   5) FACT TABLE (dernier état / upsert)
   ========================================================= */
DROP TABLE IF EXISTS fact_trip_summary;

CREATE TABLE fact_trip_summary (
  fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,

  dataset_sk BIGINT NOT NULL,
  trip_sk BIGINT NOT NULL,
  route_sk BIGINT NOT NULL,
  agency_sk BIGINT NOT NULL,
  service_sk BIGINT NOT NULL,

  origin_location_sk BIGINT NOT NULL,
  destination_location_sk BIGINT NOT NULL,

  departure_time_sk INT NOT NULL,
  arrival_time_sk INT NOT NULL,

  distance_km DECIMAL(10,3) NULL,
  duration_h DECIMAL(6,2) NULL,

  -- Optionnel: trace du dernier lot ayant mis à jour ce trip
  last_load_id BIGINT NULL,
  last_loaded_at DATETIME NULL,

  CONSTRAINT fk_fact_dataset FOREIGN KEY (dataset_sk) REFERENCES dim_dataset(dataset_sk),
  CONSTRAINT fk_fact_trip FOREIGN KEY (trip_sk) REFERENCES dim_trip(trip_sk),
  CONSTRAINT fk_fact_route FOREIGN KEY (route_sk) REFERENCES dim_route(route_sk),
  CONSTRAINT fk_fact_agency FOREIGN KEY (agency_sk) REFERENCES dim_agency(agency_sk),
  CONSTRAINT fk_fact_service FOREIGN KEY (service_sk) REFERENCES dim_service_type(service_sk),
  CONSTRAINT fk_fact_origin_loc FOREIGN KEY (origin_location_sk) REFERENCES dim_location(location_sk),
  CONSTRAINT fk_fact_dest_loc FOREIGN KEY (destination_location_sk) REFERENCES dim_location(location_sk),
  CONSTRAINT fk_fact_dep_time FOREIGN KEY (departure_time_sk) REFERENCES dim_time(time_sk),
  CONSTRAINT fk_fact_arr_time FOREIGN KEY (arrival_time_sk) REFERENCES dim_time(time_sk),

  -- 1 ligne de fait par trip (dernier état)
  UNIQUE KEY uq_fact_trip (trip_sk),

  KEY idx_fact_route (route_sk),
  KEY idx_fact_agency (agency_sk),
  KEY idx_fact_origin (origin_location_sk),
  KEY idx_fact_dest (destination_location_sk)
) ENGINE=InnoDB;