DROP DATABASE IF EXISTS rail_dw;
CREATE DATABASE rail_dw
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_0900_ai_ci;
USE rail_dw;

-- =========================================================
-- Dimensions
-- =========================================================
DROP TABLE IF EXISTS dim_dataset;
CREATE TABLE dim_dataset (
  dataset_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  dataset_id VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_dataset_id (dataset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_shape;
CREATE TABLE dim_shape (
  shape_sk  BIGINT AUTO_INCREMENT PRIMARY KEY,
  shape_id  VARCHAR(255) NOT NULL,
  UNIQUE KEY uq_dim_shape_id (shape_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_shape_point;
CREATE TABLE dim_shape_point (
  point_sk    BIGINT AUTO_INCREMENT PRIMARY KEY,
  shape_sk    BIGINT NOT NULL,
  pt_sequence INT NOT NULL,
  lat         DECIMAL(9,6) NOT NULL,
  lon         DECIMAL(9,6) NOT NULL,
  KEY idx_shape_point_shape (shape_sk),
  CONSTRAINT fk_shape_point_shape
    FOREIGN KEY (shape_sk) REFERENCES dim_shape(shape_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_trip;
CREATE TABLE dim_trip (
  trip_sk  BIGINT AUTO_INCREMENT PRIMARY KEY,
  trip_id  VARCHAR(255) NOT NULL,
  shape_sk BIGINT NULL,
  UNIQUE KEY uq_dim_trip_id (trip_id),
  CONSTRAINT fk_trip_shape
    FOREIGN KEY (shape_sk) REFERENCES dim_shape(shape_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_route;
CREATE TABLE dim_route (
  route_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  route_id VARCHAR(255) NOT NULL,
  route_name VARCHAR(255),
  UNIQUE KEY uq_dim_route_id (route_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_agency;
CREATE TABLE dim_agency (
  agency_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  agency_id VARCHAR(64) NOT NULL,
  agency_name VARCHAR(255),
  UNIQUE KEY uq_dim_agency_id (agency_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_service_type;
CREATE TABLE dim_service_type (
  service_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  service_type VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_dim_service_type (service_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_train_type;
CREATE TABLE dim_train_type (
  train_type_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  train_type VARCHAR(128) NOT NULL,
  UNIQUE KEY uq_dim_train_type (train_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_traction;
CREATE TABLE dim_traction (
  traction_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  traction VARCHAR(64) NOT NULL,
  UNIQUE KEY uq_dim_traction (traction)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_country;
CREATE TABLE dim_country (
  country_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  country_code VARCHAR(10) NOT NULL,
  country_name VARCHAR(128),
  UNIQUE KEY uq_dim_country_code (country_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_location;
CREATE TABLE dim_location (
  location_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  stop_name VARCHAR(255) NOT NULL,
  country_code VARCHAR(10),
  UNIQUE KEY uq_dim_location_name_country (stop_name, country_code),
  KEY idx_dim_location_country (country_code),
  CONSTRAINT fk_dim_location_country
    FOREIGN KEY (country_code) REFERENCES dim_country(country_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

DROP TABLE IF EXISTS dim_time;
CREATE TABLE dim_time (
  time_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  time_value VARCHAR(16) NOT NULL,
  hour TINYINT,
  minute TINYINT,
  second TINYINT,
  UNIQUE KEY uq_dim_time_value (time_value)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- Fact
-- =========================================================
DROP TABLE IF EXISTS fact_trip_summary;
CREATE TABLE fact_trip_summary (
  fact_sk BIGINT AUTO_INCREMENT PRIMARY KEY,
  trip_sk BIGINT NOT NULL,
  dataset_sk BIGINT NOT NULL,
  route_sk BIGINT NOT NULL,
  agency_sk BIGINT NOT NULL,
  service_sk BIGINT NOT NULL,
  train_type_sk BIGINT,
  traction_sk BIGINT,
  origin_location_sk BIGINT,
  origin_country_sk BIGINT,
  destination_location_sk BIGINT,
  destination_country_sk BIGINT,
  departure_time_sk BIGINT,
  arrival_time_sk BIGINT,
  distance_km DECIMAL(10,3),
  duration_h DECIMAL(10,3),
  emission_gco2e_pkm DECIMAL(10,3),
  total_emission_kgco2e DECIMAL(10,3),
  frequency_per_week INT,
  last_load_id BIGINT,
  last_loaded_at DATETIME,

  UNIQUE KEY uq_fact_trip_dataset (trip_sk, dataset_sk),
  KEY idx_fact_route (route_sk),
  KEY idx_fact_agency (agency_sk),
  KEY idx_fact_last_load (last_load_id),

  CONSTRAINT fk_fact_trip        FOREIGN KEY (trip_sk) REFERENCES dim_trip(trip_sk),
  CONSTRAINT fk_fact_dataset     FOREIGN KEY (dataset_sk) REFERENCES dim_dataset(dataset_sk),
  CONSTRAINT fk_fact_route       FOREIGN KEY (route_sk) REFERENCES dim_route(route_sk),
  CONSTRAINT fk_fact_agency      FOREIGN KEY (agency_sk) REFERENCES dim_agency(agency_sk),
  CONSTRAINT fk_fact_service     FOREIGN KEY (service_sk) REFERENCES dim_service_type(service_sk),
  CONSTRAINT fk_fact_train       FOREIGN KEY (train_type_sk) REFERENCES dim_train_type(train_type_sk),
  CONSTRAINT fk_fact_traction    FOREIGN KEY (traction_sk) REFERENCES dim_traction(traction_sk),
  CONSTRAINT fk_fact_origin_loc  FOREIGN KEY (origin_location_sk) REFERENCES dim_location(location_sk),
  CONSTRAINT fk_fact_dest_loc    FOREIGN KEY (destination_location_sk) REFERENCES dim_location(location_sk),
  CONSTRAINT fk_fact_origin_cty  FOREIGN KEY (origin_country_sk) REFERENCES dim_country(country_sk),
  CONSTRAINT fk_fact_dest_cty    FOREIGN KEY (destination_country_sk) REFERENCES dim_country(country_sk),
  CONSTRAINT fk_fact_dep_time    FOREIGN KEY (departure_time_sk) REFERENCES dim_time(time_sk),
  CONSTRAINT fk_fact_arr_time    FOREIGN KEY (arrival_time_sk) REFERENCES dim_time(time_sk)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =========================================================
-- Staging
-- =========================================================
DROP TABLE IF EXISTS stg_trips_summary;
CREATE TABLE stg_trips_summary (
  stg_id BIGINT AUTO_INCREMENT PRIMARY KEY,
  load_id BIGINT NOT NULL,
  loaded_at DATETIME NOT NULL,
  dataset_id VARCHAR(255),
  trip_id VARCHAR(255),
  route_id VARCHAR(64),
  route_name VARCHAR(255),
  agency_id VARCHAR(64),
  agency_name VARCHAR(255),
  service_type VARCHAR(64),
  origin_stop_name VARCHAR(255),
  origin_country VARCHAR(30),
  destination_stop_name VARCHAR(255),
  destination_country VARCHAR(30),
  departure_time VARCHAR(16),
  arrival_time VARCHAR(16),
  distance_km DECIMAL(10,3),
  duration_h DECIMAL(10,3),
  train_type VARCHAR(128),
  traction VARCHAR(64),
  transport_type VARCHAR(64),
  emission_gco2e_pkm DECIMAL(10,3),
  total_emission_kgco2e DECIMAL(10,3),
  frequency_per_week INT,

  KEY idx_stg_load (load_id),
  KEY idx_stg_load_stgid (load_id, stg_id),
  KEY idx_stg_load_trip (load_id, trip_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;