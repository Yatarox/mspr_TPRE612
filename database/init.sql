# Dim_Station (Pour stocker les gares de manière unique)
CREATE TABLE Dim_Station (
    station_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    country_code VARCHAR(10) NOT NULL
);


# Dim_Agency (Pour stocker les opérateurs : SNCF, ÖBB, etc.)
CREATE TABLE Dim_Agency (
    agency_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE
);


# Dim_TrainType (Pour stocker les types de train : Intercité, Régional, etc.)
CREATE TABLE Dim_TrainType (
    train_type_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE
);


# Dim_ServiceType (Crucial pour l'objectif : distinguer Jour/Nuit )
CREATE TABLE Dim_ServiceType (
    service_type_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);


# Dim_Traction (Pour stocker les types de traction)
CREATE TABLE Dim_Traction (
    traction_id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
);


# Fact_Trip (C'est ici que toutes les données de la page 7 seront stockées, en utilisant les ID des tables de dimensions)
CREATE TABLE Fact_Trip (
    -- Clé primaire (on peut garder l'ID textuel du fichier source)
    trip_id VARCHAR(50) PRIMARY KEY, 
    
    route_name VARCHAR(255),

    -- Clés étrangères vers les dimensions
    agency_id INT REFERENCES Dim_Agency(agency_id),
    train_type_id INT REFERENCES Dim_TrainType(train_type_id),
    service_type_id INT REFERENCES Dim_ServiceType(service_type_id),
    origin_station_id INT REFERENCES Dim_Station(station_id),
    destination_station_id INT REFERENCES Dim_Station(station_id),
    traction_id INT REFERENCES Dim_Traction(traction_id),

    -- Mesures et données spécifiques au trajet
    departure_time TIME,
    arrival_time TIME,
    distance_km NUMERIC(10, 2),
    duration_h NUMERIC(5, 2),
    emission_gco2e_pkm NUMERIC(6, 2),
    total_emission_kgco2e NUMERIC(10, 2),
    frequency_per_week INT,
    source_dataset VARCHAR(100) -- D'où vient la donnée, ex: 'Back-on-Track'
);