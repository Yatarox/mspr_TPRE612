# Database Microservice

## Overview

Ce microservice gère la base de données Data Warehouse pour le projet MSPR. Il définit le schéma complet avec dimensions, faits et tables de staging pour l'ETL.

## Architecture

### Structure des tables

#### Dimensions (dim_*)
- **dim_dataset** : Identifiants des datasets
- **dim_trip** : Trajets ferroviaires
- **dim_route** : Routes/lignes de transport
- **dim_agency** : Agences de transport
- **dim_service_type** : Types de services
- **dim_train_type** : Types de trains
- **dim_traction** : Types de traction
- **dim_country** : Pays européens (26 pays)
- **dim_location** : Localités avec référence pays
- **dim_time** : Heures au format HH:MM:SS

#### Fact Table
- **fact_trip_summary** : Données agrégées des trajets avec KPIs (distance, durée, émissions CO2, fréquence)

#### Staging
- **stg_trips_summary** : Table intermédiaire pour l'ETL

## Installation

### Prérequis
- MySQL 8.0+
- UTF-8MB4 support

### Initialisation

1. Créer le schéma :
```bash
mysql -u root -p < schema.sql
```

2. Charger les données de pays :
```bash
mysql -u root -p rail_dw < init_countries.sql
```

## Docker

```bash
docker build -t rail-db .
docker run -d -p 3306:3306 --name rail-db rail-db
```

## Données de référence

### Countries ([init_countries.sql](init_countries.sql))
26 pays européens pré-intégrés dans `dim_country` :
- France, Germany, Italy, Spain, etc.

## Notes

- Charset : `utf8mb4_0900_ai_ci` (support unicode complet, accent-insensitive)
- Engine : InnoDB (transactions, contraintes FK)
- Les clés étrangères assurent l'intégrité référentielle