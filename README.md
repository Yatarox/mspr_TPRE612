# MSPR_TPRE612 - Rail Data Platform

Plateforme de collecte, transformation et visualisation de données ferroviaires (GTFS), organisée en microservices :

- **database** : schéma Data Warehouse MySQL + données de référence
- **etl** : pipeline Extract / Transform / Load orchestré par Airflow
- **api** : API FastAPI pour exposer les données et statistiques
- **rail-dashboard** : frontend React pour la visualisation

---

## Architecture

```text
GTFS Sources
   │
   ▼
[ETL / Airflow]
Extract -> Transform -> Load
   │
   ▼
[MySQL Data Warehouse]
   │
   ▼
[FastAPI]
   │
   ▼
[React Dashboard]
```

---

## Structure du repository

```text
mspr_TPRE612/
├── database/               # SQL schema + init dim_country
├── etl/                    # DAGs Airflow + scripts ETL
├── api/                    # FastAPI (routes/services/models/schemas)
├── rail-dashboard/         # Frontend React
└── docker-compose.yml      # Orchestration conteneurs
```

---

## Prérequis

- Docker Desktop (Windows)
- Docker Compose

> Optionnel (hors Docker) : Python 3.12+, Node.js 18+, MySQL 8

---

## Démarrage rapide (Docker)

Depuis la racine du projet :

```powershell
docker compose up -d --build
```

Vérifier l’état des services :

```powershell
docker compose ps
```

Suivre les logs :

```powershell
docker logs -f rail-mysql
docker logs -f rail-api
docker logs -f rail-airflow
docker logs -f rail-dashboard
```

Arrêter :

```powershell
docker compose down
```

Arrêter + reset volumes :

```powershell
docker compose down -v
```

---

## Accès aux services

- Airflow : `http://localhost:8080`
- API : `http://localhost:8000`
- Swagger API : `http://localhost:8000/docs`
- Dashboard : `http://localhost:3000`

---

## Microservices

### 1) Database (`database/`)
- Initialise la base `rail_dw`
- Crée tables staging, dimensions et faits
- Charge les pays de référence (`init_countries.sql`)

### 2) ETL (`etl/`)
- **Extract** : téléchargement GTFS (API + ZIP)
- **Transform** : consolidation en `trips_summary_*.csv`
- **Load** : insertion staging + dimensions + faits dans MySQL
- Orchestration via DAG Airflow

### 3) API (`api/`)
- FastAPI
- Endpoints documentés via Swagger (`/docs`)
- Healthcheck : `/health`

### 4) Frontend (`rail-dashboard/`)
- React + Vite
- Visualisation des indicateurs et recherche de trajets

---

## Configuration (variables d’environnement)

Exemples de variables utilisées :

- **MySQL** : `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- **API** : variables de connexion BD + port
- **ETL/Airflow** : chemins GTFS, batch size, workers, URLs sources

> Utiliser un `.env` à la racine si le `docker-compose.yml` le prévoit.

---

## Tests

### API

```powershell
cd api
pytest tests/
```

### Frontend

```powershell
cd rail-dashboard
npm install
npm run dev
```

---

## Dépannage rapide

- Si `rail-mysql` est `unhealthy` :
  - vérifier les logs : `docker logs rail-mysql`
  - vérifier un conflit sur le port `3306`
  - vérifier les variables MySQL (ex: `innodb_lock_wait_timeout`)

- Si Airflow n’applique pas les nouvelles URLs GTFS :
  - supprimer les variables Airflow concernées puis redémarrer le service

```powershell
docker exec -it rail-airflow airflow variables delete gtfs_zip_urls
docker exec -it rail-airflow airflow variables delete gtfs_base_urls
docker compose restart airflowservice
```

---

## Statut

Projet en évolution. Des refactorings restent prévus (ETL volumineux, robustesse API, optimisation frontend).