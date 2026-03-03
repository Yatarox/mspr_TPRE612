# MSPR_TPRE612 - Rail Data Platform

Plateforme de traitement et de visualisation de données ferroviaires (GTFS), organisée en microservices :

- **database** : schéma Data Warehouse MySQL + données de référence
- **etl** : pipeline Extract / Transform / Load orchestré par Airflow
- **api** : API FastAPI pour exposer les statistiques
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

- Docker Desktop
- Docker Compose
- (Optionnel local) Python 3.9+, Node.js 18+, MySQL 8

---

## Démarrage rapide (Docker)

Depuis la racine du projet :

```powershell
docker compose up -d --build
```

Vérifier l’état :

```powershell
docker compose ps
```

Suivre les logs d’un service :

```powershell
docker logs -f rail-mysql
docker logs -f rail-api
docker logs -f rail-airflow
docker logs -f rail-dashboard
```

Arrêt :

```powershell
docker compose down
```

Arrêt + reset volumes :

```powershell
docker compose down -v
```

---

## Microservices

## 1) Database (`database/`)
- Initialise la base `rail_dw`
- Crée les dimensions/faits/staging
- Charge les pays de référence (`init_countries.sql`)

## 2) ETL (`etl/`)
- **Extract** : téléchargement GTFS (API + ZIP)
- **Transform** : consolidation en `trips_summary_*.csv`
- **Load** : insertion staging + dimensions + faits dans MySQL
- Orchestration via DAG Airflow

## 3) API (`api/`)
- FastAPI
- Endpoints de stats (`/api/stats/...`) + recherche (`/api/trips/search`)
- Healthcheck : `/health`
- Swagger : `/docs`

## 4) Frontend (`rail-dashboard/`)
- React + Vite
- Dashboard de visualisation des métriques et recherches

---

## Variables de configuration

Les services utilisent des variables d’environnement (exemples) :
- MySQL : `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- API : connexion BD + port
- ETL/Airflow : dossiers GTFS, batch size, workers, URLs sources

> Utiliser un fichier `.env` à la racine si prévu par `docker-compose.yml`.

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

- Si `rail-mysql is unhealthy` :
  - vérifier les logs : `docker logs rail-mysql`
  - vérifier conflit port 3306
  - vérifier les variables MySQL invalides dans la config  
    (exemple connu : utiliser `innodb_lock_wait_timeout`, pas `innodb_lock_waits_timeout`)

---

## Statut

Projet en évolution : certaines parties (notamment frontend) sont amenées à être refactorisées.