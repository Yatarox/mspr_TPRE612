# API Microservice - Rail DW

## Overview

API REST pour accéder aux données du Data Warehouse Rail. Fournit des endpoints pour consulter les dashboards et les données agrégées des trajets ferroviaires.

## Structure du projet

```
api/
├── src/
│   ├── api/
│   │   ├── __init__.py
│   │   └── routes/
│   │       ├── __init__.py
│   │       └── dashboard.py          # Endpoints REST
│   ├── models/
│   │   ├── __init__.py
│   │   └── database.py               # Gestion connexion BD
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── dashboard.py              # Schémas Pydantic
│   ├── services/
│   │   ├── __init__.py
│   │   └── dashboard_service.py      # Logique métier
│   └── main.py                       # Point d'entrée FastAPI
├── tests/
│   ├── test_dashboard_schema.py
│   ├── test_dashboard_service.py
│   └── test_main.py
├── Dockerfile
├── requirements.txt
└── README.md
```

## 📋 Fichiers et responsabilités

### **main.py**
Point d'entrée de l'API FastAPI. Configure :
- Lifecycle de l'application (init/close pool DB)
- Middlewares CORS
- Inclusion des routes
- Endpoints de base (`/` et `/health`)

### **models/database.py**
Gestion de la connexion à la base de données :
- Configuration MySQL via variables d'environnement
- Pool de connexions asynchrone (`aiomysql`)
- Fonction `execute_query()` pour les requêtes SQL

### **schemas/dashboard.py**
Schémas de validation Pydantic :
- `DashboardMetric` : modèle pour une métrique
- `DashboardCreate` : validation des créations
- `DashboardUpdate` : validation des mises à jour
- `DashboardResponse` : format de réponse API

### **services/dashboard_service.py**
Logique métier (Business Logic) :
- `get_overview()` : KPIs globaux (trajets, routes, agences, distances, émissions)
- `get_stats_by_country()` : statistiques par pays d'origine
- `get_stats_by_train_type()` : statistiques par type de train
- `get_stats_by_traction()` : statistiques par type de traction
- `get_stats_by_agency()` : top agences par nombre de trajets
- `get_emissions_by_route()` : routes les plus émettrices
- `search_trips()` : recherche de trajets avec filtres (origine, destination, type, distance)
- `get_health()` : vérification de la santé de l'API

### **api/routes/dashboard.py**
Endpoints REST exposant les services :
- `GET /api/stats/overview` : KPIs généraux
- `GET /api/stats/by-country` : données par pays
- `GET /api/stats/by-train-type` : données par type de train
- `GET /api/stats/by-traction` : données par traction
- `GET /api/stats/by-agency` : top agences (limit paramétrable)
- `GET /api/emissions/by-route` : routes émettrices (limit paramétrable)
- `GET /api/trips/search` : recherche avec filtres (origin, destination, train_type, min/max_distance, limit)

## 🔄 Flux de requête

```
Client HTTP
    ↓
routes/dashboard.py (endpoint)
    ↓
services/dashboard_service.py (logique métier)
    ↓
models/database.py (requête SQL)
    ↓
MySQL (rail_dw)
    ↓
[réponse inverse]
```

## Installation

### Prérequis
- Python 3.9+
- MySQL 8.0+

### Setup

```bash
pip install -r requirements.txt
```

### Variables d'environnement

Créer un fichier `.env` :
```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_DATABASE=rail_dw
```

## Démarrage

```bash
python src/main.py
```

L'API sera accessible sur `http://localhost:8000`

## Tests

```bash
pytest tests/
```

## Documentation

Swagger UI : `http://localhost:8000/docs`
ReDoc : `http://localhost:8000/redoc`

## Docker

```bash
docker build -t rail-api .
docker run -d -p 8000:8000 --env-file .env rail-api
```