# ETL GTFS avec Airflow

## Description
Pipeline ETL pour extraire, transformer et charger des données GTFS (General Transit Feed Specification) avec Apache Airflow.

## Prérequis
- Docker
- Docker Compose

## Structure du projet
```
etl/
├── dags/
│   └── base.py              # DAG principal gtfs_full_etl
├── scripts/
│   ├── extract_gtfs_data_gouv_script.py
│   ├── transform_gtfs_data.py
│   └── load_gtfs.py
├── data/
│   ├── raw/                 # Données brutes téléchargées
│   ├── staging/             # Données dézippées
│   └── processed/           # Données transformées
├── logs/                    # Logs Airflow
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Installation et Lancement

### 1. Cloner le projet
```bash
cd c:\Users\"CHANGME"\Desktop\ecole\mspr_TPRE612\etl
```

### 2. Construire et lancer le conteneur
```bash
docker-compose up -d
```

### 3. Accéder à l'interface Airflow
- URL: http://localhost:8080
- Username: `admin`
- Password: 'voir dans les logs'

cmd pour trouver le mdp facilement
```bash
docker exec etl-airflowservice-1 cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

### 4. Le DAG démarre automatiquement
Le DAG `gtfs_full_etl` se lance automatiquement au démarrage avec `schedule="@once"`.

## Arrêter le conteneur
```bash
docker-compose down
```

## Redémarrer après modifications
```bash
docker-compose down
docker-compose up -d --build
```

## Configuration (Variables Airflow)

Vous pouvez configurer via l'interface Airflow (Admin > Variables) :

| Variable | Défaut | Description |
|----------|--------|-------------|
| `gtfs_base_urls` | URLs par défaut | Liste des URLs d'API (JSON ou CSV) |
| `gtfs_raw_dir` | `/opt/airflow/data/raw` | Répertoire données brutes |
| `gtfs_staging_dir` | `/opt/airflow/data/staging` | Répertoire staging |
| `gtfs_processed_dir` | `/opt/airflow/data/processed` | Répertoire données transformées |
| `gtfs_db_conn_id` | None | Connexion DB pour le load |

## Logs et Debugging

### Voir les logs du conteneur
```bash
docker-compose logs -f airflowservice
```

### Voir les logs d'une tâche
Les logs sont accessibles via l'interface Airflow ou dans `./logs/`


### Relancer manuellement le DAG
Via l'interface Airflow, cliquez sur le bouton "Play" à droite du DAG.

### result dans 
le dossier data/processed