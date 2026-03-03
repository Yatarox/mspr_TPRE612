# ETL Microservice - Rail DW

## Overview

Ce microservice exécute un pipeline **ETL GTFS** :
1. **Extract** : téléchargement des jeux GTFS (API data.gouv + URLs ZIP directes)
2. **Transform** : génération de fichiers `trips_summary_*.csv`
3. **Load** : chargement en base MySQL (staging + dimensions + table de faits)

L’orchestration est faite avec **Apache Airflow**.

---

## Structure du projet

```text
etl/
├── dags/
│   └── base.py                         # DAG principal gtfs_full_etl
├── scripts/
│   ├── extract_gtfs_data_gouv_script.py
│   ├── transform_gtfs_data.py
│   ├── load_gtfs.py
│   ├── extract_script/                 # Fonctions extract (API, download, unzip, metadata…)
│   ├── transform_script/               # Helpers transform (geo, time, fréquence, processing…)
│   └── load_script/                    # Helpers load (staging, fact loader, cache dimensions…)
└── README.md
```

---

## Fichiers principaux

### `dags/base.py`
Définit le DAG `gtfs_full_etl` avec les tâches :
- `extract()`
- `transform()`
- `load()`
- `pipeline_summary()`
- `final_cleanup()`

Gère aussi :
- Variables Airflow (URLs, chemins, batch size, workers, etc.)
- Logs structurés (métriques, événements, erreurs)

---

### `scripts/transform_gtfs_data.py`
Transforme les fichiers GTFS (`agency.txt`, `routes.txt`, `trips.txt`, `stop_times.txt`, etc.) en CSV consolidés :
- calcule distances / durées
- enrichit avec pays d’origine/destination
- calcule fréquences hebdo
- écrit un `trips_summary_<dataset_id>.csv`

Fonctions clés :
- `build_trips_summary_for_dataset(...)`
- `_write_csv(...)`
- `transform_gtfs(...)`

---

### `scripts/load_gtfs.py`
Charge les CSV transformés en base :
- insertion en **staging**
- chargement **set-based** vers dimensions + table de faits
- gestion par batch pour limiter les verrous MySQL

Fonction clé :
- `load_gtfs(processed_dir, conn_id="mysql_default", batch_size=1000)`

---

### `scripts/extract_gtfs_data_gouv_script.py`
Point d’entrée des utilitaires d’extraction GTFS (imports des modules extract) :
- build des URLs à télécharger
- download
- unzip
- nettoyage anciens fichiers

---

## Variables Airflow utilisées

Dans `base.py`, principales variables :
- `gtfs_base_urls`
- `gtfs_base_url` (fallback)
- `gtfs_zip_urls`
- `gtfs_raw_dir`
- `gtfs_staging_dir`
- `gtfs_processed_dir`
- `gtfs_db_conn_id`
- `gtfs_force_download`
- `gtfs_keep_latest_zips`
- `gtfs_max_workers`
- `gtfs_load_batch_size`

---

## Flux d’exécution

```text
Extract (ZIP/API) 
  -> Transform (trips_summary CSV) 
    -> Load (staging -> dimensions/fact) 
      -> Summary
```

---

## Lancement

### Avec Airflow
Le DAG `gtfs_full_etl` est planifié en `@daily` (voir `dags/base.py`).

### Exécution locale (scripts)
Selon ton environnement Python/Airflow, les fonctions appelables sont :
- `transform_gtfs(...)`
- `load_gtfs(...)`

---

## Notes techniques

- Transform optimisée pour gros volumes (chunks + split par agency)
- Chargement MySQL en batch (`batch_size`) pour réduire risques de lock
- Logging standard + logging structuré dans le DAG
- Encodage UTF-8 recommandé sur toutes les étapes

---

## Modules complémentaires

Le projet contient d’autres fonctions dans :
- `scripts/extract_script/*`
- `scripts/transform_script/*`
- `scripts/load_script/*`

Elles couvrent les helpers spécialisés (API, géo, fréquence, temps, staging/fact, cache dimensions).
Ce README documente le **fonctionnement global** et les **points d’entrée** principaux.