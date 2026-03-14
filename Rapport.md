# Rapport Technique – Projet MSPR TPRE612  
## Plateforme d’Analyse des Données Ferroviaires Européennes  
**Statut : Version Finale (MSPR 1)**

---

## 1. Introduction

Ce projet vise à concevoir une chaîne de traitement de données **de bout en bout** pour des données de transport au format **GTFS** :  
**collecte (Extract) → transformation (Transform) → chargement (Load) → exposition API → visualisation dashboard**.

liens vers le repo: https://github.com/Yatarox/mspr_TPRE612

Objectif métier : fournir à l’observatoire **ObRail Europe** une base analytique fiable pour suivre :
- les trajets,
- les distances,
- les durées,
- les fréquences de service,
- les émissions de CO₂.

---

## 2. Justification des sources de données

### 2.1 Critères de sélection

Les sources ont été retenues selon :
- disponibilité en **Open Data**,
- présence d’un format **GTFS** exploitable,
- couverture géographique multi-pays,
- fréquence de mise à jour,
- fiabilité institutionnelle (sources officielles privilégiées).

### 2.2 Sources utilisées

- **France (SNCF / Intercités / régional)** : data.gouv.fr  
- **Belgique (NMBS/SNCB)** : irail.be  
- **Suisse (CFF/SBB)** : opentransportdata.swiss  
- **Allemagne (DING)** : nvbw.de  
- **Italie (Toscane)** : dati.toscana.it  
- **Portugal (Transtejo)** : api.transtejo.pt

### 2.3 Limites observées sur les sources

- certains endpoints deviennent obsolètes (erreurs 404),
- qualité hétérogène selon les opérateurs,
- volumes très différents (de quelques milliers à plusieurs millions de trajets).

---

## 3. Choix techniques

### 3.1 Stack retenue

- **Python 3.12** : scripts ETL 
- **Apache Airflow 3** : orchestration planifiée des workflows
- **MySQL 8** : Data Warehouse relationnel
- **FastAPI** : exposition REST des données analytiques
- **React + Vite** : dashboard
- **Docker / Docker Compose** : reproductibilité environnementale

### 3.2 Architecture des services

4 conteneurs principaux :
- `airflowservice` (port 8080)
- `mysql` (port 3306)
- api (port 8000)
- dashboard (port 3000)

Communication interne via réseau Docker dédié.

### 3.3 Justification d’architecture

- séparation claire des responsabilités,
- maintenance facilitée,
- montée en charge progressive service par service,
- exécution identique en local et en environnement cible.

---

## 4. Pipeline ETL

## 4.1 Extract

### Fonctionnement
- récupération des ZIP GTFS via URL,
- extraction et dépôt en **staging** par dataset/version,
- journalisation de chaque téléchargement.

### Difficultés rencontrées
- URLs invalides (404),
- concaténation involontaire d’URLs (erreur de liste),
- variables Airflow en cache empêchant la prise en compte des corrections.

### Correctifs appliqués
- validation systématique des URLs,
- correction des définitions de listes d’URLs,
- purge des variables Airflow obsolètes puis redéploiement.

---

## 4.2 Transform

### Objectif
Convertir les fichiers GTFS bruts (`stops.txt`, `trips.txt`, `routes.txt`, `stop_times.txt`, `agency.txt`, `calendar.txt`) en un format analytique standardisé (`trips_summary`).

### Enrichissements réalisés
- calcul de distance (`distance_km`),
- calcul de durée (`duration_h`),
- estimation CO₂ (`emission_gco2e_pkm`, `total_emission_kgco2e`),
- détermination pays origine/destination,
- calcul fréquence de service hebdomadaire.

### Règles de nettoyage
- suppression des `trip_id` vides,
- suppression des durées invalides (`<= 0`),
- suppression des distances négatives,
- dédoublonnage des `trip_id`,
- normalisation des noms de colonnes (`strip`),
- exclusion des lignes incohérentes de progression de distance.

### Gestion des gros volumes
- limitation du parallélisme (`max_workers` réduit),
- fallback séquentiel en cas de `BrokenProcessPool`,
- exclusion conditionnelle des datasets extrêmes (> 1 000 000 trips) pour éviter OOM.

---

## 4.3 Load

### Objectif
Charger les sorties transformées dans MySQL.

### Méthode
- insertion par lots (chunks),
- table(s) de staging puis intégration analytique,
- indexation des colonnes de filtre fréquentes.

### Bénéfices
- limitation des timeouts,
- meilleure traçabilité des chargements,
- reprise plus simple en cas d’échec partiel.

---

## 5. Modèle de données (partie synthétique MCD/MLD)

> TODO METTRE LE MCD MLD

Le modèle suit une logique **Data Warehouse en étoile** :

- **Table de faits** : `fact_trip_summary`
  - mesures : distance, durée, émissions, fréquence.
- **Dimensions principales** :
  - opérateur (`dim_agency`),
  - type de train (`dim_train_type`),
  - pays (`dim_country`),
  - localisation (`dim_location`),
  - temps (heures départ/arrivée),
  - dataset/source.

Cette structure facilite les agrégations (par pays, opérateur, type de train, période).

---

## 6. API REST

### 6.1 Endpoints principaux

- `GET /health` : état de santé
- `GET /trips` : liste paginée des trajets
- `GET /trips/{trip_id}` : détail trajet
- `GET /stats/emissions` : agrégats CO₂
- `GET /stats/by-country` : indicateurs par pays
- `GET /stats/by-agency` : indicateurs par opérateur

### 6.2 Documentation et validation

- documentation auto via Swagger (`/docs`),
- tests manuels des cas nominal/erreur :
  - filtres,
  - pagination,
  - paramètres invalides (400),
  - ressources non trouvées (404).

---

## 7. Résultats obtenus

Indicateurs consolidés après traitements réussis :
- **trajets traités** : 929 710
- **distance totale** : ~178 millions km
- **itinéraires distincts** : 3 184
- **émissions consolidées** : ~411 114 kg CO₂e
- **durée moyenne** : 0,87 h

Le pipeline est opérationnel et industrialisable sur un périmètre multi-pays.

---

## 8. Difficultés rencontrées

- compréhension initiale du schéma GTFS,
- hétérogénéité qualité des sources,
- endpoints indisponibles (404),
- colonnes mal nommées dans certains flux,
- calculs métiers non fournis nativement par GTFS,
- OOM sur datasets volumineux (Suisse),
- variables Airflow non rafraîchies automatiquement,
- ajustement mémoire Docker/MySQL nécessaire.

---

## 9. Sécurité, exploitation et fiabilité

- secrets gérés par variables d’environnement,
- isolation réseau via Docker,
- logs Airflow et applicatifs pour audit,
- stratégie de reprise :
  - retries Airflow,
  - fallback séquentiel,
  - skip des datasets déjà traités.

---

## 10. Pistes d’amélioration (MSPR 2)

- traitement streaming/chunks avancé pour très gros datasets,
- raffinement CO₂ selon mix énergétique national et traction,
- extension API (correspondances, itinéraires multi-segments),
- automatisation CI/CD et déploiement serveur/VM,
- renforcement de la couverture de tests unitaires et d’intégration,
- ajout de nouveaux pays/opérateurs.

---
