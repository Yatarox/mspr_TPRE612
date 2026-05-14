import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.models.xcom_arg import XComArg
from airflow.providers.mysql.hooks.mysql import MySqlHook

sys.path.append("/opt/airflow")

from scripts.extract_gtfs_data_gouv_script import (  # noqa: E402
    download_and_unzip_from_zip_urls,
    clean_old_downloads,
)
from scripts.load_gtfs import load_gtfs          # noqa: E402
from scripts.transform_gtfs_data import transform_gtfs  # noqa: E402
from scripts.train_model import train_model_pipeline    # noqa: E402

logger = logging.getLogger(__name__)


TEST_ZIP_URL = "https://www.data.gouv.fr/api/1/datasets/r/c0bd9ff1-97f9-43f8-aca4-8e80d7728324"

TEST_RAW_DIR       = "/opt/airflow/data/test/raw"
TEST_STAGING_DIR   = "/opt/airflow/data/test/staging"
TEST_PROCESSED_DIR = "/opt/airflow/data/test/processed"
TEST_DB_CONN_ID    = "mysql_test"


def clean_xcom(obj):
    if isinstance(obj, dict):
        return {k: clean_xcom(v) for k, v in obj.items() if not isinstance(v, XComArg)}
    elif isinstance(obj, list):
        return [clean_xcom(v) for v in obj if not isinstance(v, XComArg)]
    elif isinstance(obj, XComArg):
        return str(obj)
    return obj


default_args = {
    "owner": "airflow",
    "retries": 0,       
    "retry_delay": pendulum.duration(minutes=1),
}


@dag(
    dag_id="gtfs_test_pipeline",
    default_args=default_args,
    schedule=None,                      # jamais automatique
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    is_paused_upon_creation=True,       # paused par défaut
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "etl", "test"],
    description="Pipeline ETL de test — DB isolée, déclenchement manuel uniquement",
)
def gtfs_test_pipeline():

    # ── 0. Setup : nettoyage de la DB de test avant chaque run ───────────────

    @task
    def setup_test_db() -> Dict[str, Any]:
        """
        Truncate toutes les tables de la DB de test pour partir d'un état propre.
        Garantit l'isolation entre les runs de test.
        """
        hook = MySqlHook(mysql_conn_id=TEST_DB_CONN_ID)

        tables = [
            "fact_trip_summary",
            "stg_trips_summary",
            "dim_trip", "dim_route", "dim_agency", "dim_service_type",
            "dim_train_type", "dim_traction", "dim_country", "dim_location",
            "dim_time", "dim_dataset",
        ]

        hook.run("SET FOREIGN_KEY_CHECKS = 0")
        for table in tables:
            try:
                hook.run(f"TRUNCATE TABLE {table}")
                logger.info(f"✓ Truncated {table}")
            except Exception as e:
                logger.warning(f"Could not truncate {table}: {e}")
        hook.run("SET FOREIGN_KEY_CHECKS = 1")

        logger.info("✓ TEST DB setup complete — all tables empty")
        return {"setup": "ok", "timestamp": datetime.now().isoformat()}


    # ── 1. Extract ────────────────────────────────────────────────────────────

    @task
    def extract(setup_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Télécharge le fichier GTFS de test depuis l'URL fixe.
        Assertion : au moins 1 fichier téléchargé.
        """
        start_time = datetime.now()
        logger.info(f"[TEST] Extract started — URL: {TEST_ZIP_URL}")

        result = download_and_unzip_from_zip_urls(
            [TEST_ZIP_URL],
            TEST_RAW_DIR,
            TEST_STAGING_DIR,
            force_download=True,  # force en test pour toujours repartir de zéro
        )

        downloaded_count = len(result)
        duration = (datetime.now() - start_time).total_seconds()

        # ── Assertions extract ──
        assert downloaded_count >= 1, (
            f"[TEST FAIL] Extract: aucun fichier téléchargé depuis {TEST_ZIP_URL}"
        )

        import os
        assert os.path.exists(TEST_STAGING_DIR), (
            f"[TEST FAIL] Extract: staging_dir introuvable : {TEST_STAGING_DIR}"
        )

        staging_dirs = [
            d for d in os.listdir(TEST_STAGING_DIR)
            if os.path.isdir(os.path.join(TEST_STAGING_DIR, d))
        ]
        assert len(staging_dirs) >= 1, (
            "[TEST FAIL] Extract: aucun dataset dans le staging_dir"
        )

        logger.info(f"✓ [TEST] Extract OK — {downloaded_count} dataset(s), {duration:.2f}s")
        return {
            "downloaded": downloaded_count,
            "duration_seconds": duration,
            "success": True,
        }


    # ── 2. Transform ──────────────────────────────────────────────────────────

    @task
    def transform(extract_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforme les données GTFS extraites en CSV enrichis.
        Assertion : au moins 1 fichier CSV produit avec des lignes.
        """
        import os
        start_time = datetime.now()
        logger.info("[TEST] Transform started")

        result = transform_gtfs(
            TEST_STAGING_DIR,
            TEST_PROCESSED_DIR,
            max_workers=1,   # 1 worker en test pour éviter les conflits
        )

        duration = (datetime.now() - start_time).total_seconds()

        # ── Assertions transform ──
        assert len(result) >= 1, (
            "[TEST FAIL] Transform: aucun fichier CSV généré"
        )

        for csv_path in result:
            assert os.path.exists(csv_path), (
                f"[TEST FAIL] Transform: fichier CSV introuvable : {csv_path}"
            )
            size = os.path.getsize(csv_path)
            assert size > 0, (
                f"[TEST FAIL] Transform: fichier CSV vide : {csv_path}"
            )

        # Vérifie qu'au moins un CSV contient des données réelles
        import pandas as pd
        total_rows = 0
        for csv_path in result:
            try:
                df = pd.read_csv(csv_path)
                total_rows += len(df)
            except Exception:
                pass

        assert total_rows > 0, (
            "[TEST FAIL] Transform: tous les CSV sont vides"
        )

        logger.info(f"✓ [TEST] Transform OK — {len(result)} fichier(s), {total_rows} lignes, {duration:.2f}s")
        return {
            "files_generated": len(result),
            "files_list": result,
            "total_rows": total_rows,
            "duration_seconds": duration,
            "success": True,
        }


    # ── 3. Load ───────────────────────────────────────────────────────────────

    @task
    def load(transform_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Charge les données transformées dans la DB de test.
        Assertions : rows_loaded > 0, fact_trip_summary non vide.
        """
        start_time = datetime.now()
        logger.info("[TEST] Load started")

        assert transform_stats.get("files_list"), (
            "[TEST FAIL] Load: aucun fichier à charger depuis transform"
        )

        result = load_gtfs(
            TEST_PROCESSED_DIR,
            conn_id=TEST_DB_CONN_ID,
        )

        duration = (datetime.now() - start_time).total_seconds()
        rows_loaded = result.get("total_rows", 0) if isinstance(result, dict) else 0

        # ── Assertions load ──
        assert rows_loaded > 0, (
            f"[TEST FAIL] Load: aucune ligne chargée dans fact_trip_summary"
        )

        # Vérifie directement en DB
        hook = MySqlHook(mysql_conn_id=TEST_DB_CONN_ID)
        row = hook.get_first("SELECT COUNT(*) as cnt FROM fact_trip_summary")
        db_count = row[0] if row else 0

        assert db_count > 0, (
            "[TEST FAIL] Load: fact_trip_summary vide après le chargement"
        )

        assert db_count == rows_loaded, (
            f"[TEST FAIL] Load: incohérence — load_gtfs dit {rows_loaded} lignes "
            f"mais la DB en contient {db_count}"
        )

        logger.info(f"✓ [TEST] Load OK — {rows_loaded} lignes, {duration:.2f}s")
        return {
            "rows_loaded": rows_loaded,
            "db_count": db_count,
            "duration_seconds": duration,
            "success": True,
        }


    # ── 4. Train model ────────────────────────────────────────────────────────

    @task
    def train_model(load_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Entraîne le modèle ML sur les données chargées.
        Assertions : R² défini, MAE >= 0, fichier modèle créé.
        """
        import os
        start_time = datetime.now()
        logger.info("[TEST] Train model started")

        assert load_stats.get("rows_loaded", 0) > 0, (
            "[TEST FAIL] Train: impossible d'entraîner sans données chargées"
        )

        result = train_model_pipeline()
        result = clean_xcom(result)
        duration = (datetime.now() - start_time).total_seconds()

        # ── Assertions train ──
        assert result.get("r2") is not None, (
            "[TEST FAIL] Train: R² absent des métriques"
        )
        assert result.get("mae") is not None, (
            "[TEST FAIL] Train: MAE absent des métriques"
        )
        assert result["mae"] >= 0, (
            f"[TEST FAIL] Train: MAE négatif : {result['mae']}"
        )

        model_path = "/opt/airflow/models/frequency_model.joblib"
        assert os.path.exists(model_path), (
            f"[TEST FAIL] Train: fichier modèle introuvable après entraînement : {model_path}"
        )

        logger.info(
            f"✓ [TEST] Train OK — R²={result['r2']:.4f} "
            f"MAE={result['mae']:.2f} {duration:.2f}s"
        )
        return {
            "r2": result.get("r2"),
            "mae": result.get("mae"),
            "mae_pct": result.get("mae_pct"),
            "duration_seconds": duration,
            "success": True,
        }


    # ── 5. Validate API data ──────────────────────────────────────────────────

    @task
    def validate_api(load_stats: Dict[str, Any], train_stats: Dict[str, Any]) -> Dict[str, Any]:
        """
        Vérifie que l'API retourne bien les données chargées.
        Assertions : /health healthy, /api/stats/overview cohérent avec la DB,
        /api/predict retourne une prédiction.
        """
        import requests
        start_time = datetime.now()
        logger.info("[TEST] API validation started")

        api_base = "http://api:8000"   # nom du service Docker

        # Health check
        r = requests.get(f"{api_base}/health", timeout=10)
        assert r.status_code == 200, (
            f"[TEST FAIL] API /health retourne {r.status_code}"
        )
        health = r.json()
        assert health["status"] == "healthy", (
            f"[TEST FAIL] API /health status={health['status']}, error={health.get('error')}"
        )
        assert health["total_trips"] > 0, (
            "[TEST FAIL] API /health: total_trips=0 alors que des données ont été chargées"
        )

        # Overview cohérent avec ce qu'on a chargé
        r = requests.get(f"{api_base}/api/stats/overview", timeout=10)
        assert r.status_code == 200, (
            f"[TEST FAIL] API /api/stats/overview retourne {r.status_code}"
        )
        overview = r.json()
        assert overview.get("total_trips", 0) > 0, (
            "[TEST FAIL] API overview: total_trips=0"
        )

        # Cohérence overview ↔ load
        api_trips  = overview.get("total_trips", 0)
        load_trips = load_stats.get("rows_loaded", 0)
        assert api_trips <= load_trips, (
            f"[TEST FAIL] API overview incohérent: api={api_trips} > load={load_trips}"
        )

        # Predict fonctionne (modèle chargé)
        r = requests.get(f"{api_base}/api/predict", params={
            "distance_km": 450,
            "duration_h": 2.5,
            "train_type": "Grande vitesse",
            "traction": "Électrique",
        }, timeout=10)
        assert r.status_code == 200, (
            f"[TEST FAIL] API /api/predict retourne {r.status_code}"
        )
        pred = r.json()
        assert pred.get("warning") is None, (
            f"[TEST FAIL] API /api/predict warning: {pred.get('warning')}"
        )
        assert pred.get("emission_gco2e_pkm") is not None, (
            "[TEST FAIL] API /api/predict: emission_gco2e_pkm absent"
        )

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"✓ [TEST] API validation OK — "
            f"total_trips={api_trips}, predict OK, {duration:.2f}s"
        )
        return {
            "api_total_trips": api_trips,
            "predict_ok": True,
            "duration_seconds": duration,
            "success": True,
        }


    # ── 6. Test summary ───────────────────────────────────────────────────────

    @task
    def test_summary(
        setup_stats: Dict,
        extract_stats: Dict,
        transform_stats: Dict,
        load_stats: Dict,
        train_stats: Dict,
        api_stats: Dict,
    ) -> Dict[str, Any]:
        """
        Résumé final du test E2E.
        Fail explicitement si une étape a échoué.
        """
        stages = {
            "setup":     setup_stats,
            "extract":   extract_stats,
            "transform": transform_stats,
            "load":      load_stats,
            "train":     train_stats,
            "api":       api_stats,
        }

        failed = [
            name for name, stats in stages.items()
            if not stats.get("success", False)
        ]

        total_duration = sum(
            s.get("duration_seconds", 0) for s in stages.values()
            if isinstance(s, dict)
        )

        summary = {
            "dag": "gtfs_test_pipeline",
            "total_duration_seconds": total_duration,
            "stages": stages,
            "failed_stages": failed,
            "success": len(failed) == 0,
            "timestamp": datetime.now().isoformat(),
        }

        logger.info("=" * 60)
        logger.info("📊 TEST E2E SUMMARY")
        logger.info("=" * 60)
        logger.info(f"   Total Duration : {total_duration:.2f}s")
        logger.info(f"   Rows Loaded    : {load_stats.get('rows_loaded', 0)}")
        logger.info(f"   Files Generated: {transform_stats.get('files_generated', 0)}")
        logger.info(f"   API Trips      : {api_stats.get('api_total_trips', 0)}")
        logger.info(f"   Model R²       : {train_stats.get('r2', 'N/A')}")
        logger.info(f"   Failed Stages  : {failed or 'none'}")
        logger.info("=" * 60)

        assert len(failed) == 0, (
            f"[TEST FAIL] Les étapes suivantes ont échoué : {failed}"
        )

        logger.info("✅ ALL TESTS PASSED")
        return summary


    # ── 7. Teardown : nettoyage après le test ─────────────────────────────────

    @task
    def teardown_test_db(summary: Dict[str, Any]) -> str:
        """
        Nettoyage optionnel après le test.
        Truncate les tables pour ne pas polluer les runs suivants.
        """
        hook = MySqlHook(mysql_conn_id=TEST_DB_CONN_ID)
        tables = ["fact_trip_summary", "stg_trips_summary"]

        hook.run("SET FOREIGN_KEY_CHECKS = 0")
        for table in tables:
            try:
                hook.run(f"TRUNCATE TABLE {table}")
            except Exception as e:
                logger.warning(f"Teardown: could not truncate {table}: {e}")
        hook.run("SET FOREIGN_KEY_CHECKS = 1")

        logger.info("✓ [TEST] Teardown complete")
        return "teardown_ok"


    # ── Orchestration ─────────────────────────────────────────────────────────

    setup_result    = setup_test_db()
    extract_result  = extract(setup_result)
    transform_result = transform(extract_result)
    load_result     = load(transform_result)
    train_result    = train_model(load_result)
    api_result      = validate_api(load_result, train_result)
    summary_result  = test_summary(
        setup_result, extract_result, transform_result,
        load_result, train_result, api_result
    )
    teardown_result = teardown_test_db(summary_result)

    # Chaîne explicite
    (
        setup_result
        >> extract_result
        >> transform_result
        >> load_result
        >> [train_result, api_result]
        >> summary_result
        >> teardown_result
    )


dag_instance = gtfs_test_pipeline()