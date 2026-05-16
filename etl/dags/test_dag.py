import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, List

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.models.xcom_arg import XComArg
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
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
    schedule=None,                   
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    is_paused_upon_creation=True,      
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "etl", "test"],
    description="Pipeline ETL de test — DB isolée, déclenchement manuel uniquement",
)
def gtfs_test_pipeline():


    @task
    def setup_test_db() -> Dict[str, Any]:
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



    @task
    def extract(setup_stats: Dict[str, Any]) -> Dict[str, Any]:
        start_time = datetime.now()
        logger.info(f"[TEST] Extract started — URL: {TEST_ZIP_URL}")

        result = download_and_unzip_from_zip_urls(
            [TEST_ZIP_URL],
            TEST_RAW_DIR,
            TEST_STAGING_DIR,
            force_download=True, 
        )

        downloaded_count = len(result)
        duration = (datetime.now() - start_time).total_seconds()

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



    @task
    def transform(extract_stats: Dict[str, Any]) -> Dict[str, Any]:
        import os
        start_time = datetime.now()
        logger.info("[TEST] Transform started")

        result = transform_gtfs(
            TEST_STAGING_DIR,
            TEST_PROCESSED_DIR,
            max_workers=1, 
        )

        duration = (datetime.now() - start_time).total_seconds()

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



    @task
    def load(transform_stats: Dict[str, Any]) -> Dict[str, Any]:
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

        assert rows_loaded > 0, (
            f"[TEST FAIL] Load: aucune ligne chargée dans fact_trip_summary"
        )

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



    @task
    def train_model(load_stats: Dict[str, Any]) -> Dict[str, Any]:
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


    setup_result    = setup_test_db()
    extract_result  = extract(setup_result)
    transform_result = transform(extract_result)
    load_result     = load(transform_result)
    train_result    = train_model(load_result)
    summary_result  = test_summary(
        setup_result, extract_result, transform_result,
        load_result, train_result
    )
    teardown_result = teardown_test_db(summary_result)

    (
        setup_result
        >> extract_result
        >> transform_result
        >> load_result
        >> train_result
        >> summary_result
        >> teardown_result
    )


dag_instance = gtfs_test_pipeline()