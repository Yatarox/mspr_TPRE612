import sys
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

import pendulum
from airflow.sdk import dag, task, get_current_context
from airflow.models import Variable

sys.path.append("/opt/airflow")

from scripts.extract_gtfs_data_gouv_script import (  # noqa: E402
    build_download_list,
    download_and_extract_gtfs,
    download_and_unzip_from_zip_urls,
    clean_old_downloads,
)
from scripts.load_gtfs import load_gtfs  # noqa: E402
from scripts.transform_gtfs_data import transform_gtfs  # noqa: E402

logger = logging.getLogger(__name__)


class StructuredLogger:
    """Structured logger for easier monitoring."""

    def __init__(self, base_logger: logging.Logger):
        self.logger = base_logger

    def log_metric(self, metric_name: str, value: float, **kwargs):
        payload = {
            "type": "metric",
            "name": metric_name,
            "value": value,
            "timestamp": datetime.now().isoformat(),
            **kwargs,
        }
        self.logger.info(json.dumps(payload))

    def log_event(self, event_name: str, **kwargs):
        payload = {
            "type": "event",
            "name": event_name,
            "timestamp": datetime.now().isoformat(),
            **kwargs,
        }
        self.logger.info(json.dumps(payload))

    def log_error(self, error_name: str, error_msg: str, **kwargs):
        payload = {
            "type": "error",
            "name": error_name,
            "message": str(error_msg),
            "timestamp": datetime.now().isoformat(),
            **kwargs,
        }
        self.logger.error(json.dumps(payload))


structured_logger = StructuredLogger(logger)


def _parse_urls(value: str) -> List[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            data = json.loads(raw)
            return [u.strip() for u in data if isinstance(u, str) and u.strip()]
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in URL variable. Fallback to split parsing.")
    return [p.strip() for p in raw.replace("\n", ",").split(",") if p.strip()]


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


@dag(
    dag_id="gtfs_full_etl",
    default_args=default_args,
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "etl", "production"],
    description="Pipeline ETL complet pour données GTFS européennes",
)
def gtfs_full_etl():
    default_urls = [
        "https://transport.data.gouv.fr/api/datasets/563dd039b5950814b0588710",
        "https://transport.data.gouv.fr/api/datasets/5f9008f1af9cf0bed8270cde",
        "https://transport.data.gouv.fr/api/datasets/685baf2be31192d0ec7bcdc9",
        "https://transport.data.gouv.fr/api/datasets/63c02bbf4059d43863de0c81",
        "https://transport.data.gouv.fr/api/datasets/64635525318cc75a9a8a771f",
        "https://transport.data.gouv.fr/api/datasets/6449c52caeceb71273a42dd3",
        "https://transport.data.gouv.fr/api/datasets/68afa029133777b3ecd6bb8b",
        "https://transport.data.gouv.fr/api/datasets/65af92d12d38ceffacb04812",
        "https://transport.data.gouv.fr/api/datasets/63bc5f36f25deab1e855c0ff",
    ]

    default_zip_urls = [
        "https://www.data.gouv.fr/api/1/datasets/r/b2dfbaa3-47e9-4749-b6a4-750bebd760e7",
    ]

    # URLs
    urls_str = Variable.get("gtfs_base_urls", default_var=None)
    if urls_str:
        base_urls = _parse_urls(urls_str)
    else:
        single = Variable.get("gtfs_base_url", default_var=None)
        base_urls = [single] if single else default_urls

    zip_urls_str = Variable.get("gtfs_zip_urls", default_var=None)
    zip_urls = _parse_urls(zip_urls_str) if zip_urls_str else default_zip_urls

    # Paths / DB
    raw_dir = Variable.get("gtfs_raw_dir", default_var="/opt/airflow/data/raw")
    staging_dir = Variable.get("gtfs_staging_dir", default_var="/opt/airflow/data/staging")
    processed_dir = Variable.get("gtfs_processed_dir", default_var="/opt/airflow/data/processed")
    db_conn_id = Variable.get("gtfs_db_conn_id", default_var="mysql_default")

    # Runtime options
    force_download = Variable.get("gtfs_force_download", default_var="false").lower() == "true"
    keep_latest_zips = int(Variable.get("gtfs_keep_latest_zips", default_var="2"))
    max_workers = int(Variable.get("gtfs_max_workers", default_var="4"))
    load_batch_size = int(Variable.get("gtfs_load_batch_size", default_var="2000"))

    structured_logger.log_event(
        "dag_config_loaded",
        raw_dir=raw_dir,
        staging_dir=staging_dir,
        processed_dir=processed_dir,
        force_download=force_download,
        max_workers=max_workers,
        load_batch_size=load_batch_size,
        base_urls_count=len(base_urls),
        zip_urls_count=len(zip_urls),
    )

    @task
    def extract() -> Dict[str, Any]:
        context = get_current_context()
        start_time = datetime.now()

        try:
            structured_logger.log_event(
                "extract_started",
                task_id=context["task_instance"].task_id,
            )

            downloaded_count = 0

            # Option 1: direct ZIP URLs
            if zip_urls:
                logger.info("Using direct ZIP URLs")
                result_zip = download_and_unzip_from_zip_urls(
                    zip_urls,
                    raw_dir,
                    staging_dir,
                    force_download=force_download,
                )
                downloaded_count += len(result_zip)

            # Option 2: API URLs
            if base_urls:
                logger.info("Using API URLs")
                download_map = build_download_list(base_urls)
                result_api = download_and_extract_gtfs(
                    download_map,
                    raw_dir,
                    staging_dir,
                    force_download=force_download,
                )
                downloaded_count += len(result_api)

            try:
                clean_old_downloads(raw_dir, keep_latest=keep_latest_zips)
            except Exception as clean_err:
                logger.warning(f"Failed to clean old downloads: {clean_err}")

            duration = (datetime.now() - start_time).total_seconds()
            stats = {
                "downloaded": downloaded_count,
                "duration_seconds": duration,
                "success": True,
            }
            structured_logger.log_metric("extract_duration", duration, **stats)
            structured_logger.log_event("extract_completed", **stats)
            logger.info(f"✓ EXTRACT completed - Downloaded: {downloaded_count}, Duration: {duration:.2f}s")
            return stats

        except Exception as exc:
            duration = (datetime.now() - start_time).total_seconds()
            structured_logger.log_error("extract_failed", str(exc), duration_seconds=duration)
            logger.error(f"✗ EXTRACT failed after {duration:.2f}s: {exc}")
            raise

    @task
    def transform(extract_stats: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        start_time = datetime.now()

        try:
            structured_logger.log_event(
                "transform_started",
                task_id=context["task_instance"].task_id,
                extract_stats=extract_stats,
            )

            result = transform_gtfs(
                staging_dir,
                processed_dir,
                max_workers=max_workers,
            )

            duration = (datetime.now() - start_time).total_seconds()
            stats = {
                "files_generated": len(result),
                "files_list": result,
                "duration_seconds": duration,
                "success": True,
            }
            structured_logger.log_metric("transform_duration", duration, **stats)
            structured_logger.log_event("transform_completed", **stats)
            logger.info(f"✓ TRANSFORM completed - Files: {len(result)}, Duration: {duration:.2f}s")
            return stats

        except Exception as exc:
            duration = (datetime.now() - start_time).total_seconds()
            structured_logger.log_error("transform_failed", str(exc), duration_seconds=duration)
            logger.error(f"✗ TRANSFORM failed after {duration:.2f}s: {exc}")
            raise

    @task
    def load(transform_stats: Dict[str, Any]) -> Dict[str, Any]:
        context = get_current_context()
        start_time = datetime.now()

        try:
            structured_logger.log_event(
                "load_started",
                task_id=context["task_instance"].task_id,
                transform_stats=transform_stats,
            )

            if not transform_stats.get("files_list"):
                logger.warning("No files to load from transform")
                return {"rows_loaded": 0, "success": False, "message": "No files to load"}

            result = load_gtfs(
                processed_dir,
                conn_id=db_conn_id,
            )

            duration = (datetime.now() - start_time).total_seconds()
            stats = {
                "rows_loaded": result.get("total_rows", 0) if isinstance(result, dict) else 0,
                "duration_seconds": duration,
                "success": True,
                "load_result": result,
            }
            structured_logger.log_metric("load_duration", duration, **stats)
            structured_logger.log_metric("rows_loaded", stats["rows_loaded"])
            structured_logger.log_event("load_completed", **stats)
            logger.info(f"✓ LOAD completed - Rows: {stats['rows_loaded']}, Duration: {duration:.2f}s")
            return stats

        except Exception as exc:
            duration = (datetime.now() - start_time).total_seconds()
            structured_logger.log_error("load_failed", str(exc), duration_seconds=duration)
            logger.error(f"✗ LOAD failed after {duration:.2f}s: {exc}")
            raise

    @task
    def pipeline_summary(extract_stats: Dict, transform_stats: Dict, load_stats: Dict) -> Dict[str, Any]:
        total_duration = (
            extract_stats.get("duration_seconds", 0)
            + transform_stats.get("duration_seconds", 0)
            + load_stats.get("duration_seconds", 0)
        )

        summary = {
            "pipeline": "gtfs_full_etl",
            "total_duration_seconds": total_duration,
            "extract": extract_stats,
            "transform": transform_stats,
            "load": load_stats,
            "success": all(
                [
                    extract_stats.get("success"),
                    transform_stats.get("success"),
                    load_stats.get("success"),
                ]
            ),
        }

        structured_logger.log_event("pipeline_completed", **summary)
        logger.info("📊 PIPELINE SUMMARY:")
        logger.info(f"   Total Duration: {total_duration:.2f}s")
        logger.info(f"   Files Downloaded: {extract_stats.get('downloaded', 0)}")
        logger.info(f"   Files Transformed: {transform_stats.get('files_generated', 0)}")
        logger.info(f"   Rows Loaded: {load_stats.get('rows_loaded', 0)}")
        return summary

    @task
    def final_cleanup():
        logger.info("Performing final cleanup tasks...")
        return "Cleanup completed"

    extract_result = extract()
    transform_result = transform(extract_result)
    load_result = load(transform_result)
    summary = pipeline_summary(extract_result, transform_result, load_result)

    extract_result >> transform_result >> load_result >> summary >> final_cleanup()


dag_instance = gtfs_full_etl()