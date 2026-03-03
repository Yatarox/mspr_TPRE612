import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.exceptions import AirflowException
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from load_script.helpers import get_staging_country_limits
from load_script.fact_loader import load_fact_table
from load_script.dimension_cache import dim_cache
from load_script.staging import load_staging_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VERSION = "load_gtfs.py v3.0 (auto-create dim_country, set-based fact load)"
logger.info(f"[BOOT] {VERSION}")


def load_gtfs(processed_dir: str,
              conn_id: str = "mysql_default") -> Dict[str, Any]:
    hook = MySqlHook(mysql_conn_id=conn_id)
    processed = Path(processed_dir)

    if not processed.exists():
        raise AirflowException(f"Directory not found: {processed}")

    origin_max_len, dest_max_len = get_staging_country_limits(hook)

    total_loaded = 0
    datasets_done = 0

    for dataset_dir in sorted([p for p in processed.iterdir() if p.is_dir()]):
        try:
            dataset_id = int(dataset_dir.name)
        except ValueError:
            dataset_id = dataset_dir.name
            logger.info(f"Using UUID as dataset_id: {dataset_id}")

        load_id = int(datetime.now().timestamp() * 1000)

        csv_path = dataset_dir / f"trips_summary_{dataset_id}.csv"
        if not csv_path.exists():
            csv_path = dataset_dir / "trips_summary.csv"

        logger.info(f"Loading dataset {dataset_id}...")
        logger.info(f"  Looking for CSV at: {csv_path}")
        logger.info(f"  CSV exists: {csv_path.exists()}")

        if not csv_path.exists():
            logger.error(f"Available files in {dataset_dir}:")
            for f in dataset_dir.rglob("*"):
                logger.error(f"  - {f.name}")
            continue 

        loaded = load_staging_table(
            hook,
            csv_path,
            load_id,
            dataset_id,
            origin_max_len,
            dest_max_len)
        
        if loaded == 0:
            logger.warning(f"No data for dataset {dataset_id}")
            dim_cache.clear()
            continue

        processed_count = load_fact_table(hook, load_id)
        total_loaded += processed_count
        datasets_done += 1
        logger.info(f"✓ Dataset {dataset_id}: {processed_count} facts loaded")

        dim_cache.clear()

    if total_loaded == 0:
        raise AirflowException("No data loaded")

    logger.info(
        f"✓✓✓ SUCCESS: {total_loaded} total facts loaded across {datasets_done} datasets")
    return {"total_rows": total_loaded, "datasets": datasets_done}