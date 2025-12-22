import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
from typing import List

from scripts.extract_gtfs_data_gouv_script import get_zip_url, download_and_unzip
from scripts.transform_gtfs_data import transform_gtfs

# Optionnel: charge la fonction de Load si dispo
try:
    from scripts.load_gtfs import load_gtfs
except Exception:
    load_gtfs = None

def _parse_urls(value: str) -> List[str]:
    """
    Accepte:
      - JSON list: ["https://...","https://..."]
      - CSV: https://..., https://...
      - multi-lignes
    """
    import json
    v = (value or "").strip()
    if not v:
        return []
    if v.startswith("["):
        return [u.strip() for u in json.loads(v) if u and isinstance(u, str)]
    parts = [p.strip() for p in v.replace("\n", ",").split(",")]
    return [p for p in parts if p]

@dag(
    dag_id="gtfs_full_etl",
    schedule="@daily",
    start_date=pendulum.yesterday(),
    catchup=False,
    tags=["gtfs", "etl"]
)
def gtfs_full_etl():
    # URLs par défaut si aucune Variable n'est définie
    default_urls = [
        "https://transport.data.gouv.fr/api/datasets/563dd039b5950814b0588710",
        "https://transport.data.gouv.fr/api/datasets/5f9008f1af9cf0bed8270cde",
    ]

    urls_str = Variable.get("gtfs_base_urls", default_var=None)
    if urls_str:
        BASE_URLS = _parse_urls(urls_str)
    else:
        single = Variable.get("gtfs_base_url", default_var=None)
        BASE_URLS = [single] if single else default_urls

    RAW_DIR = Variable.get("gtfs_raw_dir", default_var="/opt/airflow/data/raw")
    STAGING_DIR = Variable.get("gtfs_staging_dir", default_var="/opt/airflow/data/staging")
    PROCESSED_DIR = Variable.get("gtfs_processed_dir", default_var="/opt/airflow/data/processed")

    @task
    def extract():
        all_files = {}
        for base_url in BASE_URLS:
            data = get_zip_url(base_url)
            all_files.update(data)
        download_and_unzip(all_files, RAW_DIR, STAGING_DIR)

    @task
    def transform():
        # Ecrit processed/<dataset_id>/trips_summary.csv
        return transform_gtfs(STAGING_DIR, PROCESSED_DIR)

    @task
    def load():
        if load_gtfs is None:
            raise RuntimeError("Ajoute une fonction `load_gtfs` dans scripts/load_gtfs.py (ou adapte l'import).")
        conn_id = Variable.get("gtfs_db_conn_id", default_var=None)
        load_gtfs(PROCESSED_DIR, conn_id)

    e = extract()
    t = transform()
    e >> t

    if load_gtfs is not None:
        l = load()
        t >> l

dag_instance = gtfs_full_etl()