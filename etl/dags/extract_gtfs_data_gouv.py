import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
from scripts.extract_gtfs_data_gouv_script import get_zip_url, download_and_unzip
from typing import List, Dict

@dag(
    dag_id="gtfs_extraction",
    schedule="@daily",
    start_date=pendulum.yesterday(),
    catchup=False,
    tags=["gtfs", "data"]
)

def gtfs_extraction():
    # On peut récupérer l'URL depuis une Variable Airflow
    BASE_URL = Variable.get(
        "gtfs_base_url",
        default_var="https://transport.data.gouv.fr/api/datasets/563dd039b5950814b0588710"
    )
    RAW_DIR = Variable.get("gtfs_raw_dir", default_var="/opt/airflow/data/raw")
    STAGING_DIR = Variable.get("gtfs_staging_dir", default_var="/opt/airflow/data/staging")

    @task
    def fetch_urls(base_url: str) -> List[str]:
        data = get_zip_url(base_url)
        return list(data.keys())

    @task
    def download_extract(urls: List[str], raw_dir: str, staging_dir: str):
        data = {url: os.path.basename(url) for url in urls}
        download_and_unzip(data, raw_dir, staging_dir)

    urls = fetch_urls(BASE_URL)
    download_extract(urls, RAW_DIR, STAGING_DIR)

dag_instance = gtfs_extraction()