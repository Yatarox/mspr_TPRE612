import sys
sys.path.append("/opt/airflow")
from airflow import DAG
import pendulum
from scripts.extract_gtfs_data_gouv import fetch_zip_urls_task, download_extract_task

with DAG(
    dag_id="gtfs_extraction",
    schedule="@daily",
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
):

    BASE_URL = "https://transport.data.gouv.fr/api/datasets/563dd039b5950814b0588710"

    urls_task = fetch_zip_urls_task(BASE_URL)
    download_task = download_extract_task(urls_task)

    urls_task >> download_task
