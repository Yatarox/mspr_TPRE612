import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
from scripts.transform_gtfs_data import transform_gtfs

@dag(
    dag_id="gtfs_transformation",
    schedule="@daily",
    start_date=pendulum.yesterday(),
    catchup=False,
    tags=["gtfs", "data"]
)
def gtfs_transformation():
    STAGING_DIR = Variable.get("gtfs_staging_dir", default_var="/opt/airflow/data/staging")
    PROCESSED_DIR = Variable.get("gtfs_processed_dir", default_var="/opt/airflow/data/processed")

    @task
    def transform():
        transform_gtfs(STAGING_DIR, PROCESSED_DIR)

    transform()

dag_instance = gtfs_transformation()