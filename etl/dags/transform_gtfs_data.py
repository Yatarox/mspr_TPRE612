import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from airflow.decorators import dag, task
from airflow.models import Variable
import pendulum
from typing import List, Dict

@dag(
    dag_id="gtfs_transformation",
    schedule="@daily",
    start_date=pendulum.yesterday(),
    catchup=False,
    tags=["gtfs", "data"]
)

def gtfs_transformation():
    pass

dag_instance = gtfs_transformation()