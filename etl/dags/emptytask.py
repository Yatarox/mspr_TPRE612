from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

with DAG(dag_id="empty_task") as dag:
    extract_task = EmptyOperator(task_id="empty_nothing_extract")
    transform_task = EmptyOperator(task_id="empty_nothing_transform")
    load_task = EmptyOperator(task_id="empty_nothing_load")
    