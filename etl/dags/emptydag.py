from airflow.sdk import DAG

with DAG(dag_id="empty_dag") as dag:
    pass
