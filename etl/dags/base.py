# from airflow.decorators import dag, task
# from airflow.models import Variable
# import pendulum
# import requests
# import os
# import zipfile
# from typing import List, Dict

# # -------------------------------------------------------------
# # Fonctions utilitaires
# # -------------------------------------------------------------

# def get_zip_url(base_url: str) -> Dict[str, str]:
#     data = {}
#     response = requests.get(base_url)
#     response.raise_for_status()
#     json_data = response.json()

#     for item in json_data.get("history", []):
#         payload = item.get("payload", {})
#         url = payload.get("permanent_url")
#         filename = payload.get("filename")
#         if url and filename:
#             data[url] = filename

#     if not data:
#         raise ValueError("No ZIP URLs found from API")

#     return data

# def download_and_unzip(data: Dict[str, str], download_dir: str, extract_dir: str):
#     for url, filename in data.items():
#         file_path = os.path.join(download_dir, filename)
#         os.makedirs(os.path.dirname(file_path), exist_ok=True)

#         response = requests.get(url)
#         response.raise_for_status()

#         with open(file_path, "wb") as file:
#             file.write(response.content)

#         extract_path = os.path.join(extract_dir, os.path.splitext(filename)[0])
#         os.makedirs(extract_path, exist_ok=True)

#         with zipfile.ZipFile(file_path, "r") as zip_ref:
#             zip_ref.extractall(extract_path)

# # -------------------------------------------------------------
# # DAG
# # -------------------------------------------------------------

# @dag(
#     dag_id="gtfs_extraction_airflow_native",
#     schedule="@daily",
#     start_date=pendulum.yesterday(),
#     catchup=False,
#     tags=["gtfs", "data"]
# )
# def gtfs_extraction():
#     # On peut récupérer l'URL depuis une Variable Airflow
#     BASE_URL = Variable.get(
#         "gtfs_base_url",
#         default_var="https://transport.data.gouv.fr/api/datasets/563dd039b5950814b0588710"
#     )
#     RAW_DIR = Variable.get("gtfs_raw_dir", default_var="/opt/airflow/data/raw")
#     STAGING_DIR = Variable.get("gtfs_staging_dir", default_var="/opt/airflow/data/staging")

#     @task
#     def fetch_urls(base_url: str) -> List[str]:
#         data = get_zip_url(base_url)
#         return list(data.keys())

#     @task
#     def download_extract(urls: List[str], raw_dir: str, staging_dir: str):
#         data = {url: os.path.basename(url) for url in urls}
#         download_and_unzip(data, raw_dir, staging_dir)

#     urls = fetch_urls(BASE_URL)
#     download_extract(urls, RAW_DIR, STAGING_DIR)

# dag_instance = gtfs_extraction()