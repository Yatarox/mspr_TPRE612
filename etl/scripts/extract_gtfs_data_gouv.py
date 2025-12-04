from airflow.decorators import task
import requests
import os
import zipfile
from typing import List, Dict


# ---------------------------------------------------------
# Fonctions utilitaires
# ---------------------------------------------------------

def get_zip_url(base_url: str) -> Dict[str, str]:
    """
    Récupère les URLs et filenames des fichiers ZIP à partir du dataset Gouv.
    """
    data = {}
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        json_data = response.json()

        for item in json_data.get("history", []):
            payload = item.get("payload", {})
            url = payload.get("permanent_url")
            filename = payload.get("filename")

            if url and filename:
                data[url] = filename
    except Exception as e:
        print(f"[ERROR] API error: {e}")

    return data


def download_and_unzip(data: Dict[str, str],
                       download_dir="/opt/airflow/data/raw",
                       extract_dir="/opt/airflow/data/staging"):
    """
    Télécharge et extrait les fichiers ZIP.
    """
    for url, filename in data.items():
        file_path = os.path.join(download_dir, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        try:
            response = requests.get(url)
            response.raise_for_status()

            with open(file_path, "wb") as file:
                file.write(response.content)
            print(f"[INFO] Downloaded: {file_path}")

            extract_path = os.path.join(extract_dir, os.path.splitext(filename)[0])
            os.makedirs(extract_path, exist_ok=True)

            with zipfile.ZipFile(file_path, "r") as zip_ref:
                zip_ref.extractall(extract_path)

            print(f"[INFO] Extracted {file_path} → {extract_path}")

        except Exception as e:
            print(f"[ERROR] Download/extraction error for {url}: {e}")


# ---------------------------------------------------------
# Tasks Airflow
# ---------------------------------------------------------

@task
def fetch_zip_urls_task(base_url: str) -> List[str]:
    """
    Retourne uniquement la liste des URLs pour éviter un XCom trop volumineux.
    """
    data = get_zip_url(base_url)
    return list(data.keys())


@task
def download_extract_task(urls: List[str]):
    """
    Reconstruit le dictionnaire URL → filename pour le téléchargement.
    """
    data = {}
    for url in urls:
        filename = url.split("/")[-2] + "/" + url.split("/")[-1]  # recrée le filename comme avant
        data[url] = filename

    download_and_unzip(data)
