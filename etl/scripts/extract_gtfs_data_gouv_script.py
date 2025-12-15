import requests
import os
import zipfile
from typing import Dict

# -------------------------------------------------------------
# Fonctions utilitaires
# -------------------------------------------------------------

def get_zip_url(base_url: str) -> Dict[str, str]:
    data = {}
    response = requests.get(base_url)
    response.raise_for_status()
    json_data = response.json()

    for item in json_data.get("history", []):
        payload = item.get("payload", {})
        url = payload.get("permanent_url")
        filename = payload.get("filename")
        if url and filename:
            data[url] = filename

    if not data:
        raise ValueError("No ZIP URLs found from API")

    return data

def download_and_unzip(data: Dict[str, str], download_dir: str, extract_dir: str):
    for url, filename in data.items():
        file_path = os.path.join(download_dir, filename)
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        response = requests.get(url)
        response.raise_for_status()

        with open(file_path, "wb") as file:
            file.write(response.content)

        extract_path = os.path.join(extract_dir, os.path.splitext(filename)[0])
        os.makedirs(extract_path, exist_ok=True)

        with zipfile.ZipFile(file_path, "r") as zip_ref:
            zip_ref.extractall(extract_path)