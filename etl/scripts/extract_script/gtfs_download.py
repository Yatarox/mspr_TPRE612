import os
import zipfile
import logging
from datetime import datetime
from typing import Dict, List, Optional
import requests
import hashlib
from scripts.extract_script.gtfs_utils import calculate_file_hash, check_if_already_extracted, write_metadata, GTFS_FILES

logger = logging.getLogger(__name__)

def download_file(url: str, output_path: str, force_download: bool = False) -> Optional[str]:
    if not force_download and os.path.exists(output_path):
        logger.info(f"File already exists: {output_path}")
        return calculate_file_hash(output_path)
    try:
        logger.info(f"Downloading: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        hash_md5 = hashlib.md5()
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None

def extract_zip(zip_path: str, extract_path: str) -> bool:
    try:
        logger.info(f"Extracting: {zip_path} to {extract_path}")
        os.makedirs(extract_path, exist_ok=True)
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)
        found_files = [f for f in GTFS_FILES if os.path.exists(os.path.join(extract_path, f))]
        return bool(found_files)
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return False

def download_and_extract_gtfs(download_map: Dict[str, Dict[str, str]], download_dir: str, extract_dir: str, force_download: bool = False) -> Dict[str, str]:
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)
    dataset_sources = {}
    for url, info in download_map.items():
        filename = info["filename"]
        dataset_name = os.path.splitext(filename)[0]
        source_url = info["source_url"]
        dataset_id = info.get("dataset_id", dataset_name)
        zip_path = os.path.join(download_dir, filename)
        extract_path = os.path.join(extract_dir, dataset_name)
        if not force_download and check_if_already_extracted(extract_dir, filename):
            dataset_sources[dataset_name] = source_url
            continue
        file_hash = download_file(url, zip_path, force_download)
        if not file_hash or not extract_zip(zip_path, extract_path):
            continue
        metadata = {
            "source_url": source_url,
            "dataset_id": dataset_id,
            "download_url": url,
            "filename": filename,
            "file_hash": file_hash,
            "created_at": info.get("created_at"),
            "updated_at": info.get("updated_at"),
            "extracted_at": datetime.now().isoformat(),
        }
        write_metadata(extract_path, metadata)
        dataset_sources[dataset_name] = source_url
    return dataset_sources

def clean_old_downloads(download_dir: str, keep_latest: int = 2):
    if not os.path.exists(download_dir):
        return
    zip_files = [f for f in os.listdir(download_dir) if f.endswith(".zip")]
    if len(zip_files) <= keep_latest:
        return
    zip_files_with_time = sorted(
        ((f, os.path.getmtime(os.path.join(download_dir, f))) for f in zip_files),
        key=lambda x: x[1], reverse=True
    )
    for filename, _ in zip_files_with_time[keep_latest:]:
        file_path = os.path.join(download_dir, filename)
        try:
            os.remove(file_path)
            logger.info(f"Deleted old file: {filename}")
        except Exception as e:
            logger.warning(f"Failed to delete {filename}: {e}")

def download_from_direct_urls(zip_urls: List[str], download_dir: str, extract_dir: str, force_download: bool = False) -> Dict[str, str]:
    from pathlib import Path
    from urllib.parse import urlparse
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)
    dataset_sources = {}
    for url in filter(None, map(str.strip, zip_urls)):
        url_name = Path(urlparse(url).path).name or "dataset.zip"
        filename = url_name if url_name.lower().endswith(".zip") else f"{url_name}.zip"
        dataset_name = os.path.splitext(filename)[0]
        zip_path = os.path.join(download_dir, filename)
        extract_path = os.path.join(extract_dir, dataset_name)
        if not force_download and check_if_already_extracted(extract_dir, filename):
            dataset_sources[dataset_name] = url
            continue
        file_hash = download_file(url, zip_path, force_download)
        if not file_hash or not extract_zip(zip_path, extract_path):
            continue
        metadata = {
            "source_url": url,
            "dataset_id": dataset_name,
            "download_url": url,
            "filename": filename,
            "file_hash": file_hash,
            "extracted_at": datetime.now().isoformat(),
        }
        write_metadata(extract_path, metadata)
        dataset_sources[dataset_name] = url
    return dataset_sources

download_and_unzip_from_zip_urls = download_from_direct_urls