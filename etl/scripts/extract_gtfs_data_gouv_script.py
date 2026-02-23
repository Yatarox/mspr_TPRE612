import requests
import os
import zipfile
from typing import Dict, List, Optional
import json
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime
import logging
import hashlib

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# Fonctions utilitaires API
# -------------------------------------------------------------


def get_latest_gtfs_from_api(base_url: str) -> Optional[Dict[str, str]]:
    try:
        logger.info(f"📡 Fetching latest GTFS from: {base_url}")
        response = requests.get(base_url, timeout=30)
        response.raise_for_status()
        json_data = response.json()

        history = json_data.get("history", [])

        if not history:
            logger.warning(f"No history found in API response from {base_url}")
            return None

        latest_item = history[0]
        payload = latest_item.get("payload", {})

        format_type = payload.get("format", "")
        if format_type != "GTFS":
            logger.warning(
                f"Latest file is not GTFS format (found: {format_type})")

            for item in history[:5]:
                payload = item.get("payload", {})
                if payload.get("format") == "GTFS":
                    latest_item = item
                    break
            else:
                logger.error(f"No GTFS file found in first 5 history items")
                return None

        url = payload.get("permanent_url")
        filename = payload.get("filename")
        dataset_id = payload.get("dataset_id", "")

        if not url or not filename:
            logger.error(f"Missing URL or filename in API response")
            return None

        created_at = payload.get("created_at") or latest_item.get("created_at")
        updated_at = payload.get("updated_at") or latest_item.get("updated_at")

        result = {
            "url": url,
            "filename": filename,
            "source_url": base_url,
            "dataset_id": dataset_id,
            "created_at": created_at,
            "updated_at": updated_at,
            "format": payload.get("format"),
        }

        logger.info(f"✓ Found latest GTFS: {filename} (updated: {updated_at})")
        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch from API {base_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing API response: {e}")
        return None


def build_download_list(base_urls: List[str]) -> Dict[str, Dict[str, str]]:

    download_map = {}

    for base_url in base_urls:
        if not base_url or not base_url.strip():
            continue

        latest = get_latest_gtfs_from_api(base_url.strip())

        if latest:
            url = latest["url"]
            download_map[url] = latest
        else:
            logger.warning(f"Skipping {base_url} - no valid GTFS found")

    logger.info(f"📋 Total files to download: {len(download_map)}")
    return download_map

# -------------------------------------------------------------
# Fonctions de téléchargement et extraction
# -------------------------------------------------------------


def calculate_file_hash(filepath: str) -> str:
    """Calcule le hash MD5 d'un fichier"""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_if_already_extracted(
        extract_dir: str,
        filename: str,
        file_hash: Optional[str] = None) -> bool:

    dataset_name = os.path.splitext(filename)[0]
    extract_path = os.path.join(extract_dir, dataset_name)
    metadata_path = os.path.join(extract_path, "metadata.json")

    if not os.path.exists(extract_path):
        return False

    if not os.path.exists(metadata_path):
        return False

    gtfs_files = [
        "agency.txt",
        "routes.txt",
        "trips.txt",
        "stops.txt",
        "stop_times.txt"]
    if not any(os.path.exists(os.path.join(extract_path, f))
               for f in gtfs_files):
        logger.warning(f"Dataset {dataset_name} exists but missing GTFS files")
        return False

    if file_hash:
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            stored_hash = metadata.get("file_hash")
            if stored_hash != file_hash:
                logger.info(
                    f"Dataset {dataset_name} has different version (hash mismatch)")
                return False
        except Exception as e:
            logger.warning(f"Could not verify hash for {dataset_name}: {e}")

    logger.debug(f"Dataset {dataset_name} already extracted and up-to-date")
    return True


def download_file(
        url: str,
        output_path: str,
        force_download: bool = False) -> Optional[str]:

    if not force_download and os.path.exists(output_path):
        logger.info(
            f"  → File already exists: {
                os.path.basename(output_path)}")
        return calculate_file_hash(output_path)

    try:
        logger.info(f"⬇ Downloading from {url}")

        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))

        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        hash_md5 = hashlib.md5()
        downloaded_size = 0

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    hash_md5.update(chunk)
                    downloaded_size += len(chunk)

                    if total_size > 0 and downloaded_size % (
                            10 * 1024 * 1024) == 0:
                        progress = (downloaded_size / total_size) * 100
                        logger.debug(f"  Progress: {progress:.1f}%")

        file_hash = hash_md5.hexdigest()
        logger.info(
            f"  ✓ Downloaded: {
                os.path.basename(output_path)} ({
                downloaded_size /
                1024 /
                1024:.2f} MB)")

        return file_hash

    except requests.exceptions.RequestException as e:
        logger.error(f"  ✗ Download failed: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None
    except Exception as e:
        logger.error(f"  ✗ Unexpected error during download: {e}")
        if os.path.exists(output_path):
            os.remove(output_path)
        return None


def extract_zip(zip_path: str, extract_path: str) -> bool:
    """
    Extrait un fichier ZIP.

    Args:
        zip_path: Chemin du fichier ZIP
        extract_path: Dossier d'extraction

    Returns:
        True si succès, False sinon
    """
    try:
        logger.info(f"📦 Extracting to {extract_path}")

        os.makedirs(extract_path, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_path)

        gtfs_files = [
            "agency.txt",
            "routes.txt",
            "trips.txt",
            "stops.txt",
            "stop_times.txt"]
        found_files = [
            f for f in gtfs_files if os.path.exists(
                os.path.join(
                    extract_path, f))]

        if not found_files:
            logger.warning(f"  ⚠ No GTFS files found after extraction")
            return False

        logger.info(
            f"  ✓ Extracted successfully ({
                len(found_files)} GTFS files found)")
        return True

    except zipfile.BadZipFile as e:
        logger.error(f"  ✗ Invalid ZIP file: {e}")
        return False
    except Exception as e:
        logger.error(f"  ✗ Extraction failed: {e}")
        return False


def download_and_extract_gtfs(
    download_map: Dict[str, Dict[str, str]],
    download_dir: str,
    extract_dir: str,
    force_download: bool = False
) -> Dict[str, str]:
    """
    Télécharge et extrait les fichiers GTFS.

    Args:
        download_map: Dict avec URL -> infos fichier
        download_dir: Répertoire de téléchargement
        extract_dir: Répertoire d'extraction
        force_download: Force le re-téléchargement

    Returns:
        Dict dataset_name -> source_url
    """
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    dataset_sources = {}
    success_count = 0
    skip_count = 0
    error_count = 0

    total = len(download_map)

    for idx, (url, info) in enumerate(download_map.items(), 1):
        filename = info["filename"]
        dataset_name = os.path.splitext(filename)[0]
        source_url = info["source_url"]
        dataset_id = info.get("dataset_id", dataset_name)

        logger.info(f"\n[{idx}/{total}] Processing: {dataset_name}")

        zip_path = os.path.join(download_dir, filename)
        extract_path = os.path.join(extract_dir, dataset_name)

        if not force_download and check_if_already_extracted(
                extract_dir, filename):
            logger.info(
                f"✓ Dataset '{dataset_name}' already extracted, skipping")
            dataset_sources[dataset_name] = source_url
            skip_count += 1
            continue

        file_hash = download_file(url, zip_path, force_download)

        if not file_hash:
            logger.error(f"✗ Failed to download {dataset_name}")
            error_count += 1
            continue

        if not extract_zip(zip_path, extract_path):
            logger.error(f"✗ Failed to extract {dataset_name}")
            error_count += 1
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

        metadata_path = os.path.join(extract_path, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        dataset_sources[dataset_name] = source_url
        success_count += 1
        logger.info(f"✓ Dataset '{dataset_name}' processed successfully")

    logger.info(f"\n{'=' * 60}")
    logger.info(f"📊 EXTRACTION SUMMARY:")
    logger.info(f"   Total: {total}")
    logger.info(f"   ✓ Success: {success_count}")
    logger.info(f"   ⊙ Skipped: {skip_count}")
    logger.info(f"   ✗ Errors: {error_count}")
    logger.info(f"{'=' * 60}\n")

    return dataset_sources


def clean_old_downloads(download_dir: str, keep_latest: int = 2):
    """
    Nettoie les anciens fichiers ZIP, conserve seulement les N plus récents.

    Args:
        download_dir: Répertoire contenant les ZIP
        keep_latest: Nombre de fichiers à conserver
    """
    if not os.path.exists(download_dir):
        return

    zip_files = [f for f in os.listdir(download_dir) if f.endswith(".zip")]

    if len(zip_files) <= keep_latest:
        logger.info(
            f"No cleanup needed ({
                len(zip_files)} files, keeping {keep_latest})")
        return

    zip_files_with_time = [
        (f, os.path.getmtime(os.path.join(download_dir, f)))
        for f in zip_files
    ]
    zip_files_with_time.sort(key=lambda x: x[1], reverse=True)

    deleted_count = 0
    for filename, _ in zip_files_with_time[keep_latest:]:
        file_path = os.path.join(download_dir, filename)
        try:
            os.remove(file_path)
            logger.info(f"🗑 Deleted old file: {filename}")
            deleted_count += 1
        except Exception as e:
            logger.warning(f"Failed to delete {filename}: {e}")

    if deleted_count > 0:
        logger.info(f"✓ Cleaned {deleted_count} old files")

# -------------------------------------------------------------
# Fonction compatible ancienne API (backward compatibility)
# -------------------------------------------------------------


def download_and_unzip(
    data: Dict[str, Dict[str, str]],
    download_dir: str,
    extract_dir: str,
    force_download: bool = False
) -> Dict[str, str]:
    """
    Fonction legacy pour compatibilité.
    Redirige vers la nouvelle implémentation.
    """
    logger.warning(
        "Using deprecated download_and_unzip, use download_and_extract_gtfs instead")
    return download_and_extract_gtfs(
        data, download_dir, extract_dir, force_download)


# -------------------------------------------------------------
# Option 2: URLs ZIP directes
# -------------------------------------------------------------

def download_from_direct_urls(
    zip_urls: List[str],
    download_dir: str,
    extract_dir: str,
    force_download: bool = False
) -> Dict[str, str]:
    """
    Télécharge et extrait depuis des URLs ZIP directes (pas via API).

    Args:
        zip_urls: Liste d'URLs de fichiers ZIP
        download_dir: Répertoire de téléchargement
        extract_dir: Répertoire d'extraction
        force_download: Force le re-téléchargement

    Returns:
        Dict dataset_name -> source_url
    """
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True)

    dataset_sources = {}

    for url in zip_urls:
        url = url.strip()
        if not url:
            continue

        url_name = Path(urlparse(url).path).name or "dataset.zip"
        filename = url_name if url_name.lower().endswith(
            ".zip") else f"{url_name}.zip"
        dataset_name = os.path.splitext(filename)[0]

        logger.info(f"\nProcessing direct URL: {dataset_name}")

        zip_path = os.path.join(download_dir, filename)
        extract_path = os.path.join(extract_dir, dataset_name)

        if not force_download and check_if_already_extracted(
                extract_dir, filename):
            logger.info(
                f"✓ Dataset '{dataset_name}' already extracted, skipping")
            dataset_sources[dataset_name] = url
            continue

        file_hash = download_file(url, zip_path, force_download)

        if not file_hash:
            logger.error(f"✗ Failed to download {dataset_name}")
            continue

        if not extract_zip(zip_path, extract_path):
            logger.error(f"✗ Failed to extract {dataset_name}")
            continue

        metadata = {
            "source_url": url,
            "dataset_id": dataset_name,
            "download_url": url,
            "filename": filename,
            "file_hash": file_hash,
            "extracted_at": datetime.now().isoformat(),
        }

        metadata_path = os.path.join(extract_path, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        dataset_sources[dataset_name] = url
        logger.info(f"✓ Dataset '{dataset_name}' processed successfully")

    logger.info(
        f"\n✓ Processed {
            len(dataset_sources)} datasets from direct URLs")
    return dataset_sources


# Alias pour backward compatibility
download_and_unzip_from_zip_urls = download_from_direct_urls
