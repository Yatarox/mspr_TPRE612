import logging
import requests
from typing import Dict, Optional

logger = logging.getLogger(__name__)

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
        for item in history[:5]:
            payload = item.get("payload", {})
            if payload.get("format") == "GTFS":
                url = payload.get("permanent_url")
                filename = payload.get("filename")
                dataset_id = payload.get("dataset_id", "")
                created_at = payload.get("created_at") or item.get("created_at")
                updated_at = payload.get("updated_at") or item.get("updated_at")
                if url and filename:
                    logger.info(f"✓ Found latest GTFS: {filename} (updated: {updated_at})")
                    return {
                        "url": url,
                        "filename": filename,
                        "source_url": base_url,
                        "dataset_id": dataset_id,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "format": payload.get("format"),
                    }
        logger.error(f"No GTFS file found in first 5 history items")
        return None
    except requests.RequestException as e:
        logger.error(f"Failed to fetch from API {base_url}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error parsing API response: {e}")
    return None

def build_download_list(base_urls):
    download_map = {}
    for base_url in filter(None, map(str.strip, base_urls)):
        latest = get_latest_gtfs_from_api(base_url)
        if latest:
            download_map[latest["url"]] = latest
        else:
            logger.warning(f"Skipping {base_url} - no valid GTFS found")
    logger.info(f"📋 Total files to download: {len(download_map)}")
    return download_map