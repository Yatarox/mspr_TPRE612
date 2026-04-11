import os
import json
import hashlib
from typing import Optional, Dict

GTFS_FILES = [
    "agency.txt", "routes.txt", "trips.txt", "stops.txt", "stop_times.txt"
]

def calculate_file_hash(filepath: str) -> str:
    # Strong non-cryptographic-integrity hash (preferred over MD5 for scanners/policies)
    hash_sha256 = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()

def check_if_already_extracted(extract_dir: str, filename: str, file_hash: Optional[str] = None) -> bool:
    dataset_name = os.path.splitext(filename)[0]
    extract_path = os.path.join(extract_dir, dataset_name)
    metadata_path = os.path.join(extract_path, "metadata.json")
    print(f"[DEBUG] check_if_already_extracted: extract_path={extract_path}, metadata_path={metadata_path}")
    if not (os.path.exists(extract_path) and os.path.exists(metadata_path)):
        print("[DEBUG] -> False (missing folder or metadata)")
        return False
    if not any(os.path.exists(os.path.join(extract_path, f)) for f in GTFS_FILES):
        print("[DEBUG] -> False (missing GTFS files)")
        return False
    if file_hash:
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            if metadata.get("file_hash") != file_hash:
                print("[DEBUG] -> False (hash mismatch)")
                return False
        except Exception:
            print("[DEBUG] -> False (exception reading metadata)")
            return False
    print("[DEBUG] -> True (already extracted)")
    return True

def write_metadata(extract_path: str, metadata: Dict):
    metadata_path = os.path.join(extract_path, "metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)