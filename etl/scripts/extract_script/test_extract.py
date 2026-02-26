import os
import tempfile
import json
import pytest

from scripts.extract_script.gtfs_utils import (
    calculate_file_hash,
    check_if_already_extracted,
    write_metadata,
    GTFS_FILES
)

def test_calculate_file_hash():
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"hello world")
        tmp.flush()
        hash_val = calculate_file_hash(tmp.name)
        assert hash_val == "5eb63bbbe01eeed093cb22bb8f5acdc3"
    os.remove(tmp.name)

def test_write_and_check_metadata():
    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_name = "test"
        extract_path = os.path.join(tmpdir, dataset_name)
        os.makedirs(extract_path, exist_ok=True)
        metadata = {"file_hash": "abc123"}
        write_metadata(extract_path, metadata)
        assert os.path.exists(os.path.join(extract_path, "metadata.json"))
        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")
        open(os.path.join(extract_path, "agency.txt"), "w").close()
        assert check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")

def test_check_if_already_extracted_missing_metadata():
    with tempfile.TemporaryDirectory() as tmpdir:
        # No metadata file
        open(os.path.join(tmpdir, "agency.txt"), "w").close()
        assert not check_if_already_extracted(tmpdir, "test.zip")

def test_check_if_already_extracted_hash_mismatch():
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = tmpdir
        metadata = {"file_hash": "abc123"}
        write_metadata(extract_path, metadata)
        open(os.path.join(tmpdir, "agency.txt"), "w").close()
        # Should return False (hash mismatch)
        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="wronghash")