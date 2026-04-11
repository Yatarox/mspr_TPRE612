import os
import sys
import tempfile

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from scripts.extract_script.gtfs_utils import (
    calculate_file_hash,
    check_if_already_extracted,
    write_metadata,
)


def test_calculate_file_hash():
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        tmp.write(b"hello world")
        tmp.flush()
        hash_val = calculate_file_hash(tmp.name)
        assert hash_val == "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    os.remove(tmp.name)


def test_write_and_check_metadata():
    with tempfile.TemporaryDirectory() as tmpdir:
        dataset_name = "test"
        extract_path = os.path.join(tmpdir, dataset_name)
        os.makedirs(extract_path, exist_ok=True)

        write_metadata(extract_path, {"file_hash": "abc123"})
        assert os.path.exists(os.path.join(extract_path, "metadata.json"))

        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")

        open(os.path.join(extract_path, "agency.txt"), "w", encoding="utf-8").close()
        assert check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")


def test_check_if_already_extracted_missing_metadata():
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = os.path.join(tmpdir, "test")
        os.makedirs(extract_path, exist_ok=True)
        open(os.path.join(extract_path, "agency.txt"), "w", encoding="utf-8").close()
        assert not check_if_already_extracted(tmpdir, "test.zip")


def test_check_if_already_extracted_hash_mismatch():
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = os.path.join(tmpdir, "test")
        os.makedirs(extract_path, exist_ok=True)

        write_metadata(extract_path, {"file_hash": "abc123"})
        open(os.path.join(extract_path, "agency.txt"), "w", encoding="utf-8").close()

        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="wronghash")


def test_check_if_already_extracted_invalid_metadata_json():
    # Couvre le except (lignes 33-37)
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = os.path.join(tmpdir, "test")
        os.makedirs(extract_path, exist_ok=True)

        with open(os.path.join(extract_path, "metadata.json"), "w", encoding="utf-8") as f:
            f.write("{bad json")

        open(os.path.join(extract_path, "agency.txt"), "w", encoding="utf-8").close()

        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")


def test_check_if_already_extracted_no_gtfs_files():
    with tempfile.TemporaryDirectory() as tmpdir:
        extract_path = os.path.join(tmpdir, "test")
        os.makedirs(extract_path, exist_ok=True)

        write_metadata(extract_path, {"file_hash": "abc123"})
        assert not check_if_already_extracted(tmpdir, "test.zip", file_hash="abc123")