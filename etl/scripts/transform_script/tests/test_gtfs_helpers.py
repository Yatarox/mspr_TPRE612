import os
import sys
import json
from unittest.mock import patch, MagicMock

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from transform_script.gtfs_helpers import (
    log_memory,
    latest_version_dir,
    read_csv,
    read_metadata,
    is_valid_numeric,
    get_transport_type,
)


def test_log_memory_success():
    fake_proc = MagicMock()
    fake_proc.memory_info.return_value.rss = 50 * 1024 * 1024  # 50 MB

    with patch("transform_script.gtfs_helpers.psutil.Process", return_value=fake_proc):
        # Ne doit pas lever d'exception
        log_memory("TEST-")


def test_log_memory_exception_is_swallowed():
    with patch("transform_script.gtfs_helpers.psutil.Process", side_effect=Exception("boom")):
        # Ne doit pas lever d'exception (except pass)
        log_memory("TEST-")


def test_latest_version_dir_with_version_subdirs(tmp_path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    (dataset_dir / "20240101").mkdir()
    (dataset_dir / "20250101").mkdir()
    (dataset_dir / "20230101").mkdir()

    out = latest_version_dir(dataset_dir)
    assert out is not None
    assert out.name == "20250101"


def test_latest_version_dir_returns_dataset_dir_if_required_files_exist(tmp_path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    (dataset_dir / "stops.txt").write_text("id,name\n1,A\n", encoding="utf-8")

    out = latest_version_dir(dataset_dir)
    assert out == dataset_dir


def test_latest_version_dir_none_when_no_subdir_and_no_required_files(tmp_path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    out = latest_version_dir(dataset_dir)
    assert out is None


def test_read_csv_file_not_found_returns_empty_df(tmp_path):
    missing = tmp_path / "missing.csv"

    out = read_csv(missing)
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_read_csv_success(tmp_path):
    p = tmp_path / "data.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    out = read_csv(p)
    assert list(out.columns) == ["a", "b"]
    assert out.iloc[0]["a"] == "1"  # dtype=str attendu


def test_read_metadata_exists(tmp_path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()
    metadata_path = dataset_dir / "metadata.json"
    metadata = {"source": "x", "file_hash": "abc"}
    metadata_path.write_text(json.dumps(metadata), encoding="utf-8")

    out = read_metadata(dataset_dir)
    assert out == metadata


def test_read_metadata_missing_returns_empty_dict(tmp_path):
    dataset_dir = tmp_path / "dataset"
    dataset_dir.mkdir()

    out = read_metadata(dataset_dir)
    assert out == {}


def test_is_valid_numeric():
    assert is_valid_numeric("123")
    assert is_valid_numeric("123.45")
    assert not is_valid_numeric("abc")
    assert not is_valid_numeric("12/34")
    assert not is_valid_numeric("12-34-56")
    assert not is_valid_numeric("")


def test_is_valid_numeric_additional_cases():
    assert not is_valid_numeric(None)  # type: ignore[arg-type]
    assert not is_valid_numeric(123)   # type: ignore[arg-type]
    assert is_valid_numeric("  -12.5  ")
    assert not is_valid_numeric("1e3/2")
    assert not is_valid_numeric("--1")


def test_get_transport_type():
    assert get_transport_type("2") == "Rail"
    assert get_transport_type("101") == "High Speed Rail"
    assert get_transport_type("9999") == "Type 9999"


def test_get_transport_type_with_non_string_input():
    assert get_transport_type(3) == "Bus"
    assert get_transport_type(None) == "Type None"