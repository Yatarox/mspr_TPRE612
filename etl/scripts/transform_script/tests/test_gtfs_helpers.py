import os
import sys
import json
from unittest.mock import patch, MagicMock
import numpy as np
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
    _impute_missing_distances
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

def test_impute_missing_distances_basic():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3"],
        "train_type": ["Rail", "Rail", "Rail"],
        "distance_km": [100, 0, np.nan],
        "duration_h": [2.0, 1.0, 1.5],
        "total_emission_kgco2e": [2.5, 0, np.nan]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T2", "distance_km"].values[0] > 0
    assert result.loc[result["trip_id"] == "T3", "distance_km"].values[0] > 0
    assert result.loc[result["trip_id"] == "T2", "total_emission_kgco2e"].values[0] > 0
    assert result.loc[result["trip_id"] == "T3", "total_emission_kgco2e"].values[0] > 0
    assert result.loc[result["trip_id"] == "T1", "distance_km"].values[0] == 100


def test_impute_missing_distances_only_rail():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3"],
        "train_type": ["Rail", "Bus", "Rail"],
        "distance_km": [0, 0, 100],
        "duration_h": [1.0, 1.0, 2.0],
        "total_emission_kgco2e": [0, 0, 2.5]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T1", "distance_km"].values[0] > 0
    assert result.loc[result["trip_id"] == "T2", "distance_km"].values[0] == 0
    assert result.loc[result["trip_id"] == "T3", "distance_km"].values[0] == 100


def test_impute_missing_distances_no_valid_reference():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2"],
        "train_type": ["Rail", "Rail"],
        "distance_km": [0, 0],
        "duration_h": [1.0, 2.0],
        "total_emission_kgco2e": [0, 0]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T1", "distance_km"].values[0] == 0
    assert result.loc[result["trip_id"] == "T2", "distance_km"].values[0] == 0


def test_impute_missing_distances_empty_dataframe():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df_empty = pd.DataFrame()
    
    result = _impute_missing_distances(df_empty, "test_dataset")
    
    assert result.empty


def test_impute_missing_distances_no_missing():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3"],
        "train_type": ["Rail", "Rail", "Bus"],
        "distance_km": [100, 200, 50],
        "duration_h": [2.0, 4.0, 1.0],
        "total_emission_kgco2e": [2.5, 5.0, 1.2]
    })
    
    original = df.copy()
    result = _impute_missing_distances(df, "test_dataset")
    
    pd.testing.assert_frame_equal(original, result)


def test_impute_missing_distances_uses_median_speed():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3", "T4"],
        "train_type": ["Rail", "Rail", "Rail", "Rail"],
        "distance_km": [100, 200, 0, np.nan],
        "duration_h": [2.0, 4.0, 3.0, 1.5],
        "total_emission_kgco2e": [2.5, 5.0, 0, np.nan]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T3", "distance_km"].values[0] == 150.0
    assert result.loc[result["trip_id"] == "T4", "distance_km"].values[0] == 75.0
    assert result.loc[result["trip_id"] == "T3", "total_emission_kgco2e"].values[0] == 3.75
    assert result.loc[result["trip_id"] == "T4", "total_emission_kgco2e"].values[0] == 1.88


def test_impute_missing_distances_with_mixed_valid_data():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2", "T3", "T4", "T5"],
        "train_type": ["Rail", "Rail", "Bus", "Rail", "Metro"],
        "distance_km": [80, 0, 30, np.nan, 0],
        "duration_h": [1.6, 2.0, 1.0, 2.5, 1.0],
        "total_emission_kgco2e": [2.0, 0, 0.8, np.nan, 0]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T2", "distance_km"].values[0] == 100.0
    assert result.loc[result["trip_id"] == "T4", "distance_km"].values[0] == 125.0
    assert result.loc[result["trip_id"] == "T3", "distance_km"].values[0] == 30
    assert result.loc[result["trip_id"] == "T5", "distance_km"].values[0] == 0


def test_impute_missing_distances_no_rail_at_all():
    from transform_script.gtfs_helpers import _impute_missing_distances
    
    df = pd.DataFrame({
        "trip_id": ["T1", "T2"],
        "train_type": ["Bus", "Metro"],
        "distance_km": [0, np.nan],
        "duration_h": [1.0, 1.5],
        "total_emission_kgco2e": [0, np.nan]
    })
    
    result = _impute_missing_distances(df, "test_dataset")
    
    assert result.loc[result["trip_id"] == "T1", "distance_km"].values[0] == 0
    assert np.isnan(result.loc[result["trip_id"] == "T2", "distance_km"].values[0])