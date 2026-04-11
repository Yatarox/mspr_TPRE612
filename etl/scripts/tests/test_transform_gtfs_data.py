import os
import sys
from unittest.mock import patch
import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)

from transform_gtfs_data import (
    _normalize_columns,
    _empty_trip_frame,
    _prepare_stop_times_df,
    _sanitize_dataframe,
    _write_csv,
    _resolve_dataset_output_id,
)


def test_normalize_columns():
    df = pd.DataFrame({" trip_id ": [1, 2], "route_name": ["R1", "R2"]})
    result = _normalize_columns(df)
    assert "trip_id" in result.columns
    assert "route_name" in result.columns


def test_normalize_columns_none():
    result = _normalize_columns(None)
    assert result is None


def test_normalize_columns_empty():
    df = pd.DataFrame()
    result = _normalize_columns(df)
    assert result.empty


def test_empty_trip_frame():
    result = _empty_trip_frame()
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_prepare_stop_times_df_missing_trip_id():
    df = pd.DataFrame({"stop_id": [1, 2], "arrival_time": ["08:00", "09:00"]})
    result = _prepare_stop_times_df(df, "test_dataset")
    assert result.empty


def test_prepare_stop_times_df_with_trip_id():
    df = pd.DataFrame({
        "trip_id": ["t1", "t2"],
        "stop_id": [1, 2],
        "stop_sequence": [1, 1]
    })
    result = _prepare_stop_times_df(df, "test_dataset")
    assert len(result) == 2
    assert "trip_id" in result.columns


def test_prepare_stop_times_df_sort_by_sequence():
    df = pd.DataFrame({
        "trip_id": ["t1", "t1", "t1"],
        "stop_sequence": [3, 1, 2],
        "stop_id": [3, 1, 2]
    })
    result = _prepare_stop_times_df(df, "test_dataset")
    assert result["stop_sequence"].tolist() == [1, 2, 3]


def test_prepare_stop_times_df_drop_decreasing_distance():
    df = pd.DataFrame({
        "trip_id": ["t1", "t1", "t1"],
        "stop_sequence": [1, 2, 3],
        "shape_dist_traveled": [0.0, 100.0, 50.0]
    })
    result = _prepare_stop_times_df(df, "test_dataset")
    assert len(result) < 3


def test_sanitize_dataframe_missing_trip_id():
    df = pd.DataFrame({
        "trip_id": [None, "t2", ""],
        "distance_km": [10, 20, 30],
        "duration_h": [1, 2, 3]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert len(result) <= 1


def test_sanitize_dataframe_invalid_duration():
    df = pd.DataFrame({
        "trip_id": ["t1", "t2", "t3"],
        "distance_km": [10, 20, 30],
        "duration_h": [1, 0, -1]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert len(result) <= 1


def test_sanitize_dataframe_invalid_distance():
    df = pd.DataFrame({
        "trip_id": ["t1", "t2", "t3"],
        "distance_km": [10, -5, 20],
        "duration_h": [1, 2, 3]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert len(result) <= 2


def test_sanitize_dataframe_negative_emissions():
    df = pd.DataFrame({
        "trip_id": ["t1", "t2"],
        "distance_km": [10, 20],
        "duration_h": [1, 2],
        "emission_gco2e_pkm": [10, -5]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert pd.isna(result["emission_gco2e_pkm"].iloc[1]) or result["emission_gco2e_pkm"].iloc[1] >= 0


def test_sanitize_dataframe_origin_equals_destination():
    df = pd.DataFrame({
        "trip_id": ["t1", "t2"],
        "distance_km": [10, 20],
        "duration_h": [1, 2],
        "origin_stop_name": ["Paris", "Lyon"],
        "destination_stop_name": ["Paris", "Lyon"]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert len(result) <= 1


def test_sanitize_dataframe_duplicate_trip_id():
    df = pd.DataFrame({
        "trip_id": ["t1", "t1", "t2"],
        "distance_km": [10, 10, 20],
        "duration_h": [1, 1, 2]
    })
    result = _sanitize_dataframe(df, "test_dataset")
    assert len(result) <= 2


def test_write_csv(tmp_path):
    rows = [
        {"trip_id": "t1", "route_name": "R1", "distance_km": 100},
        {"trip_id": "t2", "route_name": "R2", "distance_km": 200}
    ]
    out_csv = tmp_path / "output.csv"
    _write_csv(rows, out_csv)
    
    assert out_csv.exists()
    df = pd.read_csv(out_csv)
    assert len(df) == 2


def test_write_csv_empty_rows(tmp_path):
    out_csv = tmp_path / "output.csv"
    _write_csv([], out_csv)
    assert not out_csv.exists()


def test_resolve_dataset_output_id_fallback(tmp_path):
    staging_dir = tmp_path / "staging"
    staging_dir.mkdir()
    
    result = _resolve_dataset_output_id(str(staging_dir), "ds1")
    assert result == "ds1"


def test_resolve_dataset_output_id_with_metadata(tmp_path):
    with patch("transform_gtfs_data.latest_version_dir") as mock_latest:
        with patch("transform_gtfs_data.read_metadata") as mock_metadata:
            mock_latest.return_value = tmp_path / "v1"
            mock_metadata.return_value = {"dataset_id": "resolved_id"}
            
            result = _resolve_dataset_output_id(str(tmp_path), "ds1")
            assert result == "resolved_id"