import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _get_tmod():
    return importlib.import_module("transform_gtfs_data")


def _fake_read_csv_minimal(path):
    name = Path(path).name
    if name == "agency.txt":
        return pd.DataFrame({"agency_id": ["A1"], "agency_name": ["SNCF"]})
    if name == "routes.txt":
        return pd.DataFrame({"route_id": ["R1"], "agency_id": ["A1"], "route_name": ["R1"]})
    if name == "stops.txt":
        return pd.DataFrame({"stop_id": ["s1", "s2"], "stop_name": ["Paris", "Lyon"]})
    if name == "trips.txt":
        return pd.DataFrame({"trip_id": ["t1"], "route_id": ["R1"], "service_id": ["svc1"]})
    if name == "stop_times.txt":
        return pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})
    if name == "calendar.txt":
        return pd.DataFrame({"service_id": ["svc1"]})
    return pd.DataFrame()


def test_build_trips_summary_stop_times_empty_after_prepare(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", return_value={"dataset_id": "ds1"}), \
         patch.object(tmod, "read_csv", side_effect=_fake_read_csv_minimal), \
         patch.object(tmod, "_prepare_stop_times_df", return_value=pd.DataFrame()):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_build_trips_summary_trips_missing_trip_id(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    def _read_csv(path):
        name = Path(path).name
        if name == "trips.txt":
            return pd.DataFrame({"route_id": ["R1"], "service_id": ["svc1"]})  # no trip_id
        return _fake_read_csv_minimal(path)

    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", return_value={"dataset_id": "ds1"}), \
         patch.object(tmod, "read_csv", side_effect=_read_csv), \
         patch.object(tmod, "_prepare_stop_times_df", return_value=pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})), \
         patch.object(tmod, "compute_distances", return_value=pd.Series({"t1": 10.0})), \
         patch.object(tmod, "compute_durations", return_value=pd.Series({"t1": 60.0})):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_build_trips_summary_no_rows_generated(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", return_value={"dataset_id": "meta1"}), \
         patch.object(tmod, "read_csv", side_effect=_fake_read_csv_minimal), \
         patch.object(tmod, "_prepare_stop_times_df", return_value=pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})), \
         patch.object(tmod, "build_stop_country_map", return_value={"s1": "FR", "s2": "FR"}), \
         patch.object(tmod, "compute_distances", return_value=pd.Series({"t1": 10.0})), \
         patch.object(tmod, "compute_durations", return_value=pd.Series({"t1": 60.0})), \
         patch.object(tmod, "build_frequency_map", return_value={"t1": 7}), \
         patch.object(tmod, "_process_trips_chunk", return_value=None):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_build_trips_summary_sanitize_empty(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    def _fake_process_chunk(*args):
        all_rows = args[-1]
        all_rows.append({"trip_id": "t1", "distance_km": 10.0, "duration_h": 1.0})

    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", return_value={"dataset_id": "meta1"}), \
         patch.object(tmod, "read_csv", side_effect=_fake_read_csv_minimal), \
         patch.object(tmod, "_prepare_stop_times_df", return_value=pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})), \
         patch.object(tmod, "build_stop_country_map", return_value={"s1": "FR", "s2": "FR"}), \
         patch.object(tmod, "compute_distances", return_value=pd.Series({"t1": 10.0})), \
         patch.object(tmod, "compute_durations", return_value=pd.Series({"t1": 60.0})), \
         patch.object(tmod, "build_frequency_map", return_value={"t1": 7}), \
         patch.object(tmod, "_process_trips_chunk", side_effect=_fake_process_chunk), \
         patch.object(tmod, "_sanitize_dataframe", return_value=pd.DataFrame()):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_build_trips_summary_exception_branch(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    with patch.object(tmod, "latest_version_dir", return_value=latest), \
         patch.object(tmod, "read_metadata", side_effect=RuntimeError("boom")):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_transform_gtfs_staging_not_found(tmp_path):
    tmod = _get_tmod()
    written = tmod.transform_gtfs(
        str(tmp_path / "missing_staging"),
        str(tmp_path / "processed"),
    )
    assert written == []


def test_transform_gtfs_no_datasets(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    staging.mkdir(parents=True)

    written = tmod.transform_gtfs(str(staging), str(tmp_path / "processed"))
    assert written == []


def test_transform_gtfs_all_already_processed(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "ds1").mkdir(parents=True)
    (processed / "out1").mkdir(parents=True)
    existing = processed / "out1" / "trips_summary_out1.csv"
    existing.write_text("trip_id\n", encoding="utf-8")

    with patch.object(tmod, "_resolve_dataset_output_id", return_value="out1"):
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=True)

    assert str(existing) in written


def test_transform_gtfs_future_generic_exception(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    (staging / "a").mkdir(parents=True)

    fut_err = MagicMock()
    fut_err.result.side_effect = RuntimeError("worker failed")

    executor = MagicMock()
    executor.submit.return_value = fut_err

    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, \
         patch.object(tmod, "as_completed", return_value=[fut_err]):
        pool_cls.return_value.__enter__.return_value = executor
        written = tmod.transform_gtfs(str(staging), str(tmp_path / "processed"), skip_existing=False)

    assert written == []


def test_transform_gtfs_broken_pool_fallback(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)

    with patch.object(tmod, "ProcessPoolExecutor", side_effect=tmod.BrokenProcessPool()), \
         patch.object(tmod, "build_trips_summary_for_dataset", return_value=(2, str(processed / "a.csv"))):
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=False)

    assert written == [str(processed / "a.csv")]