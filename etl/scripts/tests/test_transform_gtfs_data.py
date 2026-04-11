import importlib
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def _get_tmod():
    return importlib.import_module("transform_gtfs_data")


def test_build_trips_summary_no_version_dir(tmp_path):
    tmod = _get_tmod()
    with patch.object(tmod, "latest_version_dir", return_value=None):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )
    assert count == 0
    assert out == ""


def test_build_trips_summary_empty_trips_or_stop_times(tmp_path):
    tmod = _get_tmod()
    latest = tmp_path / "staging" / "ds1" / "v1"
    latest.mkdir(parents=True)

    def _fake_read_csv(path):
        name = Path(path).name
        if name == "agency.txt":
            return pd.DataFrame({"agency_id": ["A1"], "agency_name": ["SNCF"]})
        if name == "routes.txt":
            return pd.DataFrame({"route_id": ["R1"], "agency_id": ["A1"]})
        if name == "stops.txt":
            return pd.DataFrame({"stop_id": ["s1"], "stop_name": ["Paris"]})
        if name == "trips.txt":
            return pd.DataFrame()  # force empty
        if name == "stop_times.txt":
            return pd.DataFrame({"trip_id": ["t1"], "stop_sequence": [1], "stop_id": ["s1"]})
        return pd.DataFrame()

    with patch.object(tmod, "latest_version_dir", return_value=latest), patch.object(
        tmod, "read_metadata", return_value={"dataset_id": "ds1"}
    ), patch.object(tmod, "read_csv", side_effect=_fake_read_csv):
        count, out = tmod.build_trips_summary_for_dataset(
            str(tmp_path / "staging"), "ds1", str(tmp_path / "processed")
        )

    assert count == 0
    assert out == ""


def test_transform_gtfs_skip_existing_returns_existing_files(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "ds1").mkdir(parents=True)

    out = processed / "ds1" / "trips_summary_ds1.csv"
    out.parent.mkdir(parents=True)
    out.write_text("trip_id\n", encoding="utf-8")

    with patch.object(tmod, "_resolve_dataset_output_id", return_value="ds1"):
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=True)

    assert str(out) in written


def test_transform_gtfs_process_pool_success_and_empty(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)
    (staging / "b").mkdir(parents=True)

    fut_ok = MagicMock()
    fut_ok.result.return_value = (3, str(processed / "a" / "trips_summary_a.csv"))

    fut_empty = MagicMock()
    fut_empty.result.return_value = (0, "")

    executor = MagicMock()
    executor.submit.side_effect = [fut_ok, fut_empty]

    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, patch.object(
        tmod, "as_completed", return_value=[fut_ok, fut_empty]
    ):
        pool_cls.return_value.__enter__.return_value = executor
        written = tmod.transform_gtfs(str(staging), str(processed), max_workers=4, skip_existing=False)

    assert len(written) == 1
    assert "trips_summary_a.csv" in written[0]
    pool_cls.assert_called_once_with(max_workers=2)


def test_transform_gtfs_timeout_branch(tmp_path):
    tmod = _get_tmod()
    staging = tmp_path / "staging"
    processed = tmp_path / "processed"
    (staging / "a").mkdir(parents=True)

    fut_timeout = MagicMock()
    fut_timeout.result.side_effect = tmod.FuturesTimeoutError()

    executor = MagicMock()
    executor.submit.return_value = fut_timeout

    with patch.object(tmod, "ProcessPoolExecutor") as pool_cls, patch.object(
        tmod, "as_completed", return_value=[fut_timeout]
    ):
        pool_cls.return_value.__enter__.return_value = executor
        written = tmod.transform_gtfs(str(staging), str(processed), skip_existing=False)

    assert written == []