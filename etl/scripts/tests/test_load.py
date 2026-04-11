import importlib
import os
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
)


def _install_airflow_stub():
    airflow = types.ModuleType("airflow")
    exceptions = types.ModuleType("airflow.exceptions")
    providers = types.ModuleType("airflow.providers")
    mysql = types.ModuleType("airflow.providers.mysql")
    hooks = types.ModuleType("airflow.providers.mysql.hooks")
    mysql_hook_mod = types.ModuleType("airflow.providers.mysql.hooks.mysql")

    class AirflowException(Exception):
        pass

    class MySqlHook:
        def __init__(self, mysql_conn_id=None):
            self.mysql_conn_id = mysql_conn_id

    exceptions.AirflowException = AirflowException
    mysql_hook_mod.MySqlHook = MySqlHook
    airflow.exceptions = exceptions
    airflow.providers = providers

    sys.modules["airflow"] = airflow
    sys.modules["airflow.exceptions"] = exceptions
    sys.modules["airflow.providers"] = providers
    sys.modules["airflow.providers.mysql"] = mysql
    sys.modules["airflow.providers.mysql.hooks"] = hooks
    sys.modules["airflow.providers.mysql.hooks.mysql"] = mysql_hook_mod


def _get_load_gtfs():
    _install_airflow_stub()
    mod = importlib.import_module("load_gtfs")
    return mod.load_gtfs


def test_load_gtfs_directory_not_found(tmp_path):
    load_gtfs = _get_load_gtfs()
    with pytest.raises(Exception) as exc_info:
        load_gtfs(str(tmp_path / "nonexistent"))
    assert "Directory not found" in str(exc_info.value)


def test_load_gtfs_no_datasets(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    processed_dir.mkdir()

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with pytest.raises(Exception) as exc_info:
            load_gtfs(str(processed_dir))
        assert "No data loaded" in str(exc_info.value)


def test_load_gtfs_dataset_with_no_csv(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    dataset_dir = processed_dir / "1"
    dataset_dir.mkdir(parents=True)

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with pytest.raises(Exception) as exc_info:
                load_gtfs(str(processed_dir))
            assert "No data loaded" in str(exc_info.value)


def test_load_gtfs_single_dataset_success(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    dataset_dir = processed_dir / "123"
    dataset_dir.mkdir(parents=True)

    csv_path = dataset_dir / "trips_summary_123.csv"
    csv_path.write_text("trip_id,route_name\nt1,R1\n")

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with patch("load_gtfs.load_staging_table", return_value=10):
                with patch("load_gtfs.load_fact_table", return_value=10):
                    with patch("load_gtfs.dim_cache") as mock_cache:
                        result = load_gtfs(str(processed_dir))

                        assert result["total_rows"] == 10
                        assert result["datasets"] == 1
                        mock_cache.clear.assert_called()


def test_load_gtfs_multiple_datasets(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"

    for dataset_id in [1, 2, 3]:
        dataset_dir = processed_dir / str(dataset_id)
        dataset_dir.mkdir(parents=True)
        csv_path = dataset_dir / f"trips_summary_{dataset_id}.csv"
        csv_path.write_text("trip_id,route_name\nt1,R1\n")

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with patch("load_gtfs.load_staging_table", return_value=100):
                with patch("load_gtfs.load_fact_table", return_value=100):
                    with patch("load_gtfs.dim_cache") as mock_cache:
                        result = load_gtfs(str(processed_dir))

                        assert result["total_rows"] == 300
                        assert result["datasets"] == 3
                        assert mock_cache.clear.call_count == 3


def test_load_gtfs_fallback_csv_name(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    dataset_dir = processed_dir / "999"
    dataset_dir.mkdir(parents=True)

    csv_path = dataset_dir / "trips_summary.csv"
    csv_path.write_text("trip_id,route_name\nt1,R1\n")

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with patch("load_gtfs.load_staging_table", return_value=50):
                with patch("load_gtfs.load_fact_table", return_value=50):
                    with patch("load_gtfs.dim_cache"):
                        result = load_gtfs(str(processed_dir))

                        assert result["total_rows"] == 50
                        assert result["datasets"] == 1


def test_load_gtfs_staging_returns_zero(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    dataset_dir = processed_dir / "555"
    dataset_dir.mkdir(parents=True)

    csv_path = dataset_dir / "trips_summary_555.csv"
    csv_path.write_text("trip_id,route_name\nt1,R1\n")

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with patch("load_gtfs.load_staging_table", return_value=0):
                with patch("load_gtfs.dim_cache"):
                    with pytest.raises(Exception) as exc_info:
                        load_gtfs(str(processed_dir))
                    assert "No data loaded" in str(exc_info.value)


def test_load_gtfs_dataset_id_as_uuid(tmp_path):
    load_gtfs = _get_load_gtfs()
    processed_dir = tmp_path / "processed"
    dataset_dir = processed_dir / "abc-def-ghi"
    dataset_dir.mkdir(parents=True)

    csv_path = dataset_dir / "trips_summary.csv"
    csv_path.write_text("trip_id,route_name\nt1,R1\n")

    with patch("load_gtfs.MySqlHook") as mock_hook_class:
        mock_hook = MagicMock()
        mock_hook_class.return_value = mock_hook

        with patch("load_gtfs.get_staging_country_limits", return_value=(3, 3)):
            with patch("load_gtfs.load_staging_table", return_value=25):
                with patch("load_gtfs.load_fact_table", return_value=25):
                    with patch("load_gtfs.dim_cache"):
                        result = load_gtfs(str(processed_dir))

                        assert result["total_rows"] == 25
                        assert result["datasets"] == 1