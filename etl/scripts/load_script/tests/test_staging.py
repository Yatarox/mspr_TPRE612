import importlib
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)


def _install_airflow_stub():
    airflow = types.ModuleType("airflow")
    providers = types.ModuleType("airflow.providers")
    mysql = types.ModuleType("airflow.providers.mysql")
    hooks = types.ModuleType("airflow.providers.mysql.hooks")
    mysql_hook_mod = types.ModuleType("airflow.providers.mysql.hooks.mysql")

    class MySqlHook:
        pass

    mysql_hook_mod.MySqlHook = MySqlHook
    sys.modules["airflow"] = airflow
    sys.modules["airflow.providers"] = providers
    sys.modules["airflow.providers.mysql"] = mysql
    sys.modules["airflow.providers.mysql.hooks"] = hooks
    sys.modules["airflow.providers.mysql.hooks.mysql"] = mysql_hook_mod


def _get_load_staging_table():
    _install_airflow_stub()
    mod = importlib.import_module("load_script.staging")
    return mod.load_staging_table


def _write_csv(path: Path):
    df = pd.DataFrame(
        [
            {
                "trip_id": "t1",
                "route_name": "R1 - Paris-Lyon",
                "agency_name": "A1:SNCF",
                "service_type": "Régional",
                "origin_stop_name": "Paris",
                "origin_country": "FR",
                "destination_stop_name": "Lyon",
                "destination_country": "FR",
                "departure_time": "08:00:00",
                "arrival_time": "10:00:00",
                "distance_km": "100.5",
                "duration_h": "2.0",
                "train_type": "TER",
                "traction": "électrique",
                "emission_gco2e_pkm": "12.3",
                "total_emission_kgco2e": "45.6",
                "frequency_per_week": "7",
            }
        ]
    )
    df.to_csv(path, index=False, encoding="utf-8")


def test_load_staging_table_csv_not_found_returns_zero(tmp_path):
    load_staging_table = _get_load_staging_table()
    hook = MagicMock()
    out = load_staging_table(
        hook=hook,
        csv_path=tmp_path / "missing.csv",
        load_id=1,
        dataset_id=10,
        origin_max_len=5,
        dest_max_len=5,
    )
    assert out == 0
    hook.run.assert_not_called()


def test_load_staging_table_one_valid_row(tmp_path):
    load_staging_table = _get_load_staging_table()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch("load_script.staging.validate_row", return_value=(True, None)), patch(
        "load_script.staging.sanitize_country_for_staging", side_effect=lambda v, *_: v
    ):
        hook = MagicMock()
        out = load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=99,
            dataset_id=77,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert out == 1
    assert hook.run.call_count == 2

    insert_sql = hook.run.call_args_list[1].args[0]
    flat_params = hook.run.call_args_list[1].kwargs["parameters"]

    assert "INSERT INTO stg_trips_summary" in insert_sql
    assert len(flat_params) == 23
    assert flat_params[0] == 99
    assert flat_params[2] == 77
    assert flat_params[3] == "t1"
    assert flat_params[4] == "R1"
    assert flat_params[6] == "A1"


def test_load_staging_table_invalid_rows_skipped(tmp_path):
    load_staging_table = _get_load_staging_table()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch("load_script.staging.validate_row", return_value=(False, "invalid")):
        hook = MagicMock()
        out = load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=1,
            dataset_id=1,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert out == 0
    assert hook.run.call_count == 1


def test_load_staging_table_truncate_error_does_not_block_loading(tmp_path):
    load_staging_table = _get_load_staging_table()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch("load_script.staging.validate_row", return_value=(True, None)), patch(
        "load_script.staging.sanitize_country_for_staging", side_effect=lambda v, *_: v
    ):
        hook = MagicMock()
        hook.run.side_effect = [Exception("truncate failed"), None]

        out = load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=2,
            dataset_id=3,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert out == 1
    assert hook.run.call_count == 2