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


def _get_staging_module():
    _install_airflow_stub()
    return importlib.import_module("load_script.staging")


def _write_csv(path: Path, rows: list = None):
    if rows is None:
        rows = [{
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
        }]
    df = pd.DataFrame(rows)
    df.to_csv(path, index=False, encoding="utf-8")


def test_load_staging_table_csv_not_found(tmp_path):
    mod = _get_staging_module()
    hook = MagicMock()
    result = mod.load_staging_table(
        hook=hook,
        csv_path=tmp_path / "missing.csv",
        load_id=1,
        dataset_id=10,
        origin_max_len=5,
        dest_max_len=5,
    )
    assert result == 0
    hook.run.assert_not_called()


def test_load_staging_table_one_valid_row(tmp_path):
    mod = _get_staging_module()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch.object(mod, "validate_row", return_value=(True, None)), \
         patch.object(mod, "sanitize_country_for_staging", side_effect=lambda v, *_: v):
        hook = MagicMock()
        result = mod.load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=99,
            dataset_id=77,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert result == 1
    assert hook.run.call_count == 2


def test_load_staging_table_invalid_rows_skipped(tmp_path):
    mod = _get_staging_module()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch.object(mod, "validate_row", return_value=(False, "invalid")):
        hook = MagicMock()
        result = mod.load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=1,
            dataset_id=1,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert result == 0
    assert hook.run.call_count == 1


def test_load_staging_table_truncate_error_continues(tmp_path):
    mod = _get_staging_module()
    csv_path = tmp_path / "input.csv"
    _write_csv(csv_path)

    with patch.object(mod, "validate_row", return_value=(True, None)), \
         patch.object(mod, "sanitize_country_for_staging", side_effect=lambda v, *_: v):
        hook = MagicMock()
        hook.run.side_effect = [Exception("truncate failed"), None]
        result = mod.load_staging_table(
            hook=hook,
            csv_path=csv_path,
            load_id=2,
            dataset_id=3,
            origin_max_len=5,
            dest_max_len=5,
        )

    assert result == 1
    assert hook.run.call_count == 2


def test_extract_route_id():
    mod = _get_staging_module()
    assert mod._extract_route_id("R1 - Paris-Lyon") == "R1"
    assert mod._extract_route_id("R2") == "R2"


def test_extract_agency_id():
    mod = _get_staging_module()
    assert mod._extract_agency_id("A1:SNCF") == "A1"
    assert mod._extract_agency_id("SNCF") == "SNCF"


def test_parse_row_to_tuple(tmp_path):
    mod = _get_staging_module()
    row = pd.Series({
        "trip_id": "t1",
        "route_name": "R1 - Paris",
        "agency_name": "A1:SNCF",
        "service_type": "Régional",
        "origin_stop_name": "Paris",
        "destination_stop_name": "Lyon",
        "departure_time": "08:00",
        "arrival_time": "10:00",
        "distance_km": "100.5",
        "duration_h": "2.0",
        "train_type": "TER",
        "traction": "électrique",
        "emission_gco2e_pkm": "12.3",
        "total_emission_kgco2e": "45.6",
        "frequency_per_week": "7",
    })
    result = mod._parse_row_to_tuple(row, 1, 10, "FR", "FR")
    assert len(result) == 23
    assert result[3] == "t1"
    assert result[4] == "R1"
    assert result[6] == "A1"