import os
import sys
import types
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock , patch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from load_script.helpers import sanitize_country_for_staging, get_staging_country_limits
# from load_script.dimension_cache import DimensionCache
# from load_script import dimension_loaders as dim_mod
from load_script.staging import (
    load_staging_table, _extract_route_id, _extract_agency_id, _parse_row_to_tuple
)

def _install_airflow_stubs():
    for mod_name in [
        "airflow", "airflow.providers", "airflow.providers.mysql",
        "airflow.providers.mysql.hooks", "airflow.providers.mysql.hooks.mysql",
        "airflow.exceptions",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = types.ModuleType(mod_name)

    class MySqlHook:
        pass

    class AirflowException(Exception):
        pass

    sys.modules["airflow.providers.mysql.hooks.mysql"].MySqlHook = MySqlHook
    sys.modules["airflow.exceptions"].AirflowException = AirflowException

_install_airflow_stubs()




# ── Helpers de test ───────────────────────────────────────────────────────────

def _make_row(**kwargs):
    base = {
        "trip_id": "T1",
        "agency_name": "SNCF",
        "route_name": "R1 - Paris-Lyon",
        "origin_stop_name": "Paris Gare de Lyon",
        "destination_stop_name": "Lyon Part-Dieu",
        "origin_country": "FR",
        "destination_country": "FR",
        "service_type": "JOUR",
        "departure_time": "08:00:00",
        "arrival_time": "10:00:00",
        "distance_km": "390.5",
        "duration_h": "2.0",
        "train_type": "Grande vitesse",
        "traction": "électrique",
        "emission_gco2e_pkm": "3.2",
        "total_emission_kgco2e": "1.249",
        "frequency_per_week": "14",
    }
    base.update(kwargs)
    return base

def _write_csv(path: Path, rows=None):
    rows = rows or [_make_row()]
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8")

def _fake_hook(get_first_side_effect=None, get_first_return=None):
    hook = MagicMock()
    if get_first_side_effect:
        hook.get_first.side_effect = get_first_side_effect
    elif get_first_return is not None:
        hook.get_first.return_value = get_first_return
    return hook


# ── validation ↔ staging ─────────────────────────────────────────────────────

class TestValidationStagingIntegration:

    def test_valid_row_passes_through_to_insert(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path)

        hook = _fake_hook()
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 1
        assert hook.run.call_count == 2

    def test_invalid_trip_id_skipped_no_insert(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, [_make_row(trip_id=float("nan"))])

        hook = _fake_hook()
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 0
        assert hook.run.call_count == 1  


    def test_invalid_distance_skipped(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, [_make_row(distance_km="-10")])

        hook = _fake_hook()
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 0

    def test_error_field_rejected(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, [_make_row(origin_stop_name="ERROR")])

        hook = _fake_hook()
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 0

    def test_mixed_valid_invalid_rows(self, tmp_path):

        rows = [
            _make_row(trip_id="T1"),
            _make_row(trip_id=float("nan")),
            _make_row(trip_id="T3"),
        ]
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, rows)

        hook = _fake_hook()
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 2

# ── helpers ↔ staging ─────────────────────────────────────────────────────────

class TestHelpersStagingIntegration:

    def test_sanitize_removes_invalid_country_before_insert(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, [_make_row(origin_country="UNKNOWN", destination_country="FR")])

        inserted_tuples = []
        original_insert = MagicMock()

        print("DEBUG: hook.run.call_args_list avant patch:", original_insert.call_args_list)

        def capture_run(sql, parameters=None):
            if "INSERT INTO stg_trips_summary" in str(sql):
                inserted_tuples.append(parameters)
            return None

        hook = MagicMock()
        hook.run.side_effect = capture_run
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 1
        flat = inserted_tuples[0]
        assert flat[10] is None

    def test_date_like_country_sanitized_to_none(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        _write_csv(csv_path, [_make_row(origin_country="2025-01-01")])

        inserted_tuples = []
        def capture_run(sql, parameters=None):
            if "INSERT INTO stg_trips_summary" in str(sql):
                inserted_tuples.append(parameters)

        hook = MagicMock()
        hook.run.side_effect = capture_run
        result = load_staging_table(hook, csv_path, 1, 42, 30, 30)

        assert result == 1
        assert inserted_tuples[0][10] is None

    def test_get_staging_country_limits_used_for_truncation(self, tmp_path):
        long_country = "FRANCEGERMANYITALY"  # 18 chars

        hook = MagicMock()
        with patch("load_script.helpers.get_column_max_length", return_value=5):
            o_len, d_len = get_staging_country_limits(hook)

        result = sanitize_country_for_staging(long_country, o_len, "test")
        assert result == "FRANC"
        assert len(result) == 5

    def test_parse_row_to_tuple_extracts_route_and_agency_ids(self):
        row = pd.Series(_make_row())
        result = _parse_row_to_tuple(row, 1, 42, "FR", "FR")

        route_id = result[4]
        agency_id = result[6]

        assert route_id == _extract_route_id("R1 - Paris-Lyon")
        assert agency_id == _extract_agency_id("SNCF")
        assert route_id == "R1"
        assert agency_id == "SNCF"

    def test_parse_row_numeric_fields_converted(self):
        row = pd.Series(_make_row())
        result = _parse_row_to_tuple(row, 1, 42, "FR", "FR")

        assert isinstance(result[15], float)   
        assert isinstance(result[16], float)   
        assert isinstance(result[20], float)  
        assert isinstance(result[21], float)  
        assert isinstance(result[22], int)   


