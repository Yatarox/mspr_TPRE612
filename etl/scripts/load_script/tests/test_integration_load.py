import os
import sys
import types
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))


# from load_script.dimension_cache import DimensionCache
# from load_script import dimension_loaders as dim_mod
from load_script.staging import load_staging_table


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
        assert hook.run.call_count == 1  # uniquement TRUNCATE


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

