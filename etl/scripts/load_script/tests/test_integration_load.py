import os
import sys
import types
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock , patch
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from load_script.fact_loader import upsert_dimensions_from_staging, load_fact_table


from load_script.helpers import sanitize_country_for_staging, get_staging_country_limits
from load_script.dimension_cache import DimensionCache
from load_script import dimension_loaders as dim_mod
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



class TestCacheLoadersIntegration:

    def _fresh_cache(self, monkeypatch):
        cache = DimensionCache(max_size=100)
        monkeypatch.setattr(dim_mod, "dim_cache", cache)
        return cache

    def test_first_call_misses_cache_second_hits(self, monkeypatch):
        cache = self._fresh_cache(monkeypatch)

        hook = MagicMock()
        hook.get_first.side_effect = [(42,)]

        r1 = dim_mod.load_dim_country(hook, "FR")
        assert r1 == 42
        assert cache.misses == 1
        assert cache.hits == 0

        hook.get_first.reset_mock()
        r2 = dim_mod.load_dim_country(hook, "FR")
        assert r2 == 42
        assert cache.hits == 1
        hook.get_first.assert_not_called()

    def test_cache_eviction_triggers_db_again(self, monkeypatch):
        cache = DimensionCache(max_size=1)
        monkeypatch.setattr(dim_mod, "dim_cache", cache)

        hook = MagicMock()
        hook.get_first.return_value = (1,)

        dim_mod.load_dim_country(hook, "FR")  
        dim_mod.load_dim_country(hook, "DE")

        assert cache.get("country_FR") is None
        assert cache.get("country_DE") == 1

    def test_multiple_dimension_types_share_cache(self, monkeypatch):
        cache = self._fresh_cache(monkeypatch)
        hook = MagicMock()
        hook.get_first.return_value = (99,)

        dim_mod.load_dim_country(hook, "FR")
        dim_mod.load_dim_traction(hook, "électrique")
        dim_mod.load_dim_train_type(hook, "Grande vitesse")

        assert cache.get("country_FR") == 99
        assert cache.get("traction_électrique") == 99
        assert cache.get("train_type_Grande vitesse") == 99

    def test_cache_cleared_between_datasets(self, monkeypatch):
        cache = self._fresh_cache(monkeypatch)
        hook = MagicMock()
        hook.get_first.return_value = (5,)

        dim_mod.load_dim_country(hook, "FR")
        assert cache.get("country_FR") == 5

        cache.clear()

        assert "country_FR" not in cache.cache
        assert cache.hits == 0
        assert cache.misses == 0

    def test_location_with_valid_country_in_db(self, monkeypatch):
        cache = self._fresh_cache(monkeypatch)
        print("DEBUG: cache initial:", cache.cache)
        hook = MagicMock()
        hook.get_first.side_effect = [(1,), None, (88,)]

        result = dim_mod.load_dim_location(hook, "Paris Gare", "FR")

        assert result == 88
        insert_params = hook.run.call_args.kwargs["parameters"]
        assert insert_params == ("Paris Gare", "FR")

    def test_location_with_invalid_country_sets_null(self, monkeypatch):
        cache = self._fresh_cache(monkeypatch)
        print("DEBUG: cache initial:", cache.cache)
        hook = MagicMock()
        hook.get_first.side_effect = [None, None, (77,)]

        result = dim_mod.load_dim_location(hook, "Unknown Stop", "XX")

        assert result == 77
        insert_params = hook.run.call_args.kwargs["parameters"]
        assert insert_params == ("Unknown Stop", None)


class TestStagingFactLoaderIntegration:

    def test_upsert_called_with_same_load_id_as_staging(self, tmp_path):
        load_id = 999
        hook = MagicMock()
        hook.get_first.return_value = (5,)

        with patch("load_script.fact_loader.upsert_dimensions_from_staging") as mock_upsert:
            result = load_fact_table(hook, load_id)
        print(f"DEBUG: {result if result is not None else 'None'}")

        mock_upsert.assert_called_once_with(hook, load_id)
        fact_params = hook.run.call_args.kwargs["parameters"]
        assert load_id in fact_params

    def test_staging_then_fact_full_flow(self, tmp_path):
        csv_path = tmp_path / "trips.csv"
        rows = [_make_row(trip_id=f"T{i}") for i in range(3)]
        _write_csv(csv_path, rows)

        hook = MagicMock()
        staged = load_staging_table(hook, csv_path, 1, 42, 30, 30)
        assert staged == 3

        hook.get_first.return_value = (3,)
        with patch("load_script.fact_loader.upsert_dimensions_from_staging"):
            count = load_fact_table(hook, 1)

        assert count == 3

    def test_upsert_dimensions_sends_load_id_to_all_tables(self):
        hook = MagicMock()
        load_id = 77

        upsert_dimensions_from_staging(hook, load_id)

        for c in hook.run.call_args_list:
            params = c.kwargs.get("parameters", ())
            assert load_id in params, f"load_id absent des params: {params}"

    def test_fact_loader_count_zero_when_no_staging_data(self):
        hook = MagicMock()
        hook.get_first.return_value = (0,)

        with patch("load_script.fact_loader.upsert_dimensions_from_staging"):
            result = load_fact_table(hook, 1)

        assert result == 0
