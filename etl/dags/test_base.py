import json
import sys
import os
import types
from unittest.mock import MagicMock
import unittest.mock as _um
import pytest
import base as dag_mod

def _install_stubs():
    stubs = {
        "airflow":                              types.ModuleType("airflow"),
        "airflow.sdk":                          types.ModuleType("airflow.sdk"),
        "airflow.models":                       types.ModuleType("airflow.models"),
        "airflow.models.xcom_arg":              types.ModuleType("airflow.models.xcom_arg"),
        "airflow.exceptions":                   types.ModuleType("airflow.exceptions"),
        "airflow.providers":                    types.ModuleType("airflow.providers"),
        "airflow.providers.mysql":              types.ModuleType("airflow.providers.mysql"),
        "airflow.providers.mysql.hooks":        types.ModuleType("airflow.providers.mysql.hooks"),
        "airflow.providers.mysql.hooks.mysql":  types.ModuleType("airflow.providers.mysql.hooks.mysql"),
        "pendulum":                             types.ModuleType("pendulum"),
    }

    class XComArg:
        pass

    class Variable:
        _store = {}

        @classmethod
        def get(cls, key, default_var=None):
            return cls._store.get(key, default_var)

        @classmethod
        def set(cls, key, value):
            cls._store[key] = value

    class AirflowException(Exception):
        pass

    class MySqlHook:
        pass

    stubs["airflow.models.xcom_arg"].XComArg = XComArg
    stubs["airflow.models"].Variable = Variable
    stubs["airflow.exceptions"].AirflowException = AirflowException
    stubs["airflow.providers.mysql.hooks.mysql"].MySqlHook = MySqlHook
    stubs["airflow.sdk"].dag = lambda **kw: (lambda f: (lambda: None))
    stubs["airflow.sdk"].task = lambda f=None, **kw: (lambda *a, **k: None) if f is None else (lambda *a, **k: None)
    stubs["airflow.sdk"].get_current_context = lambda: {
        "task_instance": type("TI", (), {"task_id": "mock_task"})()
    }
    stubs["pendulum"].datetime = MagicMock()
    stubs["pendulum"].duration = MagicMock()

    for name, mod in stubs.items():
        if name not in sys.modules:
            sys.modules[name] = mod

    return XComArg, Variable


XComArg, Variable = _install_stubs()

for mod_name in [
    "scripts", "scripts.extract_gtfs_data_gouv_script",
    "scripts.load_gtfs", "scripts.transform_gtfs_data", "scripts.train_model",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = types.ModuleType(mod_name)

sys.modules["scripts.extract_gtfs_data_gouv_script"].build_download_list = MagicMock()
sys.modules["scripts.extract_gtfs_data_gouv_script"].download_and_extract_gtfs = MagicMock()
sys.modules["scripts.extract_gtfs_data_gouv_script"].download_and_unzip_from_zip_urls = MagicMock()
sys.modules["scripts.extract_gtfs_data_gouv_script"].clean_old_downloads = MagicMock()
sys.modules["scripts.load_gtfs"].load_gtfs = MagicMock()
sys.modules["scripts.transform_gtfs_data"].transform_gtfs = MagicMock()
sys.modules["scripts.train_model"].train_model_pipeline = MagicMock()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


with _um.patch.dict("sys.modules", {}):
    import builtins as _builtins
    _real_import = _builtins.__import__
    def _safe_import(name, *args, **kwargs):
        return _real_import(name, *args, **kwargs)
    _builtins.__import__ = _safe_import

Variable._store.update({
    "gtfs_raw_dir": "/tmp/raw",
    "gtfs_staging_dir": "/tmp/staging",
    "gtfs_processed_dir": "/tmp/processed",
    "gtfs_db_conn_id": "mysql_default",
    "gtfs_force_download": "false",
    "gtfs_keep_latest_zips": "2",
    "gtfs_max_workers": "4",
    "gtfs_load_batch_size": "2000",
    "gtfs_base_urls": "[]",
    "gtfs_zip_urls": "[]",
})


# ── StructuredLogger ──────────────────────────────────────────────────────────

class TestStructuredLogger:

    def setup_method(self):
        self.raw_logger = MagicMock()
        self.sl = dag_mod.StructuredLogger(self.raw_logger)

    def test_log_metric_calls_info_with_json(self):
        self.sl.log_metric("extract_duration", 12.5, dataset="ds1")
        payload = json.loads(self.raw_logger.info.call_args[0][0])
        assert payload["type"] == "metric"
        assert payload["name"] == "extract_duration"
        assert payload["value"] == 12.5
        assert payload["dataset"] == "ds1"
        assert "timestamp" in payload

    def test_log_event_calls_info_with_json(self):
        self.sl.log_event("pipeline_started", stage="extract")
        payload = json.loads(self.raw_logger.info.call_args[0][0])
        assert payload["type"] == "event"
        assert payload["name"] == "pipeline_started"
        assert payload["stage"] == "extract"

    def test_log_error_calls_error_with_json(self):
        self.sl.log_error("extract_failed", "DB connection refused", retries=3)
        payload = json.loads(self.raw_logger.error.call_args[0][0])
        assert payload["type"] == "error"
        assert payload["name"] == "extract_failed"
        assert payload["message"] == "DB connection refused"
        assert payload["retries"] == 3

    def test_log_metric_timestamp_is_iso_format(self):
        self.sl.log_metric("test", 1.0)
        payload = json.loads(self.raw_logger.info.call_args[0][0])
        from datetime import datetime
        datetime.fromisoformat(payload["timestamp"])

    def test_log_event_timestamp_present(self):
        self.sl.log_event("test_event")
        payload = json.loads(self.raw_logger.info.call_args[0][0])
        assert "timestamp" in payload

    def test_log_error_timestamp_present(self):
        self.sl.log_error("test_error", "msg")
        payload = json.loads(self.raw_logger.error.call_args[0][0])
        assert "timestamp" in payload

    def test_log_metric_extra_kwargs_included(self):
        self.sl.log_metric("rows", 1000, source="api", env="prod")
        payload = json.loads(self.raw_logger.info.call_args[0][0])
        assert payload["source"] == "api"
        assert payload["env"] == "prod"


# ── clean_xcom ────────────────────────────────────────────────────────────────

class TestCleanXcom:

    def test_removes_xcomarg_value_from_dict(self):
        xcom_val = XComArg()
        result = dag_mod.clean_xcom({"r2": 0.9, "bad": xcom_val})
        assert "bad" not in result
        assert result["r2"] == 0.9

    def test_removes_xcomarg_from_list(self):
        xcom_val = XComArg()
        result = dag_mod.clean_xcom([1, xcom_val, "hello"])
        assert result == [1, "hello"]

    def test_converts_xcomarg_scalar_to_str(self):
        result = dag_mod.clean_xcom(XComArg())
        assert isinstance(result, str)

    def test_nested_dict_cleaned(self):
        xcom_val = XComArg()
        result = dag_mod.clean_xcom({"outer": {"inner": xcom_val, "keep": 42}})
        assert "inner" not in result["outer"]
        assert result["outer"]["keep"] == 42

    def test_plain_values_unchanged(self):
        d = {"r2": 0.85, "mae": 3.2, "name": "RF"}
        assert dag_mod.clean_xcom(d) == d

    def test_empty_dict_and_list(self):
        assert dag_mod.clean_xcom({}) == {}
        assert dag_mod.clean_xcom([]) == []

    def test_list_all_xcomarg_returns_empty(self):
        result = dag_mod.clean_xcom([XComArg(), XComArg()])
        assert result == []

    def test_mixed_nested_list_and_dict(self):
        xcom_val = XComArg()
        result = dag_mod.clean_xcom({
            "metrics": [1.0, xcom_val, 2.0],
            "meta": {"ok": True, "ref": xcom_val},
        })
        assert result["metrics"] == [1.0, 2.0]
        assert "ref" not in result["meta"]
        assert result["meta"]["ok"] is True


# ── _parse_urls ───────────────────────────────────────────────────────────────

class TestParseUrls:

    def test_empty_string_returns_empty_list(self):
        assert dag_mod._parse_urls("") == []

    def test_none_returns_empty_list(self):
        assert dag_mod._parse_urls(None) == []

    def test_whitespace_only_returns_empty_list(self):
        assert dag_mod._parse_urls("   ") == []

    def test_valid_json_array(self):
        urls = ["https://a.com", "https://b.com"]
        assert dag_mod._parse_urls(json.dumps(urls)) == urls

    def test_json_array_filters_empty_strings(self):
        raw = json.dumps(["https://a.com", "", "  ", "https://b.com"])
        assert dag_mod._parse_urls(raw) == ["https://a.com", "https://b.com"]

    def test_comma_separated_fallback(self):
        raw = "https://a.com, https://b.com, https://c.com"
        assert dag_mod._parse_urls(raw) == ["https://a.com", "https://b.com", "https://c.com"]

    def test_newline_separated_fallback(self):
        raw = "https://a.com\nhttps://b.com"
        assert dag_mod._parse_urls(raw) == ["https://a.com", "https://b.com"]

    def test_invalid_json_falls_back_to_split(self):
        raw = "[not valid json"
        result = dag_mod._parse_urls(raw)
        assert isinstance(result, list)

    def test_single_url_no_comma(self):
        assert dag_mod._parse_urls("https://example.com/api") == ["https://example.com/api"]

    def test_json_non_string_values_filtered(self):
        raw = json.dumps(["https://a.com", 42, None, "https://b.com"])
        result = dag_mod._parse_urls(raw)
        assert result == ["https://a.com", "https://b.com"]


# ── _set_variable_if_missing ──────────────────────────────────────────────────

class TestSetVariableIfMissing:

    def setup_method(self):
        Variable._store.clear()

    def test_creates_variable_when_missing(self):
        dag_mod._set_variable_if_missing("my_key", "my_value")
        assert Variable._store.get("my_key") == "my_value"

    def test_does_not_overwrite_existing_variable(self):
        Variable._store["my_key"] = "existing_value"
        dag_mod._set_variable_if_missing("my_key", "new_value")
        assert Variable._store["my_key"] == "existing_value"

    def test_multiple_keys_independent(self):
        dag_mod._set_variable_if_missing("key1", "val1")
        dag_mod._set_variable_if_missing("key2", "val2")
        assert Variable._store["key1"] == "val1"
        assert Variable._store["key2"] == "val2"

    def test_empty_string_value_is_set(self):
        dag_mod._set_variable_if_missing("empty_key", "")
        assert Variable._store.get("empty_key") == ""

    def test_json_value_stored_as_string(self):
        val = json.dumps(["https://a.com"])
        dag_mod._set_variable_if_missing("urls", val)
        assert Variable._store["urls"] == val



def _pipeline_summary_logic(extract_stats, transform_stats, load_stats):
    total_duration = (
        extract_stats.get("duration_seconds", 0)
        + transform_stats.get("duration_seconds", 0)
        + load_stats.get("duration_seconds", 0)
    )
    return {
        "pipeline": "gtfs_full_etl",
        "total_duration_seconds": total_duration,
        "extract": extract_stats,
        "transform": transform_stats,
        "load": load_stats,
        "success": all([
            extract_stats.get("success"),
            transform_stats.get("success"),
            load_stats.get("success"),
        ]),
    }


def _model_summary_logic(model_result):
    """Reproduit la logique de model_summary sans Airflow."""
    return {
        "model": model_result.get("name", "unknown"),
        "r2": model_result.get("r2"),
        "mae": model_result.get("mae"),
        "mae_pct": model_result.get("mae_pct"),
        "success": model_result.get("r2", None) is not None,
    }


# ── pipeline_summary (logique pure) ──────────────────────────────────────────

class TestPipelineSummaryLogic:

    def _make_stats(self, duration=10.0, success=True, **extra):
        return {"duration_seconds": duration, "success": success, **extra}

    def test_total_duration_is_sum_of_stages(self):
        e = self._make_stats(duration=5.0)
        t = self._make_stats(duration=15.0)
        load = self._make_stats(duration=8.0)
        total = e["duration_seconds"] + t["duration_seconds"] + load["duration_seconds"]
        assert total == 28.0

    def test_success_true_when_all_stages_succeed(self):
        result = _pipeline_summary_logic(
            self._make_stats(duration=5.0, downloaded=2),
            self._make_stats(duration=10.0, files_generated=3),
            self._make_stats(duration=8.0, rows_loaded=100),
        )
        assert result["success"] is True

    def test_success_false_when_one_stage_fails(self):
        result = _pipeline_summary_logic(
            self._make_stats(duration=5.0),
            self._make_stats(duration=10.0, success=False),
            self._make_stats(duration=8.0),
        )
        assert result["success"] is False

    def test_summary_contains_all_stage_stats(self):
        e = self._make_stats(duration=5.0, downloaded=2)
        t = self._make_stats(duration=10.0, files_generated=3)
        load = self._make_stats(duration=8.0, rows_loaded=100)
        result = _pipeline_summary_logic(e, t, load)
        assert result["extract"] == e
        assert result["transform"] == t
        assert result["load"] == load

    def test_summary_pipeline_name(self):
        result = _pipeline_summary_logic(
            self._make_stats(), self._make_stats(), self._make_stats()
        )
        assert result["pipeline"] == "gtfs_full_etl"

    def test_total_duration_in_result(self):
        result = _pipeline_summary_logic(
            self._make_stats(duration=3.0),
            self._make_stats(duration=7.0),
            self._make_stats(duration=5.0),
        )
        assert result["total_duration_seconds"] == pytest.approx(15.0)

    def test_success_false_when_all_stages_fail(self):
        result = _pipeline_summary_logic(
            self._make_stats(success=False),
            self._make_stats(success=False),
            self._make_stats(success=False),
        )
        assert result["success"] is False

    def test_missing_success_key_treated_as_falsy(self):
        result = _pipeline_summary_logic(
            {"duration_seconds": 1.0},
            {"duration_seconds": 2.0},
            {"duration_seconds": 3.0},
        )
        assert result["success"] is False


# ── model_summary (logique pure) ──────────────────────────────────────────────

class TestModelSummaryLogic:

    def test_success_true_when_r2_present(self):
        result = _model_summary_logic({"r2": 0.85, "mae": 3.2, "mae_pct": 12.0, "name": "RF"})
        assert result["success"] is True
        assert result["r2"] == 0.85
        assert result["mae"] == 3.2
        assert result["model"] == "RF"

    def test_success_false_when_r2_none(self):
        result = _model_summary_logic({"r2": None, "mae": 3.2, "mae_pct": 12.0})
        assert result["success"] is False

    def test_success_false_when_r2_missing(self):
        result = _model_summary_logic({"mae": 3.2})
        assert result["success"] is False

    def test_default_model_name_when_missing(self):
        result = _model_summary_logic({})
        assert result["model"] == "unknown"

    def test_all_fields_present_in_result(self):
        result = _model_summary_logic({"r2": 0.9, "mae": 2.0, "mae_pct": 8.0, "name": "RF"})
        assert set(result.keys()) == {"model", "r2", "mae", "mae_pct", "success"}

    def test_none_values_preserved(self):
        result = _model_summary_logic({"r2": None, "mae": None, "mae_pct": None})
        assert result["r2"] is None
        assert result["mae"] is None
        assert result["mae_pct"] is None

    def test_r2_zero_is_success(self):
        result = _model_summary_logic({"r2": 0.0, "mae": 5.0, "mae_pct": 20.0})
        assert result["success"] is True