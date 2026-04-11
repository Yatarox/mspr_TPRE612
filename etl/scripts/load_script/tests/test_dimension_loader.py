import os
import sys
from unittest.mock import MagicMock

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from load_script import dimension_loaders as mod


class _FakeCache:
    def __init__(self):
        self._d = {}

    def get(self, key):
        return self._d.get(key)

    def set(self, key, value):
        self._d[key] = value


def test_load_dim_dataset_from_cache(monkeypatch):
    cache = _FakeCache()
    cache.set("dataset_12", 99)
    monkeypatch.setattr(mod, "dim_cache", cache)

    hook = MagicMock()
    out = mod.load_dim_dataset(hook, 12)

    assert out == 99
    hook.get_first.assert_not_called()
    hook.run.assert_not_called()


def test_load_dim_trip_insert_then_select(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (123,)]

    out = mod.load_dim_trip(hook, "TRIP_1")

    assert out == 123
    assert hook.run.call_count == 1


def test_load_dim_train_type_none():
    hook = MagicMock()
    assert mod.load_dim_train_type(hook, None) is None
    assert mod.load_dim_train_type(hook, "") is None
    hook.get_first.assert_not_called()


def test_load_dim_country_insert_error_returns_none(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.return_value = None
    hook.run.side_effect = Exception("db error")

    out = mod.load_dim_country(hook, "FR")
    assert out is None


def test_load_dim_location_country_missing_sets_null(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()

    hook.get_first.side_effect = [None, None, (77,)]

    out = mod.load_dim_location(hook, "Paris Gare", "XX")

    assert out == 77
    insert_params = hook.run.call_args.kwargs["parameters"]
    assert insert_params == ("Paris Gare", None)


def test_load_dim_time_valid_and_invalid(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())
    hook = MagicMock()

    hook.get_first.side_effect = [None, (10,)]
    out = mod.load_dim_time(hook, "26:15:59")
    assert out == 10
    params = hook.run.call_args.kwargs["parameters"]
    assert params == ("26:15:59", 2, 15, 59)

    # Cas invalide: fallback 0,0,0
    hook.reset_mock()
    hook.get_first.side_effect = [None, (11,)]
    out2 = mod.load_dim_time(hook, "not-a-time")
    assert out2 == 11
    params2 = hook.run.call_args.kwargs["parameters"]
    assert params2 == ("not-a-time", 0, 0, 0)


def test_load_dim_dataset_insert_then_select(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (42,)]

    out = mod.load_dim_dataset(hook, 5)

    assert out == 42
    assert hook.run.call_count == 1


def test_load_dim_dataset_insert_but_select_returns_none_raises(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, None]

    try:
        mod.load_dim_dataset(hook, 7)
        assert False, "Should raise AirflowException"
    except Exception as e:
        assert "Cannot load dataset dimension" in str(e)


def test_load_dim_route_with_name(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (88,)]

    out = mod.load_dim_route(hook, "R1", "Paris-Lyon")

    assert out == 88
    insert_params = hook.run.call_args.kwargs["parameters"]
    assert insert_params == ("R1", "Paris-Lyon")


def test_load_dim_agency_with_name(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (99,)]

    out = mod.load_dim_agency(hook, "A1", "SNCF")

    assert out == 99
    insert_params = hook.run.call_args.kwargs["parameters"]
    assert insert_params == ("A1", "SNCF")


def test_load_dim_service_type_valid(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (55,)]

    out = mod.load_dim_service_type(hook, "Régional")

    assert out == 55
    assert hook.run.call_count == 1


def test_load_dim_traction_none(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    assert mod.load_dim_traction(hook, None) is None
    assert mod.load_dim_traction(hook, "") is None
    hook.get_first.assert_not_called()


def test_load_dim_traction_valid(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (66,)]

    out = mod.load_dim_traction(hook, "électrique")

    assert out == 66
    assert hook.run.call_count == 1


def test_load_dim_country_none(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    assert mod.load_dim_country(hook, None) is None
    assert mod.load_dim_country(hook, "") is None
    hook.get_first.assert_not_called()


def test_load_dim_country_already_exists(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.return_value = (33,)

    out = mod.load_dim_country(hook, "FR")

    assert out == 33
    hook.run.assert_not_called()


def test_load_dim_country_insert_and_select(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [None, (44,)]

    out = mod.load_dim_country(hook, "DE")

    assert out == 44
    assert hook.run.call_count == 1


def test_load_dim_location_none_stop_name(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    assert mod.load_dim_location(hook, None) is None
    assert mod.load_dim_location(hook, "") is None
    hook.get_first.assert_not_called()


def test_load_dim_location_already_exists(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.return_value = (77,)

    out = mod.load_dim_location(hook, "Paris", "FR")

    assert out == 77
    hook.run.assert_not_called()


def test_load_dim_location_insert_and_select(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.side_effect = [
        (1,),     # country exists
        None,     # location not found (before insert)
        (88,)     # location found (after insert)
    ]

    out = mod.load_dim_location(hook, "Lyon", "FR")

    assert out == 88
    assert hook.run.call_count == 1
    insert_params = hook.run.call_args.kwargs["parameters"]
    assert insert_params == ("Lyon", "FR")

def test_load_dim_time_none(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    assert mod.load_dim_time(hook, None) is None
    hook.get_first.assert_not_called()


def test_load_dim_time_already_exists(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())

    hook = MagicMock()
    hook.get_first.return_value = (99,)

    out = mod.load_dim_time(hook, "12:30:45")

    assert out == 99
    hook.run.assert_not_called()