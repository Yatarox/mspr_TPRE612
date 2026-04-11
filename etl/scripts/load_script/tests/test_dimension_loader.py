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
    hook.get_first.side_effect = [None, (123,)]  # 1er SELECT absent, 2e présent

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
    # exists country? -> None ; select location before insert -> None ; after insert -> (77,)
    hook.get_first.side_effect = [None, None, (77,)]

    out = mod.load_dim_location(hook, "Paris Gare", "XX")

    assert out == 77
    # Vérifie que le INSERT a bien reçu country_code = None
    insert_params = hook.run.call_args.kwargs["parameters"]
    assert insert_params == ("Paris Gare", None)


def test_load_dim_time_valid_and_invalid(monkeypatch):
    monkeypatch.setattr(mod, "dim_cache", _FakeCache())
    hook = MagicMock()

    # Cas valide: 26:15:59 -> hour % 24 = 2
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