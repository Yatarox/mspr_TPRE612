import os
import sys
import pytest
import numpy as np
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from services import model_service
from middleware.prometheus import PREDICTION_COUNT, PREDICTION_LATENCY, PREDICTION_VALUE


# ── Classes sérialisables au niveau module ────────────────────────────────────

class _FakeModel:
    def predict(self, X):
        return np.array([7.0])

class _NegativeModel:
    def predict(self, X):
        return np.array([-5.0])

def _make_artifact(model=None):
    return {"model": model or _FakeModel(), "name": "RandomForest"}


# ── Fixture : reset du cache RAM entre chaque test ────────────────────────────

@pytest.fixture(autouse=True)
def reset_cache(monkeypatch):
    monkeypatch.setattr(model_service, "_model", None)
    monkeypatch.setattr(model_service, "_model_name", None)
    monkeypatch.setattr(model_service, "_last_check", 0)
    monkeypatch.setattr(model_service, "_model_available", False)


# ── is_model_available ────────────────────────────────────────────────────────

def test_is_model_available_true():
    with patch("os.path.exists", return_value=True):
        assert model_service.is_model_available() is True

def test_is_model_available_false():
    with patch("os.path.exists", return_value=False):
        assert model_service.is_model_available() is False

def test_is_model_available_uses_cache(monkeypatch):
    import time
    monkeypatch.setattr(model_service, "_last_check", time.time())
    monkeypatch.setattr(model_service, "_model_available", True)
    with patch("os.path.exists", side_effect=AssertionError("ne devrait pas être appelé")):
        assert model_service.is_model_available() is True


# ── load_model ────────────────────────────────────────────────────────────────

def test_load_model_sets_cache():
    with patch("os.path.exists", return_value=True), \
         patch("joblib.load", return_value=_make_artifact()):
        model_service.load_model()
    assert model_service._model is not None
    assert model_service._model_name == "RandomForest"

def test_load_model_skips_if_file_missing():
    with patch("os.path.exists", return_value=False):
        model_service.load_model()
    assert model_service._model is None

def test_get_model_returns_cached(monkeypatch):
    fake = _FakeModel()
    monkeypatch.setattr(model_service, "_model", fake)
    monkeypatch.setattr(model_service, "_model_name", "Cached")
    model, name = model_service.get_model()
    assert model is fake
    assert name == "Cached"

def test_get_model_loads_if_not_cached():
    with patch("os.path.exists", return_value=True), \
         patch("joblib.load", return_value=_make_artifact()):
        model, name = model_service.get_model()
    assert model is not None
    assert name == "RandomForest"


# ── predict_co2 — modèle indisponible ─────────────────────────────────────────

def test_predict_co2_model_unavailable_returns_warning(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: False)
    result = model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    assert result["emission_gco2e_pkm"] is None
    assert result["warning"] == "Modèle non disponible"

def test_predict_co2_model_unavailable_increments_error_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: False)
    before = PREDICTION_COUNT.labels(status="error")._value.get()
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    after = PREDICTION_COUNT.labels(status="error")._value.get()
    assert after == before + 1


# ── predict_co2 — modèle en cache RAM ────────────────────────────────────────

def test_predict_co2_uses_cached_model(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    with patch("joblib.load", side_effect=AssertionError("ne devrait pas charger")):
        result = model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    assert result["warning"] is None
    assert result["model"] == "RandomForest"

def test_predict_co2_returns_expected_keys(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    result = model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    assert "emission_gco2e_pkm" in result
    assert "total_emission_kgco2e" in result
    assert "model" in result
    assert "warning" in result

def test_predict_co2_total_emission_formula(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    distance_km = 300
    result = model_service.predict_co2(distance_km, 2.0, 0, "Régional", "Diesel")
    expected = model_service.ADEME_GCO2E_PKM * distance_km / 1000
    assert result["total_emission_kgco2e"] == pytest.approx(expected)

def test_predict_co2_no_warning_on_success(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    result = model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    assert result["warning"] is None

def test_predict_co2_frequency_clipped(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _NegativeModel())
    monkeypatch.setattr(model_service, "_model_name", "Test")
    result = model_service.predict_co2(100, 1.0, 0, "Régional", "Diesel")
    assert result["emission_gco2e_pkm"] is not None
    assert result["warning"] is None


# ── predict_co2 — métriques Prometheus ───────────────────────────────────────

def test_predict_co2_success_increments_success_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    before = PREDICTION_COUNT.labels(status="success")._value.get()
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    after = PREDICTION_COUNT.labels(status="success")._value.get()
    assert after == before + 1

def test_predict_co2_success_observes_latency(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    before = PREDICTION_LATENCY._sum.get()
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    after = PREDICTION_LATENCY._sum.get()
    assert after > before

def test_predict_co2_success_observes_prediction_value(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest")
    before = PREDICTION_VALUE._sum.get()
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    after = PREDICTION_VALUE._sum.get()
    assert after == pytest.approx(before + model_service.ADEME_GCO2E_PKM)


# ── predict_co2 — gestion d'erreur ────────────────────────────────────────────

def test_predict_co2_exception_returns_warning(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(
        model_service, "get_model",
        lambda: (_ for _ in ()).throw(Exception("fichier corrompu"))
    )
    result = model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    assert result["emission_gco2e_pkm"] is None
    assert "fichier corrompu" in result["warning"]

def test_predict_co2_exception_increments_error_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(
        model_service, "get_model",
        lambda: (_ for _ in ()).throw(Exception("crash"))
    )
    before = PREDICTION_COUNT.labels(status="error")._value.get()
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "Électrique")
    after = PREDICTION_COUNT.labels(status="error")._value.get()
    assert after == before + 1