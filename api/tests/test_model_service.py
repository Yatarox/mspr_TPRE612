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
        return np.array([15.0])  # Fréquence plus réaliste


class _NegativeModel:
    def predict(self, X):
        return np.array([-5.0])


def _make_artifact(model=None):
    return {"model": model or _FakeModel(), "name": "RandomForest_Optimized"}


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
    assert model_service._model_name == "RandomForest_Optimized"


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
    assert name == "RandomForest_Optimized"


# ── prepare_features ──────────────────────────────────────────────────────────

def test_prepare_features_jour():
    features = model_service.prepare_features(
        distance_km=450,
        duration_h=2.5,
        service_type="JOUR",
        traction="électrique"
    )
    assert features["distance_km"] == 450
    assert features["duration_h"] == 2.5
    assert features["speed_kmh"] == 180.0  # 450/2.5
    assert features["is_night"] == 0
    assert features["distance_night"] == 0
    assert features["service_type"] == "JOUR"
    assert features["traction"] == "électrique"


def test_prepare_features_nuit():
    features = model_service.prepare_features(
        distance_km=450,
        duration_h=6.0,
        service_type="NUIT",
        traction="électrique"
    )
    assert features["speed_kmh"] == 75.0  # 450/6
    assert features["is_night"] == 1
    assert features["distance_night"] == 450
    assert features["service_type"] == "NUIT"


def test_prepare_features_traction_lowercase():
    features = model_service.prepare_features(
        distance_km=100,
        duration_h=1.0,
        service_type="JOUR",
        traction="ÉLECTRIQUE"
    )
    assert features["traction"] == "électrique"


# ── predict_frequency — modèle indisponible ──────────────────────────────────

def test_predict_frequency_model_unavailable_returns_warning(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: False)
    result = model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    assert result["frequency_per_week"] is None
    assert result["warning"] == "Modèle non disponible"


def test_predict_frequency_model_unavailable_increments_error_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: False)
    before = PREDICTION_COUNT.labels(status="error")._value.get()
    model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    after = PREDICTION_COUNT.labels(status="error")._value.get()
    assert after == before + 1


# ── predict_frequency — modèle en cache RAM ──────────────────────────────────

def test_predict_frequency_uses_cached_model(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    with patch("joblib.load", side_effect=AssertionError("ne devrait pas charger")):
        result = model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    assert result["warning"] is None
    assert result["model"] == "RandomForest_Optimized"


def test_predict_frequency_returns_expected_keys(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    result = model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    assert "frequency_per_week" in result
    assert "emission_gco2e_pkm" in result
    assert "total_emission_kgco2e" in result
    assert "model" in result
    assert "warning" in result


def test_predict_frequency_total_emission_formula(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    distance_km = 300
    result = model_service.predict_frequency(distance_km, 2.0, "JOUR", "diesel")
    expected = model_service.ADEME_GCO2E_PKM * distance_km / 1000
    assert result["total_emission_kgco2e"] == pytest.approx(expected)


def test_predict_frequency_no_warning_on_success(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    result = model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    assert result["warning"] is None


def test_predict_frequency_frequency_clipped(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _NegativeModel())
    monkeypatch.setattr(model_service, "_model_name", "Test")
    result = model_service.predict_frequency(100, 1.0, "JOUR", "diesel")
    assert result["frequency_per_week"] >= 1.0
    assert result["warning"] is None


def test_predict_frequency_service_type_nuit(monkeypatch):
    """Vérifie que le service_type NUIT est bien interprété"""
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    result = model_service.predict_frequency(450, 6.0, "NUIT", "électrique")
    assert result["frequency_per_week"] == 15.0  # _FakeModel retourne 15
    assert result["warning"] is None


# ── predict_frequency — métriques Prometheus ─────────────────────────────────

def test_predict_frequency_success_increments_success_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    before = PREDICTION_COUNT.labels(status="success")._value.get()
    model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    after = PREDICTION_COUNT.labels(status="success")._value.get()
    assert after == before + 1


def test_predict_frequency_success_observes_latency(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    before = PREDICTION_LATENCY._sum.get()
    model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    after = PREDICTION_LATENCY._sum.get()
    assert after > before


def test_predict_frequency_success_observes_prediction_value(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    before = PREDICTION_VALUE._sum.get()
    model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    after = PREDICTION_VALUE._sum.get()
    assert after == pytest.approx(before + 15.0)


# ── predict_frequency — gestion d'erreur ──────────────────────────────────────

def test_predict_frequency_exception_returns_warning(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(
        model_service, "get_model",
        lambda: (_ for _ in ()).throw(Exception("fichier corrompu"))
    )
    result = model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    assert result["frequency_per_week"] is None
    assert "fichier corrompu" in result["warning"]


def test_predict_frequency_exception_increments_error_counter(monkeypatch):
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(
        model_service, "get_model",
        lambda: (_ for _ in ()).throw(Exception("crash"))
    )
    before = PREDICTION_COUNT.labels(status="error")._value.get()
    model_service.predict_frequency(450, 2.5, "JOUR", "électrique")
    after = PREDICTION_COUNT.labels(status="error")._value.get()
    assert after == before + 1


# ── predict_co2 — compatibilité (appel vers predict_frequency) ──────────────

def test_predict_co2_compatibility(monkeypatch):
    """Vérifie que l'ancienne fonction predict_co2 fonctionne encore"""
    monkeypatch.setattr(model_service, "is_model_available", lambda: True)
    monkeypatch.setattr(model_service, "_model", _FakeModel())
    monkeypatch.setattr(model_service, "_model_name", "RandomForest_Optimized")
    
    result = model_service.predict_co2(
        distance_km=450,
        duration_h=2.5,
        nb_stops=3,
        train_type="Grande vitesse",
        traction="électrique"
    )
    assert "frequency_per_week" in result
    assert result["model"] == "RandomForest_Optimized"
    assert result["warning"] is None


def test_predict_co2_uses_predict_frequency(monkeypatch):
    """Vérifie que predict_co2 appelle bien predict_frequency"""
    called = False
    
    def mock_predict_frequency(dist, dur, service_type, traction):
        nonlocal called
        called = True
        return {"frequency_per_week": 15.0, "model": "Mock"}
    
    monkeypatch.setattr(model_service, "predict_frequency", mock_predict_frequency)
    
    model_service.predict_co2(450, 2.5, 0, "Grande vitesse", "électrique")
    assert called is True