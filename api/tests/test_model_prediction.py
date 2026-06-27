import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from api.routes.model_prediction import router
from fastapi.testclient import TestClient
from fastapi import FastAPI
from unittest.mock import patch

app = FastAPI()
app.include_router(router, prefix="/api")
client = TestClient(app)

# Paramètres valides pour l'API (avec service_type)
VALID_PARAMS = {
    "distance_km": 450.0,
    "duration_h": 2.5,
    "traction": "électrique",
    "service_type": "JOUR",
    "train_type": "Grande vitesse",  # Ignoré mais accepté
    "nb_stops": 0,                   # Ignoré mais accepté
}


def test_predict_returns_200():
    mock_result = {
        "frequency_per_week": 22.5,
        "emission_gco2e_pkm": 25.0,
        "total_emission_kgco2e": 11.25,
        "model": "RandomForest_Optimized",
        "warning": None
    }
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    assert response.status_code == 200


def test_predict_returns_expected_fields():
    mock_result = {
        "frequency_per_week": 22.5,
        "emission_gco2e_pkm": 25.0,
        "total_emission_kgco2e": 11.25,
        "model": "RandomForest_Optimized",
        "warning": None
    }
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    data = response.json()
    assert "frequency_per_week" in data
    assert "emission_gco2e_pkm" in data
    assert "total_emission_kgco2e" in data
    assert "model" in data
    assert "warning" in data


def test_predict_missing_required_param():
    # distance_km manquant
    params = {k: v for k, v in VALID_PARAMS.items() if k != "distance_km"}
    response = client.get("/api/predict", params=params)
    assert response.status_code == 422


def test_predict_missing_traction():
    params = {k: v for k, v in VALID_PARAMS.items() if k != "traction"}
    response = client.get("/api/predict", params=params)
    assert response.status_code == 422


def test_predict_with_nb_stops():
    mock_result = {
        "frequency_per_week": 22.5,
        "emission_gco2e_pkm": 25.0,
        "total_emission_kgco2e": 11.25,
        "model": "RandomForest_Optimized",
        "warning": None
    }
    params = {**VALID_PARAMS, "nb_stops": 5}
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=params)
    assert response.status_code == 200


def test_predict_with_service_type_nuit():
    mock_result = {
        "frequency_per_week": 8.5,
        "emission_gco2e_pkm": 25.0,
        "total_emission_kgco2e": 11.25,
        "model": "RandomForest_Optimized",
        "warning": None
    }
    params = {**VALID_PARAMS, "service_type": "NUIT"}
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=params)
    assert response.status_code == 200
    assert response.json()["frequency_per_week"] == 8.5


def test_predict_model_unavailable():
    mock_result = {
        "frequency_per_week": None,
        "emission_gco2e_pkm": None,
        "total_emission_kgco2e": None,
        "model": None,
        "warning": "Modèle non disponible"
    }
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    assert response.status_code == 200
    assert response.json()["warning"] == "Modèle non disponible"


def test_predict_passes_correct_args():
    captured = {}

    def fake_predict(distance_km, duration_h, service_type, traction):
        captured.update({
            "distance_km": distance_km,
            "duration_h": duration_h,
            "service_type": service_type,
            "traction": traction
        })
        return {
            "frequency_per_week": 22.5,
            "emission_gco2e_pkm": 25.0,
            "total_emission_kgco2e": 11.25,
            "model": "RandomForest_Optimized",
            "warning": None
        }

    with patch("services.model_service.predict_frequency", side_effect=fake_predict):
        client.get("/api/predict", params=VALID_PARAMS)

    assert captured["distance_km"] == 450.0
    assert captured["duration_h"] == 2.5
    assert captured["service_type"] == "JOUR"
    assert captured["traction"] == "électrique"


def test_predict_with_different_traction_values():
    """Teste différentes valeurs de traction"""
    for traction in ["électrique", "diesel", "mixte"]:
        mock_result = {
            "frequency_per_week": 20.0,
            "emission_gco2e_pkm": 25.0,
            "total_emission_kgco2e": 10.0,
            "model": "RandomForest_Optimized",
            "warning": None
        }
        params = {**VALID_PARAMS, "traction": traction}
        with patch("services.model_service.predict_frequency", return_value=mock_result):
            response = client.get("/api/predict", params=params)
        assert response.status_code == 200


def test_predict_ignores_train_type_param():
    """Vérifie que train_type est ignoré (le modèle ne l'utilise pas)"""
    captured = {}

    def fake_predict(distance_km, duration_h, service_type, traction):
        captured.update({"train_type_passed": False})
        return {
            "frequency_per_week": 22.5,
            "emission_gco2e_pkm": 25.0,
            "total_emission_kgco2e": 11.25,
            "model": "RandomForest_Optimized",
            "warning": None
        }

    with patch("services.model_service.predict_frequency", side_effect=fake_predict):
        response = client.get("/api/predict", params=VALID_PARAMS)
    assert response.status_code == 200


def test_predict_with_zero_distance():
    mock_result = {
        "frequency_per_week": 5.0,
        "emission_gco2e_pkm": 25.0,
        "total_emission_kgco2e": 0.0,
        "model": "RandomForest_Optimized",
        "warning": None
    }
    params = {**VALID_PARAMS, "distance_km": 0}
    with patch("services.model_service.predict_frequency", return_value=mock_result):
        response = client.get("/api/predict", params=params)
    assert response.status_code == 200


def test_predict_with_negative_duration():
    """Teste avec duration_h négative (devrait être rejetée par la validation)"""
    params = {**VALID_PARAMS, "duration_h": -1.0}
    response = client.get("/api/predict", params=params)
    assert response.status_code == 422