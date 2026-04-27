
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

VALID_PARAMS = {
    "distance_km": 450.0,
    "duration_h": 2.5,
    "train_type": "Grande vitesse",
    "traction": "Électrique",
}

def test_predict_returns_200():
    mock_result = {
        "emission_gco2e_pkm": 1.7,
        "total_emission_kgco2e": 0.765,
        "model": "RandomForest",
        "warning": None
    }
    with patch("services.model_service.predict_co2", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    assert response.status_code == 200

def test_predict_returns_expected_fields():
    mock_result = {
        "emission_gco2e_pkm": 1.7,
        "total_emission_kgco2e": 0.765,
        "model": "RandomForest",
        "warning": None
    }
    with patch("services.model_service.predict_co2", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    data = response.json()
    assert "emission_gco2e_pkm" in data
    assert "total_emission_kgco2e" in data
    assert "model" in data
    assert "warning" in data

def test_predict_missing_required_param():
    # distance_km manquant
    params = {k: v for k, v in VALID_PARAMS.items() if k != "distance_km"}
    response = client.get("/api/predict", params=params)
    assert response.status_code == 422

def test_predict_missing_train_type():
    params = {k: v for k, v in VALID_PARAMS.items() if k != "train_type"}
    response = client.get("/api/predict", params=params)
    assert response.status_code == 422

def test_predict_with_nb_stops():
    mock_result = {
        "emission_gco2e_pkm": 1.7,
        "total_emission_kgco2e": 0.765,
        "model": "RandomForest",
        "warning": None
    }
    params = {**VALID_PARAMS, "nb_stops": 5}
    with patch("services.model_service.predict_co2", return_value=mock_result):
        response = client.get("/api/predict", params=params)
    assert response.status_code == 200

def test_predict_model_unavailable():
    mock_result = {
        "emission_gco2e_pkm": None,
        "total_emission_kgco2e": None,
        "model": None,
        "warning": "Modèle non disponible"
    }
    with patch("services.model_service.predict_co2", return_value=mock_result):
        response = client.get("/api/predict", params=VALID_PARAMS)
    assert response.status_code == 200
    assert response.json()["warning"] == "Modèle non disponible"

def test_predict_passes_correct_args():
    captured = {}
    def fake_predict(**kwargs):
        captured.update(kwargs)
        return {"emission_gco2e_pkm": 1.7, "total_emission_kgco2e": 0.765, "model": "RF", "warning": None}

    with patch("services.model_service.predict_co2", side_effect=fake_predict):
        client.get("/api/predict", params=VALID_PARAMS)

    assert captured["distance_km"] == 450.0
    assert captured["duration_h"] == 2.5
    assert captured["train_type"] == "Grande vitesse"
    assert captured["traction"] == "Électrique"
    assert captured["nb_stops"] == 0  # valeur par défaut
