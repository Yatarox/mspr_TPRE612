"""
Tests unitaires pour la route POST /api/predict/co2.
Lance avec : pytest tests/test_co2_prediction_route.py -v
"""

from fastapi.testclient import TestClient
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# On importe l'app APRÈS avoir patché les dépendances lourdes
with patch("services.co2_prediction_service.load_model"):
    from main import app

client = TestClient(app)

# Payload valide de référence
VALID_PAYLOAD = {
    "distance_km": 450.0,
    "duration_h": 2.5,
    "nb_stops": 3,
    "train_type": "TGV",
    "traction": "Électrique",
}

MOCK_RESULT = {
    "emission_gco2e_pkm": 5.1,
    "total_emission_kgco2e": 2.295,
    "model": "fallback-ademe-iea",
    "warning": "test fallback",
}


# ── Tests statut HTTP ──────────────────────────────────────────────────────────

class TestCO2RouteStatus:
    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_200_avec_payload_valide(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        assert response.status_code == 200

    def test_422_sans_distance(self):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "distance_km"}
        response = client.post("/api/predict/co2", json=payload)
        assert response.status_code == 422

    def test_422_sans_duration(self):
        payload = {k: v for k, v in VALID_PAYLOAD.items() if k != "duration_h"}
        response = client.post("/api/predict/co2", json=payload)
        assert response.status_code == 422

    def test_422_distance_negative(self):
        response = client.post("/api/predict/co2", json={**VALID_PAYLOAD, "distance_km": -10})
        assert response.status_code == 422

    def test_422_distance_trop_grande(self):
        response = client.post("/api/predict/co2", json={**VALID_PAYLOAD, "distance_km": 99_999})
        assert response.status_code == 422

    def test_422_train_type_invalide(self):
        response = client.post("/api/predict/co2", json={**VALID_PAYLOAD, "train_type": "Rocket"})
        assert response.status_code == 422

    def test_422_traction_invalide(self):
        response = client.post("/api/predict/co2", json={**VALID_PAYLOAD, "traction": "Nuclear"})
        assert response.status_code == 422

    @patch("routes.co2_prediction.predict_co2", side_effect=Exception("crash"))
    def test_500_si_service_plante(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        assert response.status_code == 500


# ── Tests body de la réponse ───────────────────────────────────────────────────

class TestCO2RouteResponse:
    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_champs_obligatoires_presents(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        data = response.json()
        for field in [
            "distance_km", "duration_h", "nb_stops", "train_type", "traction",
            "emission_gco2e_pkm", "total_emission_kgco2e", "model",
        ]:
            assert field in data, f"Champ manquant : {field}"

    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_inputs_recopies_dans_reponse(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        data = response.json()
        assert data["distance_km"] == VALID_PAYLOAD["distance_km"]
        assert data["train_type"] == VALID_PAYLOAD["train_type"]
        assert data["traction"] == VALID_PAYLOAD["traction"]

    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_valeurs_prediction_correctes(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        data = response.json()
        assert data["emission_gco2e_pkm"] == MOCK_RESULT["emission_gco2e_pkm"]
        assert data["total_emission_kgco2e"] == MOCK_RESULT["total_emission_kgco2e"]
        assert data["model"] == MOCK_RESULT["model"]

    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_warning_present_si_fallback(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        data = response.json()
        assert data.get("warning") == MOCK_RESULT["warning"]

    @patch("routes.co2_prediction.predict_co2", return_value={**MOCK_RESULT, "warning": None})
    def test_warning_absent_si_ml_ok(self, mock_predict):
        response = client.post("/api/predict/co2", json=VALID_PAYLOAD)
        data = response.json()
        assert data.get("warning") is None

    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_valeurs_par_defaut_nb_stops_train_type_traction(self, mock_predict):
        """Vérifie que les valeurs par défaut sont acceptées sans erreur."""
        payload = {"distance_km": 100.0, "duration_h": 1.0}
        response = client.post("/api/predict/co2", json=payload)
        assert response.status_code == 200


# ── Tests métriques Prometheus (compteurs) ─────────────────────────────────────

class TestCO2RoutePrometheusMetrics:
    @patch("routes.co2_prediction.predict_co2", return_value=MOCK_RESULT)
    def test_prediction_count_incremente(self, mock_predict):
        from middleware.prometheus import PREDICTION_COUNT
        # Récupère la valeur avant
        before = PREDICTION_COUNT.labels(status="success")._value.get()
        client.post("/api/predict/co2", json=VALID_PAYLOAD)
        after = PREDICTION_COUNT.labels(status="success")._value.get()
        assert after == before + 1

    @patch("routes.co2_prediction.predict_co2", side_effect=Exception("boom"))
    def test_prediction_error_count_incremente(self, mock_predict):
        from middleware.prometheus import PREDICTION_COUNT
        before = PREDICTION_COUNT.labels(status="error")._value.get()
        client.post("/api/predict/co2", json=VALID_PAYLOAD)
        after = PREDICTION_COUNT.labels(status="error")._value.get()
        assert after == before + 1
