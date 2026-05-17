import os
import sys
import pytest
import numpy as np
from unittest.mock import patch, AsyncMock, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from fastapi.testclient import TestClient
from main import app
from services import model_service, dashboard_service
from middleware.prometheus import (
    PREDICTION_COUNT, PREDICTION_LATENCY, PREDICTION_VALUE,
    REQUEST_COUNT, REQUEST_LATENCY,
)


class _FakeModel:
    def predict(self, X):
        return np.array([7.0])


@pytest.fixture(autouse=True)
def reset_model_cache(monkeypatch):
    monkeypatch.setattr(model_service, "_model", None)
    monkeypatch.setattr(model_service, "_model_name", None)
    monkeypatch.setattr(model_service, "_last_check", 0)
    monkeypatch.setattr(model_service, "_model_available", False)


class TestDashboardRoutesServiceIntegration:

    def test_overview_route_calls_service_and_returns_data(self):
        with patch("services.dashboard_service.execute_query",
                   return_value=[{"total_trips": 42, "total_routes": 10}]):
            client = TestClient(app)
            response = client.get("/api/stats/overview")
        assert response.status_code == 200
        assert response.json()["total_trips"] == 42

    def test_overview_empty_db_returns_empty_dict(self):
        with patch("services.dashboard_service.execute_query", return_value=[]):
            client = TestClient(app)
            response = client.get("/api/stats/overview")
        assert response.status_code == 200
        assert response.json() == {}

    def test_by_country_route_returns_list(self):
        with patch("services.dashboard_service.execute_query",
                   return_value=[{"country": "FR", "trip_count": 5},
                                  {"country": "DE", "trip_count": 3}]):
            client = TestClient(app)
            response = client.get("/api/stats/by-country")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["country"] == "FR"

    def test_by_agency_passes_limit_to_service(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = [{"agency_name": "SNCF", "trip_count": 100}]
            client = TestClient(app)
            client.get("/api/stats/by-agency?limit=5")
            query, params = mock_q.call_args[0]
            assert "LIMIT %s" in query
            assert params == (5,)

    def test_by_agency_limit_validation_rejects_zero(self):
        client = TestClient(app)
        response = client.get("/api/stats/by-agency?limit=0")
        assert response.status_code == 422

    def test_by_agency_limit_validation_rejects_over_100(self):
        client = TestClient(app)
        response = client.get("/api/stats/by-agency?limit=101")
        assert response.status_code == 422

    def test_emissions_by_route_passes_limit_to_service(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = [{"route_name": "Paris-Lyon", "total_emissions": 50.0}]
            client = TestClient(app)
            client.get("/api/emissions/by-route?limit=15")
            query, params = mock_q.call_args[0]
            assert "LIMIT %s" in query
            assert params == (15,)

    def test_search_trips_all_filters_reach_service(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = [{"trip_id": "T1"}]
            client = TestClient(app)
            client.get(
                "/api/trips/search"
                "?origin=Paris&destination=Lyon&train_type=TGV"
                "&min_distance=100&max_distance=500&limit=10"
            )
            _, params = mock_q.call_args[0]
            assert "%Paris%" in params
            assert "%Lyon%" in params
            assert "TGV" in params
            assert 100.0 in params
            assert 500.0 in params
            assert 10 in params

    def test_search_trips_no_filters_uses_default_limit(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = []
            client = TestClient(app)
            client.get("/api/trips/search")
            _, params = mock_q.call_args[0]
            assert params[-1] == 50

    def test_health_route_uses_real_service_logic(self):
        with patch("services.dashboard_service.execute_query",
                   return_value=[{"count": 99}]):
            client = TestClient(app)
            response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["total_trips"] == 99
        assert "timestamp" in data

    def test_health_route_db_error_returns_unhealthy(self):
        with patch("services.dashboard_service.execute_query",
                   side_effect=Exception("connexion refusée")):
            client = TestClient(app)
            response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "unhealthy"
        assert "connexion refusée" in data["error"]

    def test_by_service_type_route_returns_correct_structure(self):
        with patch("services.dashboard_service.execute_query",
                   return_value=[{"service_type": "Jour", "trip_count": 200},
                                  {"service_type": "Nuit", "trip_count": 50}]):
            client = TestClient(app)
            response = client.get("/api/stats/by-service-type")
        assert response.status_code == 200
        data = response.json()
        assert data[0]["service_type"] == "Jour"
        assert data[1]["service_type"] == "Nuit"


class TestModelPredictionServicePrometheusIntegration:

    VALID_PARAMS = {
        "distance_km": 450.0,
        "duration_h": 2.5,
        "train_type": "Grande vitesse",
        "traction": "Électrique",
    }

    def test_predict_route_calls_real_predict_co2_model_unavailable(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: False)
        client = TestClient(app)
        response = client.get("/api/predict", params=self.VALID_PARAMS)
        assert response.status_code == 200
        assert response.json()["warning"] == "Modèle non disponible"

    def test_predict_route_with_real_model_in_cache(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: True)
        monkeypatch.setattr(model_service, "_model", _FakeModel())
        monkeypatch.setattr(model_service, "_model_name", "RandomForest")

        client = TestClient(app)
        response = client.get("/api/predict", params=self.VALID_PARAMS)

        assert response.status_code == 200
        data = response.json()
        assert data["warning"] is None
        assert data["model"] == "RandomForest"
        assert data["emission_gco2e_pkm"] == model_service.ADEME_GCO2E_PKM
        expected_total = model_service.ADEME_GCO2E_PKM * 450.0 / 1000
        assert data["total_emission_kgco2e"] == pytest.approx(expected_total)

    def test_predict_increments_prometheus_success_counter(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: True)
        monkeypatch.setattr(model_service, "_model", _FakeModel())
        monkeypatch.setattr(model_service, "_model_name", "RandomForest")

        before = PREDICTION_COUNT.labels(status="success")._value.get()
        client = TestClient(app)
        client.get("/api/predict", params=self.VALID_PARAMS)
        after = PREDICTION_COUNT.labels(status="success")._value.get()

        assert after == before + 1

    def test_predict_unavailable_increments_error_counter(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: False)

        before = PREDICTION_COUNT.labels(status="error")._value.get()
        client = TestClient(app)
        client.get("/api/predict", params=self.VALID_PARAMS)
        after = PREDICTION_COUNT.labels(status="error")._value.get()

        assert after == before + 1

    def test_predict_success_observes_latency(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: True)
        monkeypatch.setattr(model_service, "_model", _FakeModel())
        monkeypatch.setattr(model_service, "_model_name", "RandomForest")

        before = PREDICTION_LATENCY._sum.get()
        client = TestClient(app)
        client.get("/api/predict", params=self.VALID_PARAMS)
        after = PREDICTION_LATENCY._sum.get()

        assert after > before

    def test_predict_success_observes_prediction_value(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: True)
        monkeypatch.setattr(model_service, "_model", _FakeModel())
        monkeypatch.setattr(model_service, "_model_name", "RandomForest")

        before = PREDICTION_VALUE._sum.get()
        client = TestClient(app)
        client.get("/api/predict", params=self.VALID_PARAMS)
        after = PREDICTION_VALUE._sum.get()

        assert after == pytest.approx(before +  7.0)

    def test_predict_frequency_clipped_to_minimum_one(self, monkeypatch):
        class NegModel:
            def predict(self, X):
                return np.array([-10.0])

        monkeypatch.setattr(model_service, "is_model_available", lambda: True)
        monkeypatch.setattr(model_service, "_model", NegModel())
        monkeypatch.setattr(model_service, "_model_name", "NegModel")

        client = TestClient(app)
        response = client.get("/api/predict", params=self.VALID_PARAMS)
        assert response.status_code == 200
        assert response.json()["warning"] is None


class TestPrometheusMiddlewareRoutesIntegration:

    def test_dashboard_route_increments_http_counter(self):
        with patch("services.dashboard_service.execute_query", return_value=[]):
            client = TestClient(app)
            before = REQUEST_COUNT.labels(
                method="GET", endpoint="/api/stats/overview", status_code=200
            )._value.get()
            client.get("/api/stats/overview")
            after = REQUEST_COUNT.labels(
                method="GET", endpoint="/api/stats/overview", status_code=200
            )._value.get()
        assert after == before + 1

    def test_predict_route_increments_http_counter(self, monkeypatch):
        monkeypatch.setattr(model_service, "is_model_available", lambda: False)
        client = TestClient(app)
        before = REQUEST_COUNT.labels(
            method="GET", endpoint="/api/predict", status_code=200
        )._value.get()
        client.get("/api/predict", params={
            "distance_km": 100, "duration_h": 1,
            "train_type": "TGV", "traction": "Électrique"
        })
        after = REQUEST_COUNT.labels(
            method="GET", endpoint="/api/predict", status_code=200
        )._value.get()
        assert after == before + 1

    def test_metrics_endpoint_not_instrumented(self):
        client = TestClient(app)
        before = REQUEST_COUNT.labels(
            method="GET", endpoint="/metrics", status_code=200
        )._value.get()
        client.get("/metrics")
        after = REQUEST_COUNT.labels(
            method="GET", endpoint="/metrics", status_code=200
        )._value.get()
        assert after == before

    def test_dashboard_route_records_latency(self):
        with patch("services.dashboard_service.execute_query", return_value=[]):
            client = TestClient(app)
            before = REQUEST_LATENCY.labels(
                method="GET", endpoint="/api/stats/overview"
            )._sum.get()
            client.get("/api/stats/overview")
            after = REQUEST_LATENCY.labels(
                method="GET", endpoint="/api/stats/overview"
            )._sum.get()
        assert after > before


class TestDashboardServiceExecuteQueryIntegration:

    @pytest.mark.asyncio
    async def test_get_agency_sql_contains_limit_param(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = []
            await dashboard_service.get_stats_by_agency(7)
            sql, params = mock_q.call_args[0]
            assert "LIMIT %s" in sql
            assert params == (7,)

    @pytest.mark.asyncio
    async def test_get_emissions_sql_contains_limit_param(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = []
            await dashboard_service.get_emissions_by_route(25)
            sql, params = mock_q.call_args[0]
            assert "LIMIT %s" in sql
            assert params == (25,)

    @pytest.mark.asyncio
    async def test_search_trips_builds_where_clauses_correctly(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = []
            await dashboard_service.search_trips(
                origin="Lyon", destination=None,
                train_type="TER", min_distance=50.0,
                max_distance=None, limit=20
            )
            sql, params = mock_q.call_args[0]
            assert "lo.stop_name LIKE %s" in sql
            assert "tt.train_type = %s" in sql
            assert "ld.stop_name LIKE %s" not in sql
            assert "f.distance_km <= %s" not in sql
            assert params == ("%Lyon%", "TER", 50.0, 20)

    @pytest.mark.asyncio
    async def test_search_trips_no_filters_produces_where_1_equals_1(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = []
            await dashboard_service.search_trips(
                origin=None, destination=None, train_type=None,
                min_distance=None, max_distance=None, limit=None
            )
            sql, params = mock_q.call_args[0]
            assert "WHERE 1=1" in sql
            assert params[-1] == 50

    @pytest.mark.asyncio
    async def test_get_overview_returns_first_row_only(self):
        with patch("services.dashboard_service.execute_query",
                   return_value=[{"total_trips": 5}, {"total_trips": 99}]):
            result = await dashboard_service.get_overview()
        assert result["total_trips"] == 5

    @pytest.mark.asyncio
    async def test_get_health_uses_count_query(self):
        with patch("services.dashboard_service.execute_query") as mock_q:
            mock_q.return_value = [{"count": 123}]
            result = await dashboard_service.get_health()
            sql = mock_q.call_args[0][0]
            assert "COUNT(*)" in sql
            assert "fact_trip_summary" in sql
        assert result["total_trips"] == 123


class TestModelServiceLifespanIntegration:

    def test_lifespan_calls_load_model(self):
        with patch("main.init_db_pool", new_callable=AsyncMock), \
             patch("main.close_db_pool", new_callable=AsyncMock), \
             patch("os.path.exists", return_value=False):
            client = TestClient(app)
            response = client.get("/")
            assert response.status_code == 200

    def test_model_cache_populated_after_lifespan_with_model(self, monkeypatch):
        artifact = {"model": _FakeModel(), "name": "IntegrationTest"}
        with patch("main.init_db_pool", new_callable=AsyncMock), \
             patch("main.close_db_pool", new_callable=AsyncMock), \
             patch("os.path.exists", return_value=True), \
             patch("joblib.load", return_value=artifact):
            with TestClient(app):
                assert model_service._model is not None
                assert model_service._model_name == "IntegrationTest"

    def test_predict_after_load_model_uses_cache(self, monkeypatch):
        monkeypatch.setattr(model_service, "_model", _FakeModel())
        monkeypatch.setattr(model_service, "_model_name", "Cached")
        monkeypatch.setattr(model_service, "is_model_available", lambda: True)

        with patch("joblib.load", side_effect=AssertionError("ne devrait pas charger")):
            result = model_service.predict_co2(100, 1.0, 0, "Régional", "Diesel")

        assert result["warning"] is None
        assert result["model"] == "Cached"