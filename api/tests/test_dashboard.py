import sys
import os
from unittest.mock import patch, AsyncMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


@patch("api.routes.dashboard.dashboard_service.get_overview", new_callable=AsyncMock)
def test_get_overview_endpoint(mock_service):
    mock_service.return_value = {"total_trips": 10}
    response = client.get("/api/stats/overview")
    assert response.status_code == 200
    assert response.json()["total_trips"] == 10


@patch("api.routes.dashboard.dashboard_service.get_stats_by_country", new_callable=AsyncMock)
def test_get_stats_by_country_endpoint(mock_service):
    mock_service.return_value = [{"country": "FR", "trip_count": 12}]
    response = client.get("/api/stats/by-country")
    assert response.status_code == 200
    assert response.json()[0]["country"] == "FR"


@patch("api.routes.dashboard.dashboard_service.get_stats_by_train_type", new_callable=AsyncMock)
def test_get_stats_by_train_type_endpoint(mock_service):
    mock_service.return_value = [{"train_type": "TGV", "trip_count": 8}]
    response = client.get("/api/stats/by-train-type")
    assert response.status_code == 200
    assert response.json()[0]["train_type"] == "TGV"


@patch("api.routes.dashboard.dashboard_service.get_stats_by_traction", new_callable=AsyncMock)
def test_get_stats_by_traction_endpoint(mock_service):
    mock_service.return_value = [{"traction": "Electric", "trip_count": 9}]
    response = client.get("/api/stats/by-traction")
    assert response.status_code == 200
    assert response.json()[0]["traction"] == "Electric"


@patch("api.routes.dashboard.dashboard_service.get_stats_by_agency", new_callable=AsyncMock)
def test_get_stats_by_agency_endpoint(mock_service):
    mock_service.return_value = [{"agency_name": "SNCF", "trip_count": 15}]
    response = client.get("/api/stats/by-agency?limit=10")
    assert response.status_code == 200
    assert response.json()[0]["agency_name"] == "SNCF"
    mock_service.assert_awaited_once_with(10)


@patch("api.routes.dashboard.dashboard_service.get_emissions_by_route", new_callable=AsyncMock)
def test_get_emissions_by_route_endpoint(mock_service):
    mock_service.return_value = [{"route_name": "Paris-Lyon", "total_emissions": 123.4}]
    response = client.get("/api/emissions/by-route?limit=20")
    assert response.status_code == 200
    assert response.json()[0]["route_name"] == "Paris-Lyon"
    mock_service.assert_awaited_once_with(20)


@patch("api.routes.dashboard.dashboard_service.search_trips", new_callable=AsyncMock)
def test_search_trips_endpoint(mock_service):
    mock_service.return_value = [{"trip_id": "T1"}]
    response = client.get(
        "/api/trips/search?origin=Paris&destination=Lyon&train_type=TGV&min_distance=100&max_distance=900&limit=5"
    )
    assert response.status_code == 200
    assert response.json()[0]["trip_id"] == "T1"
    mock_service.assert_awaited_once_with(
        origin="Paris",
        destination="Lyon",
        train_type="TGV",
        min_distance=100.0,
        max_distance=900.0,
        limit=5,
    )


@patch("api.routes.dashboard.dashboard_service.get_stats_by_service_type", new_callable=AsyncMock)
def test_get_stats_by_service_type_endpoint(mock_service):
    mock_service.return_value = [{"service_type": "Jour", "trip_count": 150}]
    response = client.get("/api/stats/by-service-type")
    assert response.status_code == 200
    assert response.json()[0]["service_type"] == "Jour"