import os
import sys
import pytest
from unittest.mock import patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from services import dashboard_service


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_overview(mock_execute_query):
    mock_execute_query.return_value = [{"total_trips": 10}]
    result = await dashboard_service.get_overview()
    assert result["total_trips"] == 10


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_overview_empty(mock_execute_query):
    mock_execute_query.return_value = []
    result = await dashboard_service.get_overview()
    assert result == {}


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_country(mock_execute_query):
    mock_execute_query.return_value = [{"country": "FR", "trip_count": 5}]
    result = await dashboard_service.get_stats_by_country()
    assert result[0]["country"] == "FR"
    assert result[0]["trip_count"] == 5


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_health_healthy(mock_execute_query):
    mock_execute_query.return_value = [{"count": 42}]
    result = await dashboard_service.get_health()
    assert result["status"] == "healthy"
    assert result["database"] == "connected"
    assert result["total_trips"] == 42
    assert "timestamp" in result


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query", side_effect=Exception("DB error"))
async def test_get_health_unhealthy(mock_execute_query):
    result = await dashboard_service.get_health()
    assert result["status"] == "unhealthy"
    assert result["database"] == "error"
    assert result["error"] == "DB error"
    assert "timestamp" in result


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_service_type(mock_execute_query):
    mock_execute_query.return_value = [
        {"service_type": "Jour", "trip_count": 150},
        {"service_type": "Nuit", "trip_count": 80},
    ]
    result = await dashboard_service.get_stats_by_service_type()
    assert len(result) == 2
    assert result[0]["service_type"] == "Jour"


# ...existing code...

@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_train_type(mock_execute_query):
    mock_execute_query.return_value = [{"train_type": "TGV", "trip_count": 8}]
    result = await dashboard_service.get_stats_by_train_type()
    assert result[0]["train_type"] == "TGV"
    assert result[0]["trip_count"] == 8


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_traction(mock_execute_query):
    mock_execute_query.return_value = [{"traction": "Electric", "trip_count": 12}]
    result = await dashboard_service.get_stats_by_traction()
    assert result[0]["traction"] == "Electric"


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_agency(mock_execute_query):
    mock_execute_query.return_value = [{"agency_name": "SNCF", "trip_count": 15}]
    result = await dashboard_service.get_stats_by_agency(10)
    assert result[0]["agency_name"] == "SNCF"

    query, params = mock_execute_query.call_args[0]
    assert "LIMIT %s" in query
    assert params == (10,)


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_emissions_by_route(mock_execute_query):
    mock_execute_query.return_value = [{"route_name": "Paris-Lyon", "total_emissions": 123.4}]
    result = await dashboard_service.get_emissions_by_route(20)
    assert result[0]["route_name"] == "Paris-Lyon"

    query, params = mock_execute_query.call_args[0]
    assert "LIMIT %s" in query
    assert params == (20,)


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_search_trips_with_filters(mock_execute_query):
    mock_execute_query.return_value = [{"trip_id": "T1"}]

    result = await dashboard_service.search_trips(
        origin="Paris",
        destination="Lyon",
        train_type="TGV",
        min_distance=100.0,
        max_distance=900.0,
        limit=5,
    )

    assert result[0]["trip_id"] == "T1"

    query, params = mock_execute_query.call_args[0]
    assert "lo.stop_name LIKE %s" in query
    assert "ld.stop_name LIKE %s" in query
    assert "tt.train_type = %s" in query
    assert "f.distance_km >= %s" in query
    assert "f.distance_km <= %s" in query
    assert params == ("%Paris%", "%Lyon%", "TGV", 100.0, 900.0, 5)


@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_search_trips_without_filters_default_limit(mock_execute_query):
    mock_execute_query.return_value = []

    await dashboard_service.search_trips(
        origin=None,
        destination=None,
        train_type=None,
        min_distance=None,
        max_distance=None,
        limit=None,
    )

    query, params = mock_execute_query.call_args[0]
    assert "WHERE 1=1" in query
    assert params == (50,)