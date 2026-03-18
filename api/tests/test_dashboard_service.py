import pytest
import asyncio
from unittest.mock import patch
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from services import dashboard_service

@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_overview(mock_execute_query):
    # Arrange
    mock_execute_query.return_value = [{"total_trips": 10}]
    # Act
    result = await dashboard_service.get_overview()
    # Assert
    assert result["total_trips"] == 10

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
    assert result["total_trips"] == 42

@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query", side_effect=Exception("DB error"))
async def test_get_health_unhealthy(mock_execute_query):
    result = await dashboard_service.get_health()
    assert result["status"] == "unhealthy"
    assert result["database"] == "error"
    assert "error" in result

@pytest.mark.asyncio
@patch("services.dashboard_service.execute_query")
async def test_get_stats_by_service_type(mock_execute_query):
    mock_execute_query.return_value = [
        {"service_type": "Jour", "trip_count": 150},
        {"service_type": "Nuit", "trip_count": 80}
    ]
    result = await dashboard_service.get_stats_by_service_type()
    assert len(result) == 2
    assert result[0]["service_type"] == "Jour"