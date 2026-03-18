import pytest
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from fastapi.testclient import TestClient
from unittest.mock import patch
from main import app

client = TestClient(app)

@patch("api.routes.dashboard.dashboard_service.get_stats_by_service_type")
def test_get_stats_by_service_type_endpoint(mock_service):
    mock_service.return_value = [
        {"service_type": "Jour", "trip_count": 150}
    ]
    response = client.get("/stats/by-service-type")
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["service_type"] == "Jour"