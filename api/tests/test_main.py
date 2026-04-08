import os
import sys
from unittest.mock import AsyncMock, patch

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from fastapi.testclient import TestClient
from main import app


def test_read_root():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {
        "name": "Rail Data Warehouse API",
        "version": "1.0.0",
        "docs": "/docs",
    }

@patch("api.routes.dashboard.dashboard_service.get_health", new_callable=AsyncMock)
def test_health_check(mock_get_health):
    mock_get_health.return_value = {"status": "ok"}
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("main.init_db_pool", new_callable=AsyncMock)
@patch("main.close_db_pool", new_callable=AsyncMock)
def test_lifespan_calls_db_pool(mock_close_db_pool, mock_init_db_pool):
    with TestClient(app):
        pass

    mock_init_db_pool.assert_awaited_once()
    mock_close_db_pool.assert_awaited_once()