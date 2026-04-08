import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))
from models import database


@pytest.mark.asyncio
@patch("models.database.aiomysql.create_pool", new_callable=AsyncMock)
async def test_init_db_pool_sets_global_pool(mock_create_pool):
    fake_pool = MagicMock()
    mock_create_pool.return_value = fake_pool

    await database.init_db_pool()

    mock_create_pool.assert_awaited_once()
    assert database.pool is fake_pool


@pytest.mark.asyncio
async def test_close_db_pool_closes_and_waits():
    fake_pool = MagicMock()
    fake_pool.wait_closed = AsyncMock()

    database.pool = fake_pool
    await database.close_db_pool()

    fake_pool.close.assert_called_once()
    fake_pool.wait_closed.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_query_runs_and_returns_rows():
    rows = [{"id": 1, "name": "test"}]

    # cursor mock
    mock_cursor = AsyncMock()
    mock_cursor.execute = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=rows)

    cursor_cm = AsyncMock()
    cursor_cm.__aenter__.return_value = mock_cursor

    # connection mock
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = cursor_cm

    conn_cm = AsyncMock()
    conn_cm.__aenter__.return_value = mock_conn

    # pool mock
    fake_pool = MagicMock()
    fake_pool.acquire.return_value = conn_cm
    database.pool = fake_pool

    query = "SELECT 1"
    params = ("x",)

    result = await database.execute_query(query, params)

    mock_cursor.execute.assert_awaited_once_with(query, params)
    mock_cursor.fetchall.assert_awaited_once()
    assert result == rows