import os
import sys
from unittest.mock import MagicMock, patch

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from load_script.fact_loader import upsert_dimensions_from_staging, load_fact_table


def test_upsert_dimensions_from_staging_runs_all_inserts():
    hook = MagicMock()
    load_id = 42

    upsert_dimensions_from_staging(hook, load_id)

    # 10 inserts dimensions attendus
    assert hook.run.call_count == 10

    first_sql = hook.run.call_args_list[0].args[0]
    first_params = hook.run.call_args_list[0].kwargs["parameters"]
    assert "INSERT INTO dim_country" in first_sql
    assert first_params == (load_id, load_id)

    last_sql = hook.run.call_args_list[-1].args[0]
    last_params = hook.run.call_args_list[-1].kwargs["parameters"]
    assert "INSERT INTO dim_location" in last_sql
    assert last_params == (load_id, load_id)


@patch("load_script.fact_loader.upsert_dimensions_from_staging")
def test_load_fact_table_happy_path(mock_upsert):
    hook = MagicMock()
    load_id = 7
    hook.get_first.return_value = (123,)

    result = load_fact_table(hook, load_id)

    mock_upsert.assert_called_once_with(hook, load_id)
    assert hook.run.call_count == 1

    fact_sql = hook.run.call_args.args[0]
    fact_params = hook.run.call_args.kwargs["parameters"]
    assert "INSERT INTO fact_trip_summary" in fact_sql
    assert fact_params == (load_id, load_id)

    hook.get_first.assert_called_once_with(
        "SELECT COUNT(*) FROM fact_trip_summary WHERE last_load_id = %s",
        parameters=(load_id,),
    )
    assert result == 123


@patch("load_script.fact_loader.upsert_dimensions_from_staging")
def test_load_fact_table_returns_zero_when_row_none(mock_upsert):
    hook = MagicMock()
    load_id = 7
    hook.get_first.return_value = None

    result = load_fact_table(hook, load_id)

    mock_upsert.assert_called_once_with(hook, load_id)
    assert result == 0


@patch("load_script.fact_loader.upsert_dimensions_from_staging")
def test_load_fact_table_returns_zero_when_count_is_none(mock_upsert):
    hook = MagicMock()
    load_id = 7
    hook.get_first.return_value = (None,)

    result = load_fact_table(hook, load_id)

    mock_upsert.assert_called_once_with(hook, load_id)
    assert result == 0