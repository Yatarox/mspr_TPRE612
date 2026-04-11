import os
import sys
from unittest.mock import MagicMock, patch


sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
)

from load_script.helpers import (
    sanitize_country_for_staging,
    get_column_max_length,
    get_staging_country_limits,
)


def test_sanitize_country_for_staging_basic():
    assert sanitize_country_for_staging("FR", 5, "origin_country") == "FR"
    assert sanitize_country_for_staging("fr", 5, "origin_country") == "FR"
    assert sanitize_country_for_staging("France", 5, "origin_country") == "FRANC"
    assert sanitize_country_for_staging("2022-01-01", 5, "origin_country") is None
    assert sanitize_country_for_staging("UNK", 5, "origin_country") is None
    assert sanitize_country_for_staging(None, 5, "origin_country") is None
    assert sanitize_country_for_staging("", 5, "origin_country") is None


def test_sanitize_country_for_staging_special_cases():
    assert sanitize_country_for_staging("N/A", 5, "origin_country") is None
    assert sanitize_country_for_staging("NULL", 5, "origin_country") is None
    assert sanitize_country_for_staging("   ", 5, "origin_country") is None
    assert sanitize_country_for_staging("F-R!", 5, "origin_country") == "FR"
    assert sanitize_country_for_staging("ABCDEFGHIJK", 5, "origin_country") == "ABCDE"
    assert sanitize_country_for_staging(float("nan"), 5, "origin_country") is None


def test_sanitize_country_for_staging_unknown_variants():
    assert sanitize_country_for_staging("UNKNOWN", 10, "origin_country") is None
    assert sanitize_country_for_staging("UNKN", 10, "origin_country") is None
    assert sanitize_country_for_staging("NA", 10, "origin_country") is None
    assert sanitize_country_for_staging("NONE", 10, "origin_country") is None


def test_sanitize_country_for_staging_date_slash_format():
    assert sanitize_country_for_staging("31/12/2025", 10, "origin_country") is None


def test_sanitize_country_for_staging_becomes_empty_after_cleanup():
    # uniquement des chiffres/symboles -> regex nettoie tout -> None
    assert sanitize_country_for_staging("1234-+_()", 10, "origin_country") is None


def test_sanitize_country_for_staging_logs_warning_on_date_and_truncate():
    with patch("load_script.helpers.logger.warning") as mock_warn:
        assert sanitize_country_for_staging("2025-01-01", 5, "origin_country") is None
        assert sanitize_country_for_staging("ABCDEFGHIJK", 5, "origin_country") == "ABCDE"
        assert mock_warn.call_count == 2


def test_get_column_max_length_success():
    hook = MagicMock()
    hook.get_first.return_value = (42,)
    assert get_column_max_length(hook, "t", "c") == 42


def test_get_column_max_length_returns_none_on_empty_or_exception():
    hook = MagicMock()
    hook.get_first.return_value = None
    assert get_column_max_length(hook, "t", "c") is None

    hook.get_first.side_effect = Exception("boom")
    assert get_column_max_length(hook, "t", "c") is None


def test_get_staging_country_limits_default_and_custom():
    hook = MagicMock()

    # custom
    with patch("load_script.helpers.get_column_max_length", side_effect=[12, 8]):
        assert get_staging_country_limits(hook) == (12, 8)

    # fallback default (30)
    with patch("load_script.helpers.get_column_max_length", side_effect=[None, None]):
        assert get_staging_country_limits(hook) == (30, 30)