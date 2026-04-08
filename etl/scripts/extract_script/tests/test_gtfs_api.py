import requests
from unittest.mock import Mock, patch

import sys
import os

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)
from scripts.extract_script.gtfs_api import get_latest_gtfs_from_api, build_download_list


def _mock_response(json_payload, status_ok=True):
    resp = Mock()
    resp.json.return_value = json_payload
    if status_ok:
        resp.raise_for_status.return_value = None
    else:
        resp.raise_for_status.side_effect = requests.HTTPError("HTTP error")
    return resp


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_success(mock_get):
    mock_get.return_value = _mock_response(
        {
            "history": [
                {
                    "created_at": "2025-01-01",
                    "updated_at": "2025-01-02",
                    "payload": {
                        "format": "GTFS",
                        "permanent_url": "https://example.com/a.zip",
                        "filename": "a.zip",
                        "dataset_id": "ds_1",
                    },
                }
            ]
        }
    )

    result = get_latest_gtfs_from_api("https://api.example.com")
    assert result is not None
    assert result["url"] == "https://example.com/a.zip"
    assert result["filename"] == "a.zip"
    assert result["dataset_id"] == "ds_1"
    assert result["format"] == "GTFS"


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_no_history(mock_get):
    mock_get.return_value = _mock_response({"history": []})
    assert get_latest_gtfs_from_api("https://api.example.com") is None


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_no_gtfs_in_first_5(mock_get):
    mock_get.return_value = _mock_response(
        {
            "history": [
                {"payload": {"format": "CSV"}},
                {"payload": {"format": "JSON"}},
                {"payload": {"format": "TXT"}},
                {"payload": {"format": "XML"}},
                {"payload": {"format": "PDF"}},
                {"payload": {"format": "GTFS", "permanent_url": "https://x", "filename": "x.zip"}},
            ]
        }
    )
    assert get_latest_gtfs_from_api("https://api.example.com") is None


@patch("scripts.extract_script.gtfs_api.requests.get", side_effect=requests.RequestException("boom"))
def test_get_latest_gtfs_from_api_request_exception(_mock_get):
    assert get_latest_gtfs_from_api("https://api.example.com") is None


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_with_fallback_dates(mock_get):
    """Test quand created_at/updated_at sont dans item au lieu de payload"""
    mock_get.return_value = _mock_response(
        {
            "history": [
                {
                    "created_at": "2025-01-01",
                    "updated_at": "2025-01-02",
                    "payload": {
                        "format": "GTFS",
                        "permanent_url": "https://example.com/b.zip",
                        "filename": "b.zip",
                        "dataset_id": "ds_2",
                    },
                }
            ]
        }
    )
    result = get_latest_gtfs_from_api("https://api.example.com")
    assert result is not None
    assert result["created_at"] == "2025-01-01"
    assert result["updated_at"] == "2025-01-02"


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_missing_optional_fields(mock_get):
    """Test avec dataset_id manquant"""
    mock_get.return_value = _mock_response(
        {
            "history": [
                {
                    "payload": {
                        "format": "GTFS",
                        "permanent_url": "https://example.com/c.zip",
                        "filename": "c.zip",
                    }
                }
            ]
        }
    )
    result = get_latest_gtfs_from_api("https://api.example.com")
    assert result is not None
    assert result["dataset_id"] == ""


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_http_error(mock_get):
    """Test HTTPError après raise_for_status"""
    mock_get.return_value = _mock_response({}, status_ok=False)
    result = get_latest_gtfs_from_api("https://api.example.com")
    assert result is None


@patch("scripts.extract_script.gtfs_api.requests.get")
def test_get_latest_gtfs_from_api_json_decode_error(mock_get):
    """Test erreur JSON parsing"""
    mock_resp = Mock()
    mock_resp.raise_for_status.return_value = None
    mock_resp.json.side_effect = ValueError("Invalid JSON")
    mock_get.return_value = mock_resp
    
    result = get_latest_gtfs_from_api("https://api.example.com")
    assert result is None


def test_build_download_list_empty_urls():
    """Test avec liste vide"""
    result = build_download_list([])
    assert result == {}


@patch("scripts.extract_script.gtfs_api.get_latest_gtfs_from_api")
def test_build_download_list_all_none(mock_latest):
    """Test quand toutes les APIs retournent None"""
    mock_latest.return_value = None
    result = build_download_list(["https://api1", "https://api2"])
    assert result == {}


@patch("scripts.extract_script.gtfs_api.get_latest_gtfs_from_api")
def test_build_download_list_filters_and_deduplicates(mock_latest):
    mock_latest.side_effect = [
        {"url": "https://example.com/a.zip", "filename": "a.zip"},
        {"url": "https://example.com/a.zip", "filename": "a.zip"},  # duplicate url
        None,
    ]

    result = build_download_list(["  https://api1  ", "https://api2", "", "https://api3"])
    assert len(result) == 1
    assert "https://example.com/a.zip" in result