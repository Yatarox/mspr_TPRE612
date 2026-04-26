
import os
import sys
import zipfile
import tempfile
from unittest.mock import patch, Mock

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from scripts.extract_script.gtfs_api import build_download_list, get_latest_gtfs_from_api
from scripts.extract_script.gtfs_download import download_and_extract_gtfs


# ── Helpers ───────────────────────────────────────────────────────────────────

def _api_response(url, filename, dataset_id="ds-1"):
    resp = Mock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {
        "history": [{
            "created_at": "2025-01-01",
            "updated_at": "2025-01-02",
            "payload": {
                "format": "GTFS",
                "permanent_url": url,
                "filename": filename,
                "dataset_id": dataset_id,
            }
        }]
    }
    return resp

def _make_zip_bytes(files=None):
    import io
    files = files or ["stops.txt", "agency.txt"]
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for f in files:
            zf.writestr(f, f"id,name\n1,Test\n")
    return buf.getvalue()


# ── build_download_list → download_and_extract_gtfs ──────────────────────────

def test_build_then_extract_single_source(tmp_path):
    """
    build_download_list produit un map → download_and_extract_gtfs l'exploite
    correctement pour extraire les fichiers et écrire les métadonnées.
    """
    zip_bytes = _make_zip_bytes()

    class _FakeZipResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    with patch("scripts.extract_script.gtfs_api.requests.get") as mock_api:
        mock_api.return_value = _api_response(
            "http://fake/feed.zip", "feed.zip", "ds-sncf"
        )
        download_map = build_download_list(["http://api.example.com/dataset"])

    assert len(download_map) == 1
    assert "http://fake/feed.zip" in download_map

    info = download_map["http://fake/feed.zip"]
    assert info["filename"] == "feed.zip"
    assert info["dataset_id"] == "ds-sncf"

    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeZipResp()):
        result = download_and_extract_gtfs(
            download_map,
            str(tmp_path / "downloads"),
            str(tmp_path / "extracted"),
            force_download=True,
        )

    assert "feed" in result

    import json
    meta = json.loads((tmp_path / "extracted" / "feed" / "metadata.json").read_text())
    assert meta["dataset_id"] == "ds-sncf"
    assert meta["source_url"] == "http://api.example.com/dataset"


def test_build_then_extract_deduplicates_same_url(tmp_path):
    """
    Deux sources API pointant sur le même zip → build_download_list déduplique →
    download_and_extract_gtfs ne télécharge qu'une seule fois.
    """
    zip_bytes = _make_zip_bytes()

    class _FakeZipResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    with patch("scripts.extract_script.gtfs_api.requests.get") as mock_api:
        mock_api.return_value = _api_response("http://fake/same.zip", "same.zip")
        download_map = build_download_list([
            "http://api1.example.com",
            "http://api2.example.com",
        ])

    assert len(download_map) == 1  # dédupliqué par URL

    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeZipResp()) as mock_dl:
        download_and_extract_gtfs(
            download_map,
            str(tmp_path / "downloads"),
            str(tmp_path / "extracted"),
            force_download=True,
        )
        assert mock_dl.call_count == 1


def test_build_then_extract_multiple_sources(tmp_path):
    """
    Deux sources API avec des zips différents → deux datasets extraits.
    """
    zip_bytes_a = _make_zip_bytes(["stops.txt"])
    zip_bytes_b = _make_zip_bytes(["agency.txt", "routes.txt"])

    responses_api = [
        _api_response("http://fake/a.zip", "a.zip", "ds-a"),
        _api_response("http://fake/b.zip", "b.zip", "ds-b"),
    ]

    zip_responses = {
        "http://fake/a.zip": zip_bytes_a,
        "http://fake/b.zip": zip_bytes_b,
    }

    class _FakeZipResp:
        def __init__(self, data): self._data = data
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield self._data

    with patch("scripts.extract_script.gtfs_api.requests.get", side_effect=responses_api):
        download_map = build_download_list([
            "http://api-a.example.com",
            "http://api-b.example.com",
        ])

    assert len(download_map) == 2

    def fake_get(url, **kwargs):
        return _FakeZipResp(zip_responses[url])

    with patch("scripts.extract_script.gtfs_download.requests.get", side_effect=fake_get):
        result = download_and_extract_gtfs(
            download_map,
            str(tmp_path / "downloads"),
            str(tmp_path / "extracted"),
            force_download=True,
        )

    assert "a" in result
    assert "b" in result
    assert (tmp_path / "extracted" / "a" / "stops.txt").exists()
    assert (tmp_path / "extracted" / "b" / "agency.txt").exists()


def test_build_then_extract_api_failure_skips(tmp_path):
    """
    Si une source API échoue, build_download_list la skippe →
    download_and_extract_gtfs ne reçoit que les sources valides.
    """
    zip_bytes = _make_zip_bytes()

    class _FakeZipResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    import requests as req_lib
    def api_side_effect(url, **kwargs):
        if "failing" in url:
            raise req_lib.RequestException("timeout")
        return _api_response("http://fake/ok.zip", "ok.zip")

    with patch("scripts.extract_script.gtfs_api.requests.get", side_effect=api_side_effect):
        download_map = build_download_list([
            "http://api-ok.example.com",
            "http://api-failing.example.com",
        ])

    assert len(download_map) == 1
    assert "http://fake/ok.zip" in download_map


def test_metadata_fields_from_api_are_preserved(tmp_path):
    """
    Les champs created_at / updated_at remontés par l'API se retrouvent
    bien dans metadata.json après extraction.
    """
    zip_bytes = _make_zip_bytes()

    class _FakeZipResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    with patch("scripts.extract_script.gtfs_api.requests.get") as mock_api:
        mock_api.return_value = _api_response("http://fake/timed.zip", "timed.zip")
        download_map = build_download_list(["http://api.example.com"])

    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeZipResp()):
        download_and_extract_gtfs(
            download_map,
            str(tmp_path / "dl"),
            str(tmp_path / "ex"),
            force_download=True,
        )

    import json
    meta = json.loads((tmp_path / "ex" / "timed" / "metadata.json").read_text())
    assert meta["created_at"] == "2025-01-01"
    assert meta["updated_at"] == "2025-01-02"