import os
import sys
import tempfile

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from scripts.extract_script.gtfs_download import download_file, extract_zip
from scripts.extract_script.gtfs_api import get_latest_gtfs_from_api


def test_download_gtfs_file_real():
    url = "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com/83582/83582.20260227.001749.202024.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = os.path.join(tmpdir, "feed.zip")
        download_file(url, dest)
        assert os.path.exists(dest)
        assert os.path.getsize(dest) > 0


def test_get_latest_gtfs_from_api_real():
    result = get_latest_gtfs_from_api(
        "https://transport.data.gouv.fr/api/datasets/6853c089b3ed5781f6adfdf7"
    )
    assert isinstance(result, dict)
    assert "filename" in result
    assert "url" in result
    assert "format" in result


def test_extract_gtfs_zip_real():
    url = "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com/83582/83582.20260227.001749.202024.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "feed.zip")
        download_file(url, zip_path)
        extract_path = os.path.join(tmpdir, "extracted")
        extract_zip(zip_path, extract_path)
        assert os.path.exists(os.path.join(extract_path, "agency.txt"))