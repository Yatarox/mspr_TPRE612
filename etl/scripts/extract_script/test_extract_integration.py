import tempfile
from scripts.extract_script.gtfs_download import download_file
from scripts.extract_script.gtfs_download import extract_zip
from scripts.extract_script.gtfs_api import get_latest_gtfs_from_api
import os
def test_download_gtfs_file_real():
    url = "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com/83582/83582.20260227.001749.202024.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = f"{tmpdir}/feed.zip"
        download_file(url, dest)
        assert os.path.exists(dest)
        assert os.path.getsize(dest) > 0



def test_get_latest_gtfs_from_api_real():
    result = get_latest_gtfs_from_api()
    assert isinstance(result, dict)
    assert "feeds" in result or "results" in result



def test_extract_gtfs_zip_real():
    url = "https://transport-data-gouv-fr-resource-history-prod.cellar-c2.services.clever-cloud.com/83582/83582.20260227.001749.202024.zip"
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = f"{tmpdir}/feed.zip"
        download_file(url, zip_path)
        extract_path = f"{tmpdir}/extracted"
        extract_zip(zip_path, extract_path)
        assert os.path.exists(f"{extract_path}/agency.txt")