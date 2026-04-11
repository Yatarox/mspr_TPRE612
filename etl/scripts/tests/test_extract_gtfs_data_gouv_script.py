import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


def test_gtfs_api_imports():
    from scripts.extract_script.gtfs_api import build_download_list, get_latest_gtfs_from_api

    assert callable(get_latest_gtfs_from_api)
    assert callable(build_download_list)


def test_gtfs_utils_imports():
    from scripts.extract_script.gtfs_utils import (
        GTFS_FILES,
        calculate_file_hash,
        check_if_already_extracted,
        write_metadata,
    )

    assert GTFS_FILES is not None
    assert callable(calculate_file_hash)
    assert callable(check_if_already_extracted)
    assert callable(write_metadata)


def test_gtfs_download_imports():
    from scripts.extract_script.gtfs_download import (
        clean_old_downloads,
        download_and_extract_gtfs,
        download_and_unzip_from_zip_urls,
        download_file,
        download_from_direct_urls,
        extract_zip,
    )

    assert callable(download_file)
    assert callable(extract_zip)
    assert callable(download_and_extract_gtfs)
    assert callable(clean_old_downloads)
    assert callable(download_from_direct_urls)
    assert callable(download_and_unzip_from_zip_urls)


def test_extract_script_main_exports():
    import extract_gtfs_data_gouv_script as script

    assert hasattr(script, "get_latest_gtfs_from_api")
    assert hasattr(script, "build_download_list")