import logging
from scripts.extract_script.gtfs_api import get_latest_gtfs_from_api, build_download_list
from scripts.extract_script.gtfs_utils import GTFS_FILES, calculate_file_hash, check_if_already_extracted, write_metadata
from scripts.extract_script.gtfs_download import (
    download_file,
    extract_zip,
    download_and_extract_gtfs,
    clean_old_downloads,
    download_from_direct_urls,
    download_and_unzip_from_zip_urls
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    pass