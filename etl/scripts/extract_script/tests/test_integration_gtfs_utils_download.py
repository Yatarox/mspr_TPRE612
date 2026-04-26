
import os
import sys
import zipfile
import hashlib
import tempfile

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)

from scripts.extract_script.gtfs_utils import (
    calculate_file_hash,
    check_if_already_extracted,
    write_metadata,
    GTFS_FILES,
)
from scripts.extract_script.gtfs_download import (
    extract_zip,
    download_and_extract_gtfs,
    download_from_direct_urls,
    clean_old_downloads,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_gtfs_zip(path: str, files=None):
    files = files or ["stops.txt", "agency.txt"]
    with zipfile.ZipFile(path, "w") as zf:
        for f in files:
            zf.writestr(f, f"# {f}\nid,name\n1,Test\n")
    return hashlib.sha256(open(path, "rb").read()).hexdigest()


# ── extract_zip → check_if_already_extracted ──────────────────────────────────

def test_extract_then_check_already_extracted():
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "feed.zip")
        extract_path = os.path.join(tmpdir, "feed")
        file_hash = _make_gtfs_zip(zip_path)

        assert extract_zip(zip_path, extract_path) is True

        write_metadata(extract_path, {"file_hash": file_hash, "source_url": "http://test"})

        assert check_if_already_extracted(tmpdir, "feed.zip", file_hash=file_hash) is True


def test_extract_then_check_wrong_hash():

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "feed.zip")
        extract_path = os.path.join(tmpdir, "feed")
        _make_gtfs_zip(zip_path)

        extract_zip(zip_path, extract_path)
        write_metadata(extract_path, {"file_hash": "oldhash"})

        assert check_if_already_extracted(tmpdir, "feed.zip", file_hash="newhash") is False


def test_extract_zip_produces_expected_files():

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "full.zip")
        extract_path = os.path.join(tmpdir, "full")
        _make_gtfs_zip(zip_path, files=GTFS_FILES)

        assert extract_zip(zip_path, extract_path) is True

        for f in GTFS_FILES:
            fp = os.path.join(extract_path, f)
            assert os.path.exists(fp), f"Fichier manquant : {f}"
            h = calculate_file_hash(fp)
            assert len(h) == 64  # SHA-256 hex


# ── download_and_extract_gtfs (pipeline complet sans réseau) ──────────────────

def test_download_and_extract_gtfs_full_pipeline(tmp_path):

    import shutil
    from unittest.mock import patch

    zip_path = tmp_path / "src" / "data.zip"
    zip_path.parent.mkdir()
    file_hash = _make_gtfs_zip(str(zip_path))

    download_dir = tmp_path / "downloads"
    extract_dir = tmp_path / "extracted"

    download_map = {
        "http://fake/data.zip": {
            "filename": "data.zip",
            "source_url": "http://fake/source",
            "dataset_id": "ds-test",
            "created_at": "2025-01-01",
            "updated_at": "2025-01-02",
        }
    }

    zip_bytes = zip_path.read_bytes()

    class _FakeResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeResp()):
        result = download_and_extract_gtfs(
            download_map,
            str(download_dir),
            str(extract_dir),
            force_download=True,
        )

    assert "data" in result
    assert result["data"] == "http://fake/source"

    extract_path = extract_dir / "data"
    assert (extract_path / "stops.txt").exists()

    import json
    meta = json.loads((extract_path / "metadata.json").read_text())
    assert meta["source_url"] == "http://fake/source"
    assert meta["dataset_id"] == "ds-test"
    assert "file_hash" in meta
    assert "extracted_at" in meta


def test_download_and_extract_gtfs_skip_on_second_run(tmp_path):
    from unittest.mock import patch

    zip_bytes = b""
    with zipfile.ZipFile(__import__("io").BytesIO(), "w") as zf:
        zf.writestr("stops.txt", "id,name\n")
        zip_bytes = __import__("io").BytesIO()
        zf2 = zipfile.ZipFile(zip_bytes, "w")

    extract_dir = tmp_path / "extracted"
    dataset_extract = extract_dir / "data"
    dataset_extract.mkdir(parents=True)
    (dataset_extract / "stops.txt").write_text("id,name\n1,A\n")
    (dataset_extract / "metadata.json").write_text('{"file_hash": "abc"}')

    download_map = {
        "http://fake/data.zip": {
            "filename": "data.zip",
            "source_url": "http://fake/source",
        }
    }

    with patch("scripts.extract_script.gtfs_download.requests.get") as mock_get:
        result = download_and_extract_gtfs(
            download_map,
            str(tmp_path / "downloads"),
            str(extract_dir),
            force_download=False,
        )
        mock_get.assert_not_called()

    assert result == {"data": "http://fake/source"}



def test_download_from_direct_urls_full_pipeline(tmp_path):
    from unittest.mock import patch

    zip_path = tmp_path / "feed.zip"
    _make_gtfs_zip(str(zip_path))
    zip_bytes = zip_path.read_bytes()

    class _FakeResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield zip_bytes

    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeResp()):
        result = download_from_direct_urls(
            ["http://fake/feed.zip"],
            str(tmp_path / "downloads"),
            str(tmp_path / "extracted"),
            force_download=True,
        )

    assert "feed" in result

    import json
    meta_path = tmp_path / "extracted" / "feed" / "metadata.json"
    assert meta_path.exists()
    meta = json.loads(meta_path.read_text())
    assert meta["source_url"] == "http://fake/feed.zip"
    assert "file_hash" in meta


# ── clean_old_downloads + vrai filesystem ─────────────────────────────────────

def test_clean_old_downloads_integration(tmp_path):
    d = tmp_path / "dl"
    d.mkdir()

    files = []
    for i, name in enumerate(["old.zip", "mid.zip", "new.zip"]):
        p = d / name
        _make_gtfs_zip(str(p))
        os.utime(p, (i + 1, i + 1))
        files.append(p)

    clean_old_downloads(str(d), keep_latest=2)

    assert not files[0].exists()  # old.zip supprimé
    assert files[1].exists()      # mid.zip conservé
    assert files[2].exists()      # new.zip conservé


# ── calculate_file_hash cohérence avec download_file ─────────────────────────

def test_hash_consistency_between_download_and_utils(tmp_path):
    from unittest.mock import patch
    import scripts.extract_script.gtfs_download as dl_mod

    content = b"fake zip content for hash test"

    class _FakeResp:
        def raise_for_status(self): pass
        def iter_content(self, chunk_size=8192):
            yield content

    dest = tmp_path / "test.zip"
    with patch("scripts.extract_script.gtfs_download.requests.get", return_value=_FakeResp()):
        returned_hash = dl_mod.download_file(
            "http://fake/test.zip", str(dest), force_download=True
        )

    recalculated = calculate_file_hash(str(dest))
    assert returned_hash == recalculated
    assert returned_hash == hashlib.sha256(content).hexdigest()
