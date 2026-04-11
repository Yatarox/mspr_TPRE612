import os
import sys
import hashlib
from unittest.mock import patch

sys.path.append(
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
)
from scripts.extract_script import gtfs_download as mod


class _FakeResponse:
    def __init__(self, chunks):
        self._chunks = chunks

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=8192):
        for c in self._chunks:
            yield c


@patch("scripts.extract_script.gtfs_download.calculate_file_hash")
def test_download_file_already_exists(mock_hash, tmp_path):
    file_path = tmp_path / "a.zip"
    file_path.write_bytes(b"abc")
    mock_hash.return_value = "hash123"

    result = mod.download_file("http://x", str(file_path), force_download=False)

    assert result == "hash123"
    mock_hash.assert_called_once_with(str(file_path))


@patch("scripts.extract_script.gtfs_download.requests.get")
def test_download_file_success(mock_get, tmp_path):
    file_path = tmp_path / "b.zip"
    chunks = [b"hello ", b"world"]
    mock_get.return_value = _FakeResponse(chunks)

    result = mod.download_file("http://x/file.zip", str(file_path), force_download=True)

    assert file_path.exists()
    assert file_path.read_bytes() == b"hello world"
    assert result == hashlib.md5(b"hello world").hexdigest()


@patch("scripts.extract_script.gtfs_download.requests.get", side_effect=Exception("boom"))
def test_download_file_failure_returns_none_and_cleans(_mock_get, tmp_path):
    file_path = tmp_path / "c.zip"

    result = mod.download_file("http://x/file.zip", str(file_path), force_download=True)

    assert result is None
    assert not file_path.exists()


@patch("scripts.extract_script.gtfs_download.requests.get")
def test_download_file_success(mock_get, tmp_path):
    file_path = tmp_path / "b.zip"
    chunks = [b"hello ", b"world"]
    mock_get.return_value = _FakeResponse(chunks)

    result = mod.download_file("http://x/file.zip", str(file_path), force_download=True)

    assert file_path.exists()
    assert file_path.read_bytes() == b"hello world"
    assert result == hashlib.sha256(b"hello world").hexdigest()


@patch("scripts.extract_script.gtfs_download.requests.get")
def test_download_file_with_only_empty_chunks(mock_get, tmp_path):
    """Ligne 31: test avec tous les chunks vides"""
    file_path = tmp_path / "only_empty.zip"
    chunks = [b"", b"", b""]
    mock_get.return_value = _FakeResponse(chunks)

    result = mod.download_file("http://x/file.zip", str(file_path), force_download=True)

    assert file_path.exists()
    assert file_path.read_bytes() == b""
    assert result is not None


def test_extract_zip_success(tmp_path):
    zip_path = tmp_path / "d.zip"
    extract_path = tmp_path / "out"

    import zipfile
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("stops.txt", "id,name\n1,A\n")

    ok = mod.extract_zip(str(zip_path), str(extract_path))
    assert ok is True


def test_extract_zip_failure(tmp_path):
    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_text("not a zip", encoding="utf-8")

    ok = mod.extract_zip(str(bad_zip), str(tmp_path / "out2"))
    assert ok is False


def test_extract_zip_no_gtfs_files(tmp_path):
    """Ligne 62: test extraction sans fichiers GTFS"""
    zip_path = tmp_path / "no_gtfs.zip"
    extract_path = tmp_path / "out_no_gtfs"

    import zipfile
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("random.txt", "hello")

    ok = mod.extract_zip(str(zip_path), str(extract_path))
    assert ok is False


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_and_extract_gtfs_happy_path(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    mock_check.return_value = False
    mock_download.return_value = "abc123"
    mock_extract.return_value = True

    download_map = {
        "https://example.com/x.zip": {
            "filename": "x.zip",
            "source_url": "https://source-x",
            "dataset_id": "ds-x",
            "created_at": "2025-01-01",
            "updated_at": "2025-01-02",
        }
    }

    out = mod.download_and_extract_gtfs(
        download_map,
        str(tmp_path / "downloads"),
        str(tmp_path / "extract"),
        force_download=False,
    )

    assert out == {"x": "https://source-x"}
    mock_write.assert_called_once()


@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_and_extract_gtfs_skip_if_already_extracted(
    mock_check, mock_download, mock_extract, tmp_path
):
    mock_check.return_value = True

    download_map = {
        "https://example.com/x.zip": {
            "filename": "x.zip",
            "source_url": "https://source-x",
        }
    }

    out = mod.download_and_extract_gtfs(
        download_map, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {"x": "https://source-x"}
    mock_download.assert_not_called()
    mock_extract.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_and_extract_gtfs_download_fails(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Ligne 79: test échec du téléchargement"""
    mock_check.return_value = False
    mock_download.return_value = None

    download_map = {
        "https://example.com/x.zip": {
            "filename": "x.zip",
            "source_url": "https://source-x",
        }
    }

    out = mod.download_and_extract_gtfs(
        download_map, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {}
    mock_write.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_and_extract_gtfs_extract_fails(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Ligne 82: test échec de l'extraction"""
    mock_check.return_value = False
    mock_download.return_value = "hash1"
    mock_extract.return_value = False

    download_map = {
        "https://example.com/x.zip": {
            "filename": "x.zip",
            "source_url": "https://source-x",
        }
    }

    out = mod.download_and_extract_gtfs(
        download_map, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {}
    mock_write.assert_not_called()


def test_clean_old_downloads_keeps_latest(tmp_path):
    d = tmp_path / "downloads"
    d.mkdir()
    f1 = d / "a.zip"
    f2 = d / "b.zip"
    f3 = d / "c.zip"
    f1.write_bytes(b"a")
    f2.write_bytes(b"b")
    f3.write_bytes(b"c")

    os.utime(f1, (1, 1))
    os.utime(f2, (2, 2))
    os.utime(f3, (3, 3))

    mod.clean_old_downloads(str(d), keep_latest=2)

    assert f3.exists()
    assert f2.exists()
    assert not f1.exists()


def test_clean_old_downloads_dir_not_exists(tmp_path):
    """Lignes 92-93: test dossier qui n'existe pas"""
    non_existent = str(tmp_path / "non_existent_downloads_xyz")
    mod.clean_old_downloads(non_existent, keep_latest=2)


def test_clean_old_downloads_not_enough_files(tmp_path):
    """Ligne 93: test quand y a moins de fichiers que keep_latest"""
    d = tmp_path / "downloads2"
    d.mkdir()
    (d / "a.zip").write_bytes(b"a")

    mod.clean_old_downloads(str(d), keep_latest=2)

    assert (d / "a.zip").exists()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_happy_path(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    mock_check.return_value = False
    mock_download.return_value = "hash1"
    mock_extract.return_value = True

    urls = [" https://example.com/mydata "]
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {"mydata": "https://example.com/mydata"}
    mock_write.assert_called_once()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_skip_if_extracted(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Ligne 108-109: test skip si déjà extrait"""
    mock_check.return_value = True
    mock_download.return_value = "hash1"
    mock_extract.return_value = True

    urls = ["https://example.com/data.zip"]
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {"data": "https://example.com/data.zip"}
    mock_download.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_download_fails(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Ligne 112: test échec du téléchargement"""
    mock_check.return_value = False
    mock_download.return_value = None

    urls = ["https://example.com/fail.zip"]
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {}
    mock_write.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_extract_fails(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Test échec extraction dans download_from_direct_urls"""
    mock_check.return_value = False
    mock_download.return_value = "hash1"
    mock_extract.return_value = False

    urls = ["https://example.com/fail_extract.zip"]
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {}
    mock_write.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_empty_list(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Test avec liste vide"""
    urls = []
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert out == {}
    mock_download.assert_not_called()


@patch("scripts.extract_script.gtfs_download.write_metadata")
@patch("scripts.extract_script.gtfs_download.extract_zip")
@patch("scripts.extract_script.gtfs_download.download_file")
@patch("scripts.extract_script.gtfs_download.check_if_already_extracted")
def test_download_from_direct_urls_with_whitespace_urls(
    mock_check, mock_download, mock_extract, mock_write, tmp_path
):
    """Test avec URLs avec espaces et URLs vides"""
    mock_check.return_value = False
    mock_download.return_value = "hash1"
    mock_extract.return_value = True

    urls = ["  ", "", "  https://example.com/data.zip  ", "  https://example.com/other.zip  "]
    out = mod.download_from_direct_urls(
        urls, str(tmp_path / "d"), str(tmp_path / "e"), force_download=False
    )

    assert len(out) == 2
    assert "data" in out
    assert "other" in out


def test_clean_old_downloads_delete_fails(tmp_path, monkeypatch):
    """Test quand la suppression échoue"""
    d = tmp_path / "downloads_fail"
    d.mkdir()
    (d / "a.zip").write_bytes(b"a")
    (d / "b.zip").write_bytes(b"b")

    os.utime(d / "a.zip", (1, 1))
    os.utime(d / "b.zip", (2, 2))

    def fake_remove(path):
        raise Exception("Permission denied")

    monkeypatch.setattr("os.remove", fake_remove)
    mod.clean_old_downloads(str(d), keep_latest=1)

    assert (d / "a.zip").exists()

@patch("scripts.extract_script.gtfs_download.requests.get")
def test_download_file_chunk_filtering(mock_get, tmp_path):
    """Teste que les chunks vides sont filtrés (ligne 31: if chunk)"""
    file_path = tmp_path / "filtered.zip"
    chunks = [b"part1", b"", b"part2"]
    mock_get.return_value = _FakeResponse(chunks)

    result = mod.download_file("http://x/file.zip", str(file_path), force_download=True)

    expected_hash = hashlib.sha256(b"part1part2").hexdigest()
    assert result == expected_hash