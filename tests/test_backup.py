import hashlib
import os
import shutil
from datetime import datetime
from os import stat_result
from pathlib import Path
from unittest import mock

import pytest
from freezegun import freeze_time

from nc_s3_backup.api.backup import (
    REPOSITORY_DIRNAME,
    SNAPSHOT_DIRNAME,
    NextcloudS3Backup,
)
from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles, NextcloudFile


@pytest.fixture()
def patch_path_get(request):
    """mimic s3 path .get method to download file but
    from Windows or PosixPath used in unit test
    """

    def copy_file(self, dest):
        shutil.copy(self, dest)

    Path.copy = copy_file

    def unpatch_pathlib():
        Path.copy = None

    request.addfinalizer(unpatch_pathlib)


@pytest.fixture()
def patch_stat_result(request):
    def patching(etag_value):
        @property
        def etag(self):
            return etag_value

        stat_result.etag = etag

        def unpatch_stat_result():
            stat_result.etag = None

        request.addfinalizer(unpatch_stat_result)

    return patching


@mock.patch("nc_s3_backup.api.backup.NextcloudS3Backup._backup_file")
@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup(dao_mock, backup_mock):
    nc_dir_conf = NextcloudDirectoryConfig(
        storage_id=2,
        user_name="pverkest",
        bucket="s3://test-bucket",
        nextcloud_path="files/",
        backup_root_path="/var/lib/backups/nextcloud/",
    )

    f1 = NextcloudFile(
        fileid=33,
        storage=2,
        path="files/some/path/to/file.txt",
        checksum="SHA1:00dea5ca03e5597312d44b767b4c1394d34d1623",
        size=2,
    )
    f2 = NextcloudFile(
        fileid=88,
        storage=2,
        path="files/other/file.txt",
        checksum="SHA1:0d3ra5ca03e5597312d44b767b4c1394d34d14df",
        size=5,
    )
    with mock.patch(
        "nc_s3_backup.api.db.DaoNextcloudFiles.get_nc_subtree", return_value=[f1, f2]
    ):
        nc_backup = NextcloudS3Backup(
            DaoNextcloudFiles("postgres://test"),
            config=NextCloudS3BackupConfig(mapping=[nc_dir_conf]),
        )
        nc_backup.backup()
        backup_mock.mock_calls == [
            mock.call(f1, nc_dir_conf),
            mock.call(f2, nc_dir_conf),
        ]


def _test_backup_file(
    test_dir,
    s3_present=True,
    repo_present=False,
    etag_repo_present=False,
    etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
    checksum="SHA1:ba8607f049f59aeadcff2adb9fae48d0cf16b4ad",
    content=b"Binary file contents",
):
    bucket = test_dir / "bucket-test"
    bucket.mkdir(parents=True, exist_ok=True)
    fake_file = test_dir / "test-content-file"
    fake_file.write_bytes(content)
    root_backup = test_dir / "var" / "backup"
    assert not root_backup.exists()

    nc_dir_conf = NextcloudDirectoryConfig(
        storage_id=2,
        user_name="pverkest",
        bucket=bucket,
        nextcloud_path="files/",
        backup_root_path=root_backup,
    )
    f1 = NextcloudFile(
        fileid=33,
        storage=2,
        path="files/some/path/to/file.txt",
        checksum=checksum,
        size=fake_file.stat().st_size,
    )
    sha1 = hashlib.sha1(fake_file.read_bytes()).hexdigest()  # nosec
    expected_repo_file = root_backup / REPOSITORY_DIRNAME / "sha1" / sha1[:2] / sha1[2:]
    if repo_present:
        expected_repo_file.parent.mkdir(parents=True, exist_ok=True)
        os.link(fake_file, expected_repo_file)

    expected_etag_repo_file = (
        root_backup
        / REPOSITORY_DIRNAME
        / "etag"
        / "dd"
        / "0a2a1748da571835f70c95340aa6a7-2"
    )
    if etag_repo_present:
        expected_etag_repo_file.parent.mkdir(parents=True, exist_ok=True)
        os.link(fake_file, expected_etag_repo_file)

    nc_backup = NextcloudS3Backup(
        DaoNextcloudFiles("postgres://test"),
        config=NextCloudS3BackupConfig(mapping=[nc_dir_conf], backup_date_format="%y"),
    )
    local_file = (
        root_backup
        / SNAPSHOT_DIRNAME
        / datetime.now().strftime("%y")
        / "pverkest"
        / "files/some/path/to/file.txt"
    )
    s3_path = bucket / "urn:oid:33"
    if s3_present:
        os.link(fake_file, s3_path)

    assert s3_path.exists() == s3_present
    assert expected_repo_file.exists() == repo_present
    assert not local_file.exists()

    method_res = nc_backup._backup_file(f1, nc_dir_conf)
    return method_res, s3_path, expected_repo_file, local_file, expected_etag_repo_file


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_new_file(dao_mock, tmpdir, patch_path_get):
    res, s3, sha1_repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)), s3_present=True, repo_present=False
    )
    assert res == local
    assert s3.exists()
    assert sha1_repo.exists()
    assert sha1_repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert not etag_repo.exists()
    assert sha1_repo.stat().st_ino == local.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_present_file_create_link(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)), s3_present=True, repo_present=True
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert not etag_repo.exists()
    assert repo.stat().st_ino == local.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_ignore_missing_s3_file_data_exists(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)), s3_present=False, repo_present=True
    )
    assert res == local
    assert not s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert not etag_repo.exists()
    assert repo.stat().st_ino == local.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_ignore_missing_s3_file_data_not_exists(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)), s3_present=False, repo_present=False
    )
    assert res is None
    assert not s3.exists()
    assert not repo.exists()
    assert not local.exists()
    assert not etag_repo.exists()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_download_sha1_mismatch(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)), s3_present=True, repo_present=False, checksum="SHA1:wrong"
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert not etag_repo.exists()
    assert repo.stat().st_ino == local.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_new_file_without_sha1_etag(
    dao_mock, tmpdir, patch_path_get, patch_stat_result
):
    patch_stat_result("dd0a2a1748da571835f70c95340aa6a7-2")
    res, s3, sha1_repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=True,
        repo_present=False,
        etag_repo_present=False,
        etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
        checksum="",
    )
    assert res == local
    assert s3.exists()
    assert sha1_repo.exists()
    assert sha1_repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert etag_repo.exists()
    assert etag_repo.is_file()
    assert sha1_repo.stat().st_ino == local.stat().st_ino == etag_repo.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_without_sha1_etag_sha1_present_file_create_link(
    dao_mock, tmpdir, patch_path_get, patch_stat_result
):
    patch_stat_result("dd0a2a1748da571835f70c95340aa6a7-2")
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=True,
        repo_present=True,
        etag_repo_present=False,
        etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
        checksum="",
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert etag_repo.exists()
    assert etag_repo.is_file()
    assert repo.stat().st_ino == local.stat().st_ino == etag_repo.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_without_sha1_etag_sha1_and_etag_present_file_create_link(
    dao_mock, tmpdir, patch_path_get, patch_stat_result
):
    patch_stat_result("dd0a2a1748da571835f70c95340aa6a7-2")
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=True,
        repo_present=True,
        etag_repo_present=True,
        etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
        checksum="",
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert etag_repo.exists()
    assert etag_repo.is_file()
    assert repo.stat().st_ino == local.stat().st_ino == etag_repo.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_without_sha1_repo_not_present_etag_present(
    dao_mock, tmpdir, patch_path_get, patch_stat_result
):
    patch_stat_result("dd0a2a1748da571835f70c95340aa6a7-2")
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=True,
        repo_present=False,
        etag_repo_present=True,
        etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
        checksum=None,
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()
    assert etag_repo.exists()
    assert etag_repo.is_file()
    assert repo.stat().st_ino == local.stat().st_ino == etag_repo.stat().st_ino


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_without_sha1_ignore_missing_s3_file_data_not_exists(
    dao_mock, tmpdir, patch_path_get
):
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=False,
        repo_present=False,
        etag_repo_present=False,
        etag="ETAG:dd0a2a1748da571835f70c95340aa6a7-2",
        checksum="",
    )
    assert res is None
    assert not s3.exists()
    assert not repo.exists()
    assert not local.exists()
    assert not etag_repo.exists()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_empty_content_do_not_create_data_file_nor_hardlink(
    dao_mock, tmpdir, patch_path_get
):
    """Due to https://github.com/nextcloud/desktop/issues/4909 issue
    on nextcloud client where software use only resource fork to save
    data we are getting a lot of empty file. Knowing empty file is yet
    interessing as file placeholder so we won't create hardlink but
    only touch file to create empty file inplace"""
    res, s3, repo, local, etag_repo = _test_backup_file(
        Path(str(tmpdir)),
        s3_present=True,
        repo_present=False,
        etag_repo_present=False,
        checksum="SHA1:da39a3ee5e6b4b0d3255bfef95601890afd80709",
        content=b"",
    )
    assert res == local
    assert s3.exists()
    assert not repo.exists()
    assert not etag_repo.exists()
    assert local.exists()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_current_backup_formatted_date(dao_mock):

    nc_backup = NextcloudS3Backup(
        DaoNextcloudFiles("postgres://test"),
        config=NextCloudS3BackupConfig(
            mapping=[],
            backup_date_format="%d at %H:%M",
        ),
    )
    with freeze_time("1984-06-15 14:18"):
        assert nc_backup.current_backup_formatted_date == "15 at 14:18"

    # next call should keep first time
    with freeze_time("1985-07-16 15:19"):
        assert nc_backup.current_backup_formatted_date == "15 at 14:18"
