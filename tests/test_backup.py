import shutil
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
from freezegun import freeze_time

from nc_s3_backup.api.backup import REPOSITORY_DIRNAME, NextcloudS3Backup
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
    )
    f2 = NextcloudFile(
        fileid=88,
        storage=2,
        path="files/other/file.txt",
        checksum="SHA1:0d3ra5ca03e5597312d44b767b4c1394d34d14df",
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


def _test_backup_file(test_dir, s3_present=True, repo_present=False):
    bucket = test_dir / "bucket-test"
    bucket.mkdir(parents=True, exist_ok=True)
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
        checksum="SHA1:00abcd",
    )
    expected_repo_file = root_backup / REPOSITORY_DIRNAME / "sha1" / "00" / "abcd"
    if repo_present:
        expected_repo_file.parent.mkdir(parents=True, exist_ok=True)
        expected_repo_file.touch()
    nc_backup = NextcloudS3Backup(
        DaoNextcloudFiles("postgres://test"),
        config=NextCloudS3BackupConfig(mapping=[nc_dir_conf], backup_date_format="%y"),
    )
    local_file = (
        root_backup
        / datetime.now().strftime("%y")
        / "pverkest"
        / "files/some/path/to/file.txt"
    )
    s3_path = bucket / "urn:oid:33"
    if s3_present:
        s3_path.touch()

    assert s3_path.exists() == s3_present
    assert expected_repo_file.exists() == repo_present
    assert not local_file.exists()
    method_res = nc_backup._backup_file(f1, nc_dir_conf)
    return method_res, s3_path, expected_repo_file, local_file


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_new_file(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local = _test_backup_file(
        Path(str(tmpdir)), s3_present=True, repo_present=False
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_backup_present_file_create_link(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local = _test_backup_file(
        Path(str(tmpdir)), s3_present=True, repo_present=True
    )
    assert res == local
    assert s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_ignore_missing_s3_file_data_exists(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local = _test_backup_file(
        Path(str(tmpdir)), s3_present=False, repo_present=True
    )
    assert res == local
    assert not s3.exists()
    assert repo.exists()
    assert repo.is_file()
    assert local.exists()
    assert local.is_file()


@mock.patch("nc_s3_backup.api.db.Dao")
def test_ignore_missing_s3_file_data_not_exists(dao_mock, tmpdir, patch_path_get):
    res, s3, repo, local = _test_backup_file(
        Path(str(tmpdir)), s3_present=False, repo_present=False
    )
    assert res is None
    assert not s3.exists()
    assert not repo.exists()
    assert not local.exists()


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
