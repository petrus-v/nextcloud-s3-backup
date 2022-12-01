from unittest import mock

from nc_s3_backup.api.backup import NextcloudS3Backup
from nc_s3_backup.cli import main


@mock.patch("nc_s3_backup.api.backup.NextcloudS3Backup.backup")
@mock.patch("nc_s3_backup.api.db.Dao")
def test_main_cli(dao_mock, backup_mock):
    with mock.patch(
        "sys.argv",
        [
            "nextcloud-s3-backup-prog",
            "-l",
            "DEBUG",
            "--s3-access-key",
            "s3-access-test",
            "--s3-secret-key",
            "s3-secret-test",
            "--s3-region-name",
            "s3-reg-test",
            "--s3-endpoint-url",
            "https://my-s3-endpoint.test",
            "--pg-dsn",
            "postgresql:///testdb",
            "tests/config.yaml",
        ],
    ):
        nc_s3_backup = main(testing=True)

    assert isinstance(nc_s3_backup, NextcloudS3Backup)
    backup_mock.assert_called_once()
    assert nc_s3_backup.config.backup_date_format == "%y%m"
    assert nc_s3_backup.config.excluded_mimetype_ids == [15, 84]
    assert len(nc_s3_backup.config.mapping) == 3
    assert str(nc_s3_backup.config.mapping[0].backup_root_path) == "backup/data"
    assert nc_s3_backup.config.mapping[0].bucket.as_uri() == "s3://test-bucket"
    assert nc_s3_backup.config.mapping[0].nextcloud_path == ""
    assert nc_s3_backup.config.mapping[0].storage_id == 2
    assert nc_s3_backup.config.mapping[0].user_name == "pverkest"
    assert nc_s3_backup.config.mapping[1].nextcloud_path == "files/projects/"
