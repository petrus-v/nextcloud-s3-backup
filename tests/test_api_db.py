from nc_s3_backup.api.db import NextcloudFile


def test_nc_file():
    nc_file = NextcloudFile(
        fileid=33,
        storage=99,
        path="/files/some/path/to/file.txt",
        checksum="SHA1:00dea5ca03e5597312d44b767b4c1394d34d1623",
    )
    assert str(nc_file.hash_path) == "sha1/00/dea5ca03e5597312d44b767b4c1394d34d1623"
