import os
import shutil
from pathlib import Path

import pytest

from nc_s3_backup.api.backup import (
    REPOSITORY_DIRNAME,
    SNAPSHOT_DIRNAME,
    NextcloudS3Backup,
)
from nc_s3_backup.api.db import NextcloudFile
from nc_s3_backup.cli import parse_config


@pytest.fixture()
def config():
    Path("tests/config.yaml").exists()
    with Path("tests/config.yaml").open("r") as f:
        return parse_config(f)


@pytest.fixture(name="sha1_files")
def prepare_repo_and_snapshots(tmpdir, config):
    """Generate tree files that is expected to works with test/config.yaml
    file in order to test purge.

    for simplicity assuming file with the same name are hard linked:

    .
    └── backup
        ├── data
        │   ├── .data
        │   │   ├── sha1
        │   │   │   └── fe
        │   │   │       ├── abc...
        │   │   │       ├── def...
        │   │   │       └── ghi...
        │   │   └── etag
        │   │       └── fe
        │   │           └── abc...
        │   └── snapshots
        │       ├── 20230105
        │       │   ├── pverkest
        │       │   │   └──  def
        │       │   └── mc
        │       │       └── files
        │       │           └── projects
        │       │               └──  abc
        │       ├── 20230104
        │       │   ├── pverkest
        │       │   │   ├──  abc
        │       │   │   └──  def
        │       │   └── mc
        │       │       └── files
        │       │           └── projects
        │       │               ├──  abc
        │       │               └──  ghi
        │       └── 20230103
        │           ├── pverkest
        │           │   └──  def
        │           └── mc
        │               └── files
        │                   └── projects
        │                       └──  ghi
        └── sensitive_data
            ├── .data
            │   ├── sha1
            │   │   └── fe
            │   │       ├── xyz...
            │   │       ├── uvw...  # un-used file
            │   │       └── rst...
            │   └── etag
            │       └── fe
            │           └── opq...  # un-consistency file
            └── snapshots
                └── 20230105
                    └── mc
                        └── files
                            └── HR
                                ├──  xyz
                                └──  rst
    """

    file_tree = [
        (
            "backup/data",
            [
                (
                    "abc",
                    [
                        f"{REPOSITORY_DIRNAME}/etag/fe/abc",
                        f"{SNAPSHOT_DIRNAME}/20230104/pverkest/abc",
                        f"{SNAPSHOT_DIRNAME}/20230104/mc/files/projects/abc",
                        f"{SNAPSHOT_DIRNAME}/20230105/mc/files/projects/abc",
                    ],
                ),
                (
                    "def",
                    [
                        f"{SNAPSHOT_DIRNAME}/20230105/pverkest/def",
                        f"{SNAPSHOT_DIRNAME}/20230104/pverkest/def",
                        f"{SNAPSHOT_DIRNAME}/20230103/pverkest/def",
                    ],
                ),
                (
                    "ghi",
                    [
                        f"{SNAPSHOT_DIRNAME}/20230104/mc/files/projects/ghi",
                        f"{SNAPSHOT_DIRNAME}/20230103/mc/files/projects/ghi",
                    ],
                ),
            ],
        ),
        (
            "backup/sensitive_data",
            [
                ("opq", [f"{REPOSITORY_DIRNAME}/etag/fe/opq"]),
                (
                    "rst",
                    [
                        f"{SNAPSHOT_DIRNAME}/20230105/mc/files/HR/rst",
                    ],
                ),
                ("uvw", []),
                ("xyz", [f"{SNAPSHOT_DIRNAME}/20230105/mc/files/HR/xyz"]),
            ],
        ),
    ]
    repo_tree = {}
    tmp = Path(str(tmpdir))
    for repo, data in file_tree:
        repo_path = tmp / repo
        (tmp / repo).mkdir(parents=True, exist_ok=True)
        for file_content, link_files in data:
            cur_file = tmp / repo / file_content
            with cur_file.open("w") as f:
                f.write(file_content)
            repo_file = (
                repo_path
                / REPOSITORY_DIRNAME
                / NextcloudFile(
                    checksum=NextcloudS3Backup._compute_sha1(cur_file),
                    fileid=None,
                    storage=None,
                    path=None,
                    size=None,
                ).hash_path
            )
            repo_file.parent.mkdir(parents=True, exist_ok=True)
            cur_file.rename(repo_file)
            repo_tree[file_content] = repo_file
            for link in link_files:
                link_path = repo_path / link
                link_path.parent.mkdir(parents=True, exist_ok=True)
                os.link(repo_file, link_path)
    repo_tree["opq"].unlink()

    for conf in config.mapping:
        conf.backup_root_path = tmp / conf.backup_root_path

    return repo_tree


@pytest.mark.parametrize(
    "remove_directories,expected_existing_files,expected_missing_files",
    [
        pytest.param(
            [],
            [
                "abc",
                "def",
                "ghi",
                "rst",
                "xyz",
            ],
            [
                "opq",
                "uvw",
            ],
            id="test-purge-1",
        ),
        pytest.param(
            ["backup/data/snapshots/20230103"],
            [
                "abc",
                "def",
                "ghi",
                "rst",
                "xyz",
            ],
            [
                "opq",
                "uvw",
            ],
            id="test-purge-2",
        ),
        pytest.param(
            ["backup/data/snapshots/20230103", "backup/data/snapshots/20230104"],
            [
                "abc",
                "def",
                "rst",
                "xyz",
            ],
            [
                "ghi",
                "opq",
                "uvw",
            ],
            id="test-purge-3",
        ),
        pytest.param(
            ["backup/data/snapshots/20230104", "backup/data/snapshots/20230105"],
            [
                "def",
                "ghi",
                "rst",
                "xyz",
            ],
            [
                "abc",
                "opq",
                "uvw",
            ],
            id="test-purge-4",
        ),
        pytest.param(
            [
                "backup/data/snapshots/20230103",
                "backup/data/snapshots/20230104",
                "backup/data/snapshots/20230105",
                "backup/sensitive_data/snapshots/20230105",
            ],
            [],
            [
                "abc",
                "def",
                "ghi",
                "opq",
                "rst",
                "uvw",
                "xyz",
            ],
            id="test-purge-5",
        ),
    ],
)
def test_purge_sha1(
    tmpdir,
    config,
    sha1_files,
    remove_directories,
    expected_existing_files,
    expected_missing_files,
):
    tmp = Path(str(tmpdir))
    for rm_dir in remove_directories:
        shutil.rmtree(tmp / rm_dir)
    backup = NextcloudS3Backup(
        dao=None,
        config=config,
    )
    backup.purge()
    _assert_repo_state(
        sha1_files,
        expected_existing_files=expected_existing_files,
        expected_missing_files=expected_missing_files,
    )


def _assert_repo_state(
    sha1_files, expected_existing_files=None, expected_missing_files=None
):
    assert all(
        [sha1_files[f].exists() for f in expected_existing_files]
    ), "Following file is missing but expected present: {!r}".format(
        [f for f in expected_existing_files if not sha1_files[f].exists()],
    )
    assert all(
        [not sha1_files[f].exists() for f in expected_missing_files]
    ), "Following file is present but expected missing: {!r}".format(
        [f for f in expected_missing_files if sha1_files[f].exists()],
    )


@pytest.mark.parametrize(
    "remove_directories,expected_existing_files,expected_missing_files",
    [
        pytest.param(
            [],
            [
                f"backup/data/{REPOSITORY_DIRNAME}/etag/fe/abc",
            ],
            [
                f"backup/sensitive_data/{REPOSITORY_DIRNAME}/etag/fe/opq",
            ],
            id="test-purge-etag-1",
        ),
        pytest.param(
            [
                "backup/data/snapshots/20230104",
                "backup/data/snapshots/20230105",
            ],
            [],
            [
                f"backup/data/{REPOSITORY_DIRNAME}/etag/fe/abc",
                f"backup/sensitive_data/{REPOSITORY_DIRNAME}/etag/fe/opq",
            ],
            id="test-purge-etag-1",
        ),
    ],
)
def test_purge_etag(
    tmpdir,
    config,
    sha1_files,
    remove_directories,
    expected_existing_files,
    expected_missing_files,
):
    # def test_purge_etag(tmpdir, config, sha1_files ):
    #     remove_directories = []
    #     expected_existing_files = [f"backup/data/{REPOSITORY_DIRNAME}/etag/fe/abc",]
    #     expected_missing_files = [f"backup/sensitive_data/{REPOSITORY_DIRNAME}/etag/fe/opq", ],
    tmp = Path(str(tmpdir))
    for rm_dir in remove_directories:
        shutil.rmtree(tmp / rm_dir)
    backup = NextcloudS3Backup(
        dao=None,
        config=config,
    )
    backup.purge()
    assert all(
        [(tmp / f).exists() for f in expected_existing_files]
    ), "Following file is missing but expected present: {!r}".format(
        [f for f in expected_existing_files if not (tmp / f).exists()],
    )
    assert all(
        [not (tmp / f).exists() for f in expected_missing_files]
    ), "Following file is present but expected missing: {!r}".format(
        [f for f in expected_missing_files if (tmp / f).exists()],
    )
