import hashlib
import logging
import os
import statistics
from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Set

from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles, NextcloudFile

logger = logging.getLogger(__name__)


REPOSITORY_DIRNAME = ".data"
SNAPSHOT_DIRNAME = "snapshots"
GB = 1024 * 1024 * 1024
time_reports = {}

PurgedFile = namedtuple("PurgedFile", ["size"])


def timer(func):
    def wrap_func(*args, **kwargs):
        t1 = perf_counter()
        result = func(*args, **kwargs)
        t2 = perf_counter()
        if func.__name__ not in time_reports.keys():
            time_reports[func.__name__] = []
        time_reports[func.__name__].append(t2 - t1)
        return result

    return wrap_func


@dataclass
class NextcloudS3Backup:
    """Main class that download files on the locale FS"""

    dao: DaoNextcloudFiles
    config: NextCloudS3BackupConfig

    _current_backup_formatted_date: datetime = None

    _sha1_file_per_inode: Dict[int, Path] = None

    @timer
    def populate_sha1_file_per_inode(self, dir_config: NextcloudDirectoryConfig):
        sha1_dir = dir_config.backup_root_path / REPOSITORY_DIRNAME / "sha1"
        if sha1_dir.exists():
            self._sha1_file_per_inode = self._populate_sha1_file_per_inode(
                dir_config.backup_root_path / REPOSITORY_DIRNAME / "sha1", {}
            )
        else:
            self._sha1_file_per_inode = {}

    def _populate_sha1_file_per_inode(
        self, directory: Path, inodes: Dict[int, Path] = None
    ) -> Set[int]:
        for child in directory.iterdir():
            if child.is_dir():
                inodes = self._populate_sha1_file_per_inode(child, inodes=inodes)
            else:
                inodes[child.stat().st_ino] = child
        return inodes

    def _ensure_sha1_file_per_inode_exists(self, repo_file):
        self._sha1_file_per_inode[repo_file.stat().st_ino] = repo_file

    @property
    def current_backup_formatted_date(self):
        if not self._current_backup_formatted_date:
            self._current_backup_formatted_date = datetime.now().strftime(
                self.config.backup_date_format
            )
        return self._current_backup_formatted_date

    def backup(self):
        logger.info("%s mapping to backup", len(self.config.mapping))
        for dir_config in self.config.mapping:
            self.populate_sha1_file_per_inode(dir_config)
            self._backup_directory(dir_config)

        self.print_timer_info()
        logger.info("Backup done")

    @property
    def distinct_backup_root_paths(self):
        return list({conf.backup_root_path for conf in self.config.mapping})

    @timer
    def purge(self):
        purged = []
        logger.info("Purging %d directories", len(self.distinct_backup_root_paths))
        for root_path in self.distinct_backup_root_paths:
            snapshots_inodes = self._get_inodes(root_path / SNAPSHOT_DIRNAME)
            repo_purged = self._purge_directory(
                root_path / REPOSITORY_DIRNAME / "sha1", snapshots_inodes
            )
            logger.info(
                "**SHA1** Directory: %s - %d file(s) removed that represent %.3f GB",
                root_path,
                len(repo_purged),
                sum([f.size for f in repo_purged]),
            )
            purged.extend(repo_purged)
            # purging etag separately because:
            # * sha1 and etags are hard linked to and we just purge sha1
            #   files that are not present in snapshots
            # * we don't want to sum etag and sha1 file size
            etag_purged = self._purge_directory(
                root_path / REPOSITORY_DIRNAME / "etag", snapshots_inodes
            )
            logger.info(
                "**Etag** Directory: %s - %d file(s) removed that represent %.3f GB",
                root_path,
                len(etag_purged),
                sum([f.size for f in etag_purged]),
            )
        self.print_timer_info()
        logger.info(
            "Total purged: %d file(s) removed that represent %.3f GB",
            len(repo_purged) + len(etag_purged),
            sum([f.size for f in repo_purged]),
        )

    @timer
    def _get_inodes(self, directory: Path, inodes: Set[int] = None) -> Set[int]:
        if not inodes:
            inodes = set()
        for child in directory.iterdir():
            if child.is_dir():
                inodes = self._get_inodes(child, inodes=inodes)
            else:
                inodes |= {child.stat().st_ino}
        return inodes

    @timer
    def _purge_directory(
        self, repo_directory: Path, snapshots_inodes: Set[int]
    ) -> List[PurgedFile]:
        purged = []
        for child in repo_directory.iterdir():
            if child.is_dir():
                purged.extend(self._purge_directory(child, snapshots_inodes))
            else:
                purged.extend(self._purge_file(child, snapshots_inodes))
        return purged

    @timer
    def _purge_file(
        self, repo_file: Path, snapshots_inodes: Set[int]
    ) -> List[PurgedFile]:
        unlink_file_stat = []
        if repo_file.stat().st_ino not in snapshots_inodes:
            unlink_file_stat.append(PurgedFile(size=repo_file.stat().st_size / GB))
            repo_file.unlink()
        return unlink_file_stat

    def print_timer_info(self):
        logger.info("Timmer info...")
        for method_name, times in time_reports.items():
            logger.info(
                "Method %s - Calls count: %d - Total time: %.1f - AVG: %.5f - MEDIAN: %.5f",
                method_name,
                len(times),
                sum(times),
                statistics.mean(times),
                statistics.median(times),
            )

    def _backup_directory(self, dir_config: NextcloudDirectoryConfig):
        logger.info(
            "Backup-ing %s - %s ...", dir_config.user_name, dir_config.nextcloud_path
        )
        for nc_file in self.dao.get_nc_subtree(
            dir_config.storage_id,
            dir_config.nextcloud_path,
            self.config.excluded_mimetype_ids,
        ):
            self._backup_file(nc_file, dir_config)

    @timer
    def _backup_file(
        self, nc_file: NextcloudFile, dir_config: NextcloudDirectoryConfig
    ):
        local_file = (
            dir_config.backup_root_path
            / SNAPSHOT_DIRNAME
            / self.current_backup_formatted_date
            / dir_config.user_name
        ) / nc_file.path
        if nc_file.size == 0:
            # mainly caused by this issue
            # https://github.com/nextcloud/desktop/issues/4909
            # (files with data store in resource fork only are not synced
            # by nextcloud client) we got a lot of empty file in our nextcloud
            # so we get the hardlink count limit maximum (~65000 links with less
            # than 30 snapshots)
            # In such case of empty file we leave placeholder creating new empty file
            # instead hard links based on the nextcloud table information
            local_file.parent.mkdir(parents=True, exist_ok=True)
            local_file.touch()
            return local_file

        s3_path = dir_config.bucket / f"urn:oid:{nc_file.fileid}"
        logger.debug(
            "Backup-ing %s - %s - %s to %s...",
            dir_config.user_name,
            nc_file.path,
            s3_path,
            local_file,
        )
        if nc_file.checksum and nc_file.checksum.lower().startswith("sha1"):
            repo_file = self._backup_file_with_sha1(nc_file, dir_config, s3_path)
            if not repo_file:
                return
        else:
            repo_file = self._backup_file_without_sha1(nc_file, dir_config, s3_path)
            if not repo_file:
                return
        self._ensure_sha1_file_per_inode_exists(repo_file)
        # from python 3.10 only
        # local_file.hardlink_to(repo_file)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        os.link(repo_file, local_file)
        return local_file

    @timer
    def _download_s3_file(self, s3_path: Path, download_path: Path):
        s3_path.copy(download_path)

    @classmethod
    @timer
    def _compute_sha1(cls, file: Path) -> str:
        # consider stream file as allowed from python 3.11
        return f"SHA1:{hashlib.sha1(file.read_bytes()).hexdigest()}"  # nosec

    @timer
    def _backup_file_with_sha1(
        self,
        nc_file: NextcloudFile,
        dir_config: NextcloudDirectoryConfig,
        s3_path: Path,
    ) -> Path:
        repo_file = dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
        if not repo_file.exists():
            if s3_path.exists():
                downloading_path = repo_file.with_suffix(".downloading")
                downloading_path.parent.mkdir(parents=True, exist_ok=True)
                self._download_s3_file(s3_path, downloading_path)
                sha1 = self._compute_sha1(downloading_path)
                if sha1.lower() != nc_file.checksum.lower():
                    logger.warning(
                        "SHA1 hash mismatched on file %s (%s). "
                        "NC table: %s - downloaded: %s. "
                        "Use local downloaded file hash instead.",
                        nc_file.fileid,
                        nc_file.path,
                        nc_file.checksum,
                        sha1,
                    )
                    nc_file.checksum = sha1
                    repo_file = (
                        dir_config.backup_root_path
                        / REPOSITORY_DIRNAME
                        / nc_file.hash_path
                    )
                    repo_file.parent.mkdir(parents=True, exist_ok=True)
                downloading_path.rename(repo_file)
            else:
                logger.warning(
                    "Ignoring Nextcloud record DB file not found on s3. "
                    "Storage: %s - path %s",
                    nc_file.storage,
                    s3_path,
                )
                return
        return repo_file

    @timer
    def _backup_file_without_sha1(
        self,
        nc_file: NextcloudFile,
        dir_config: NextcloudDirectoryConfig,
        s3_path: Path,
    ):
        if s3_path.exists():
            return self._backup_file_with_etag(
                nc_file,
                dir_config,
                s3_path,
                f"ETAG:{s3_path.stat().etag}",
            )
        else:
            logger.warning(
                "Ignoring Nextcloud record DB file not found on s3. "
                "Storage: %s - path %s",
                nc_file.storage,
                s3_path,
            )
            return

    @timer
    def _find_sha1_from_inode(
        self, dir_config: NextcloudDirectoryConfig, searched_file: Path
    ) -> Path:
        sha1_directory = dir_config.backup_root_path / REPOSITORY_DIRNAME / "sha1"
        repo_file = self._find_files_with_same_inode_as(sha1_directory, searched_file)
        if repo_file:
            # only the first one we shouldn't get two here
            return repo_file
        logger.warning(
            "No sha1 files found searching files %s (inode: %s) in %s",
            searched_file,
            searched_file.stat().st_ino,
            sha1_directory,
        )
        return repo_file

    def _find_files_with_same_inode_as(
        self, root_search_directory: Path, searched_file: Path
    ):
        return self._sha1_file_per_inode.get(searched_file.stat().st_ino)

    @timer
    def _backup_file_with_etag(
        self,
        nc_file: NextcloudFile,
        dir_config: NextcloudDirectoryConfig,
        s3_path: Path,
        etag: str,
    ) -> Path:
        nc_file.checksum = etag
        etag_repo_file = (
            dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
        )
        repo_file = None
        if not etag_repo_file.exists():
            downloading_path = etag_repo_file.with_suffix(".downloading")
            downloading_path.parent.mkdir(parents=True, exist_ok=True)
            self._download_s3_file(s3_path, downloading_path)
            nc_file.checksum = self._compute_sha1(downloading_path)
            repo_file = (
                dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
            )
            if repo_file.exists():
                downloading_path.unlink()
                os.link(repo_file, etag_repo_file)
            else:
                downloading_path.rename(etag_repo_file)
                repo_file.parent.mkdir(parents=True, exist_ok=True)
                os.link(etag_repo_file, repo_file)
        else:
            repo_file = self._find_sha1_from_inode(dir_config, etag_repo_file)
            if not repo_file or not repo_file.exists():
                # weird case
                nc_file.checksum = self._compute_sha1(etag_repo_file)
                repo_file = (
                    dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
                )
                if repo_file.exists():
                    # assuming inconsistency data wrongly synced
                    # .data losing hardlink
                    # in such case the best thing to do is to recreate
                    # etag from sha1 as long snapshot files point to sha1 files
                    # we do not want to remove sha1
                    etag_repo_file.unlink()
                    os.link(repo_file, etag_repo_file)
                else:
                    repo_file.parent.mkdir(parents=True, exist_ok=True)
                    os.link(etag_repo_file, repo_file)
            else:
                sha1 = "".join(repo_file.parts[:2])
                nc_file.checksum = f"SHA1:{sha1}"

        return repo_file
