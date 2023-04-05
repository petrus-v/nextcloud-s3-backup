import hashlib
import logging
import os
import statistics
import subprocess  # nosec
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import perf_counter

from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles, NextcloudFile

logger = logging.getLogger(__name__)


REPOSITORY_DIRNAME = ".data"
SNAPSHOT_DIRNAME = "snapshots"

time_reports = {}


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
            self._backup_directory(dir_config)

        self.print_timer_info()
        logger.info("Backup done")

    def print_timer_info(self):
        logger.info("Timmer info...")
        for method_name, times in time_reports.items():
            logger.info(
                "Method %s - Calls count: %d - Total time: %d - AVG: %d - MEDIAN: %d",
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
        # from python 3.10 only
        # local_file.hardlink_to(repo_file)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        os.link(repo_file, local_file)
        return local_file

    @timer
    def _download_s3_file(self, s3_path: Path, download_path: Path):
        s3_path.copy(download_path)

    @timer
    def _compute_sha1(self, file: Path) -> str:
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
        self, dir_config: NextcloudDirectoryConfig, inode: int
    ) -> Path:
        sha1_directory = dir_config.backup_root_path / REPOSITORY_DIRNAME / "sha1"
        command = [
            "find",
            str(sha1_directory),
            "-inum",
            str(inode),
        ]
        logger.debug("Running %s...", command)
        sha1 = None
        try:
            res = subprocess.run(  # nosec
                command, shell=False, check=True, capture_output=True
            )
            res = res.stdout.decode().strip("\n").strip()
            if not res:
                raise Exception(
                    f"No file found in {sha1_directory} directory with inode {inode}"
                )
            sha1 = Path(res)
        except Exception as ex:
            logger.warning("Ignore error while getting sha1 on inode %s: %s", inode, ex)
            sha1 = None
        return sha1

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
            repo_file = self._find_sha1_from_inode(
                dir_config, etag_repo_file.stat().st_ino
            )
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
