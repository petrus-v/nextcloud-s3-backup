import hashlib
import logging
import os
import subprocess  # nosec
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles, NextcloudFile

logger = logging.getLogger(__name__)


REPOSITORY_DIRNAME = ".data"
SNAPSHOT_DIRNAME = "snapshots"


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
        logger.info("Backup done")

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

    def _backup_file(
        self, nc_file: NextcloudFile, dir_config: NextcloudDirectoryConfig
    ):
        local_file = (
            dir_config.backup_root_path
            / SNAPSHOT_DIRNAME
            / self.current_backup_formatted_date
            / dir_config.user_name
        ) / nc_file.path
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

    def _compute_sha1(self, file: Path) -> str:
        # consider stream file as allowed from python 3.11
        return f"SHA1:{hashlib.sha1(file.read_bytes()).hexdigest()}"  # nosec

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
                s3_path.copy(downloading_path)
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
            )
        else:
            logger.warning(
                "Ignoring Nextcloud record DB file not found on s3. "
                "Storage: %s - path %s",
                nc_file.storage,
                s3_path,
            )
            return

    def _find_sha1_from_inode(
        self, dir_config: NextcloudDirectoryConfig, inode: int
    ) -> Path:
        command = [
            "find",
            str(dir_config.backup_root_path / REPOSITORY_DIRNAME / "sha1"),
            "-inum",
            str(inode),
        ]
        logger.debug("Running %s...", command)
        sha1 = None
        try:
            res = subprocess.run(  # nosec
                command, shell=False, check=True, capture_output=True
            )
            sha1 = Path(res.stdout.decode().strip("\n"))
        except Exception as ex:
            logger.warning("Ignore error while getting sha1 on inode %s: %s", inode, ex)
            sha1 = None
        return sha1

    def _backup_file_with_etag(
        self,
        nc_file: NextcloudFile,
        dir_config: NextcloudDirectoryConfig,
        s3_path: Path,
    ) -> Path:
        nc_file.checksum = f"ETAG:{s3_path.stat().etag}"
        etag_repo_file = (
            dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
        )
        repo_file = None
        if not etag_repo_file.exists():
            downloading_path = etag_repo_file.with_suffix(".downloading")
            downloading_path.parent.mkdir(parents=True, exist_ok=True)
            s3_path.copy(downloading_path)
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
                repo_file.parent.mkdir(parents=True, exist_ok=True)
                os.link(etag_repo_file, repo_file)
            else:
                sha1 = "".join(repo_file.parts[:2])
                nc_file.checksum = f"SHA1:{sha1}"

        return repo_file
