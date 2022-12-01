import logging
import os
from dataclasses import dataclass
from datetime import datetime

from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles, NextcloudFile

logger = logging.getLogger(__name__)


REPOSITORY_DIRNAME = ".data"


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
        # TODO: do not assume SH1 is always set
        repo_file = dir_config.backup_root_path / REPOSITORY_DIRNAME / nc_file.hash_path
        local_file = (
            dir_config.backup_root_path
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
        if not repo_file.exists():
            if s3_path.exists():
                repo_file.parent.mkdir(parents=True, exist_ok=True)
                s3_path.copy(repo_file)
            else:
                logger.warning(
                    "Ignoring Nextcloud record DB file not found on s3. "
                    "Storage: %s - path %s",
                    nc_file.storage,
                    s3_path,
                )
                return

        # from python 3.10 only
        # local_file.hardlink_to(repo_file)
        local_file.parent.mkdir(parents=True, exist_ok=True)
        os.link(repo_file, local_file)
        return local_file
