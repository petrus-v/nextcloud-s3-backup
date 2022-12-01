from dataclasses import field
from pathlib import Path
from typing import List

from pydantic.dataclasses import dataclass


@dataclass
class NextcloudDirectoryConfig:

    storage_id: int = None
    user_name: str = None
    bucket: Path = None
    nextcloud_path: str = None
    backup_root_path: Path = None


@dataclass
class NextCloudS3BackupConfig:
    """Config file contains tree mapping to backup"""

    backup_date_format: str = "%y%m%d-%H%M"
    excluded_mimetype_ids: List[int] = field(default_factory=list)
    mapping: List[NextcloudDirectoryConfig] = field(default_factory=list)
