import argparse
import json
import logging
import sys
import threading
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.client import Config as BotoConfig
from pydantic.json import pydantic_encoder
from s3path import PureS3Path, register_configuration_parameter
from uri_pathlib_factory import load_pathlib_monkey_patch
from yaml import safe_dump, safe_load

from nc_s3_backup.api.backup import (
    REPOSITORY_DIRNAME,
    SNAPSHOT_DIRNAME,
    NextcloudS3Backup,
)
from nc_s3_backup.api.config import NextcloudDirectoryConfig, NextCloudS3BackupConfig
from nc_s3_backup.api.db import DaoNextcloudFiles

logger = logging.getLogger(__name__)

load_pathlib_monkey_patch()


def logging_params(parser):
    logging_group = parser.add_argument_group("Logging params")
    logging_group.add_argument(
        "-f",
        "--logging-file",
        type=argparse.FileType("r"),
        help="Logging configuration file, (logging-level and logging-format "
        "are ignored if provide)",
    )
    logging_group.add_argument("-l", "--logging-level", default="INFO")
    logging_group.add_argument(
        "--logging-format",
        default="%(asctime)s - %(levelname)s (%(module)s%(funcName)s): " "%(message)s",
    )


def pg_params(parser):
    gp = parser.add_argument_group("Postgresql connection")
    gp.add_argument(
        "--pg-dsn",
        help="Postgresql connection string",
        default="postgresql:///nc-backup?application_name=%s" % parser.prog,
    )
    gp.add_argument("--pg-schema", help="Postgresql default schema", default="public")


def s3_params(parser):
    group = parser.add_argument_group("S3 configuration")

    group.add_argument(
        "--s3-endpoint-url",
        dest="s3_endpoint_url",
        default="https://s3.fr-par.scw.cloud",
        type=str,
        help="S3 endpoint url",
    )
    group.add_argument(
        "--s3-region-name",
        dest="s3_region_name",
        default="fr-par",
        type=str,
        help="S3 region name.",
    )
    group.add_argument(
        "--s3-access-key", dest="s3_access_key_id", type=str, help="S3 access key ID."
    )
    group.add_argument(
        "--s3-secret-key", dest="s3_secret_access_key", type=str, help="S3 secret key."
    )

    group_s3_transfer = parser.add_argument_group("S3 Transfer configuration")

    group_s3_transfer.add_argument(
        "--s3-multipart-threshold",
        dest="s3_multipart_threshold_mb",
        default=120,
        type=int,
        help=(
            "The transfer size threshold for which "
            "multipart uploads, downloads, and copies will automatically be "
            "triggered.. (MB)"
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-multipart-chunksize",
        dest="s3_multipart_chunksize_mb",
        default=100,
        type=int,
        help=("The partition size of each part for a multipart transfer. (MB)"),
    )
    group_s3_transfer.add_argument(
        "--s3-no-threads",
        dest="s3_no_threads",
        action="store_true",
        help=(
            "If False, threads will be used when performing "
            "S3 transfers. If True, no threads will be used in "
            "performing transfers; all logic will be run in the main thread."
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-max-bandwidth",
        dest="s3_max_bandwidth_mb",
        type=int,
        help=(
            "The maximum bandwidth that will be consumed "
            "in uploading and downloading file content. The value is an integer "
            "in terms of Mega bytes per second. (MB)"
        ),
    )

    group_s3_transfer.add_argument(
        "--s3-max-concurrency",
        dest="s3_max_concurrency",
        default=10,
        type=int,
        help=(
            "The maximum number of threads that will be "
            "making requests to perform a transfer. If ``use_threads`` is "
            "set to ``False``, the value provided is ignored as the transfer "
            "will only ever use the main thread. (10)"
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-num-download-attempts",
        dest="s3_num_download_attempts",
        default=5,
        type=int,
        help=(
            "The number of download attempts that "
            "will be retried upon errors with downloading an object in S3. "
            "Note that these retries account for errors that occur when "
            "streaming  down the data from s3 (i.e. socket errors and read "
            "timeouts that occur after receiving an OK response from s3). "
            "Other retryable exceptions such as throttling errors and 5xx "
            "errors are already retried by botocore (this default is 5). This "
            "does not take into account the number of exceptions retried by "
            "botocore."
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-max-io-queue",
        dest="s3_max_io_queue_mb",
        default=100,
        type=int,
        help=(
            "The maximum amount of read parts that can be "
            "queued in memory to be written for a download. The size of each "
            "of these read parts is at most the size of ``io_chunksize``. (MB)"
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-io-chunksize",
        dest="s3_io_chunksize_mb",
        default=1,
        type=int,
        help=(
            "The max size of each chunk in the io queue. "
            "Currently, this is size used when ``read`` is called on the "
            "downloaded stream as well. (MB)"
        ),
    )
    group_s3_transfer.add_argument(
        "--s3-progress",
        dest="s3_progress",
        action="store_true",
        help=(
            "Display upload/downloads progress in stdout. To be use with "
            "--mt-thread-size=1 not properly working otherwise"
        ),
    )


def parse_setup_s3(arguments):
    default_aws_s3_path = PureS3Path("/")
    params = {}
    if arguments.s3_endpoint_url:
        params["endpoint_url"] = arguments.s3_endpoint_url
    if arguments.s3_region_name:
        params["region_name"] = arguments.s3_region_name
    if arguments.s3_access_key_id:
        params["aws_access_key_id"] = arguments.s3_access_key_id
    if arguments.s3_secret_access_key:
        params["aws_secret_access_key"] = arguments.s3_secret_access_key

    params["config"] = BotoConfig(signature_version="s3v4")

    KB = 1024
    MB = KB**2
    GB = KB**3

    transfert_config_params = dict(
        use_threads=not arguments.s3_no_threads,
        multipart_threshold=arguments.s3_multipart_threshold_mb * MB,
        multipart_chunksize=arguments.s3_multipart_chunksize_mb * MB,
        max_bandwidth=arguments.s3_max_bandwidth_mb * MB
        if arguments.s3_max_bandwidth_mb
        else None,
        max_concurrency=arguments.s3_max_concurrency,
        num_download_attempts=arguments.s3_num_download_attempts,
        max_io_queue=arguments.s3_max_io_queue_mb * MB,
        io_chunksize=arguments.s3_io_chunksize_mb * MB,
    )

    class ProgressPercentage:
        """This is not working in multiprocessing
        to be use with --mt-thread-size=1
        """

        def __init__(self, filename):
            self._filename = filename
            self._size = filename.stat().st_size
            self._seen_so_far = 0
            self._lock = threading.Lock()

        def __call__(self, bytes_amount):
            # To simplify we'll assume this is hooked up
            # to a single filename.
            with self._lock:
                self._seen_so_far += bytes_amount
                percentage = (self._seen_so_far / self._size) * 100
                sys.stdout.write(
                    "\r%s  %.3f GB / %.3f GB  (%.2f%%)"
                    % (
                        self._filename,
                        self._seen_so_far / GB,
                        self._size / GB,
                        percentage,
                    )
                )
                sys.stdout.flush()

    if params:
        register_configuration_parameter(
            default_aws_s3_path,
            resource=boto3.resource("s3", **params),
            parameters={
                "StorageClass": "GLACIER",
                "transfert_config": TransferConfig(**transfert_config_params),
                "callback_class": ProgressPercentage if arguments.s3_progress else None,
            },
        )


def parse_config(config_file):
    """Content is NextCloudS3Config to be deserialized by pydantic
    as far we are using yaml parser, you can define as json or yaml
    format
    """
    return NextCloudS3BackupConfig(**safe_load(config_file.read()))


def main(testing: bool = False):
    parser = argparse.ArgumentParser(
        description="Nextcloud S3 backup",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "config",
        type=argparse.FileType("r"),
        help=(
            "Nextcloud S3 backup config file is a json/yaml file that "
            "contains mapping of directories to backup."
        ),
    )
    logging_params(parser)
    s3_params(parser)
    pg_params(parser)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, arguments.logging_level.upper()),
        format=arguments.logging_format,
    )
    if arguments.logging_file:
        try:
            json_config = json.loads(arguments.logging_file.read())
            logging.config.dictConfig(json_config)
        except json.JSONDecodeError:
            logging.config.fileConfig(arguments.logging_file.name)

    parse_setup_s3(arguments)
    config = parse_config(arguments.config)
    arguments.config.close()
    dao = DaoNextcloudFiles(arguments.pg_dsn, schema=arguments.pg_schema)
    nextcloud_s3_backup = NextcloudS3Backup(dao, config)
    nextcloud_s3_backup.backup()
    if testing:
        return nextcloud_s3_backup


def purge(testing: bool = False):
    parser = argparse.ArgumentParser(
        description=(
            "Nextcloud S3 backup purge\n\n"
            "Use with caution! \n"
            "This script loop over uniques backup_root_path present "
            "in your config file to remove files present in "
            f"the {REPOSITORY_DIRNAME} that are not present in "
            f"the {SNAPSHOT_DIRNAME} dicectory"
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "config",
        type=argparse.FileType("r"),
        help=(
            "Nextcloud S3 backup config file is a json/yaml file that "
            "contains mapping of directories to backup."
        ),
    )
    logging_params(parser)
    arguments = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, arguments.logging_level.upper()),
        format=arguments.logging_format,
    )
    if arguments.logging_file:
        try:
            json_config = json.loads(arguments.logging_file.read())
            logging.config.dictConfig(json_config)
        except json.JSONDecodeError:
            logging.config.fileConfig(arguments.logging_file.name)

    config = parse_config(arguments.config)
    arguments.config.close()
    nextcloud_s3_backup = NextcloudS3Backup(None, config)
    nextcloud_s3_backup.purge()
    if testing:
        return nextcloud_s3_backup


def config_helper():
    parser = argparse.ArgumentParser(
        description="Helper to validate/convert NextCloudS3Config"
        "and add entry. Do not change current files, display result in output.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    group = parser.add_argument_group("Optional add mapping")

    group.add_argument(
        "--storage-id",
        dest="storage_id",
        type=int,
        help="Nextcloud storage id (`storage`), used to query oc_filecache table",
    )
    group.add_argument(
        "--user-name",
        dest="user_name",
        type=str,
        help=(
            "User name used to create directory root tree "
            "and make config file easier to read. "
            "This value can differ from nextcloud database information"
        ),
    )
    group.add_argument(
        "--bucket",
        dest="bucket",
        type=Path,
        help=(
            "S3 bucket uri. the root directory where are stored Nextcloud user "
            "storage. ie: s3://com.example.nextcloud/"
        ),
    )
    group.add_argument(
        "--nextcloud-path",
        dest="nextcloud_path",
        # default="/",
        type=str,
        help=("Root path to archive for given storage user as seen in nextcloud"),
    )
    group.add_argument(
        "--backup-root-path",
        dest="backup_root_path",
        # default="./",
        type=Path,
        help=("Root path where to backup files"),
    )
    parser.add_argument(
        "parser",
        type=str,
        choices=["json", "yaml"],
        help="choose output format",
    )
    parser.add_argument(
        "config",
        type=argparse.FileType("r"),
        help=(
            "Nextcloud S3 backup config file is a json/yaml file that "
            "contains mapping of directories to backup."
        ),
    )

    arguments = parser.parse_args()
    config = parse_config(arguments.config)
    arguments.config.close()
    if any(
        [
            arguments.storage_id,
            arguments.user_name,
            arguments.bucket,
            arguments.nextcloud_path,
            arguments.backup_root_path,
        ]
    ):
        config.mapping.append(
            NextcloudDirectoryConfig(
                storage_id=arguments.storage_id,
                user_name=arguments.user_name,
                bucket=arguments.bucket,
                nextcloud_path=arguments.nextcloud_path,
                backup_root_path=arguments.backup_root_path,
            )
        )
    json_data = json.dumps(config, indent=2, default=pydantic_encoder)
    print("# USE WITH CAUTION")
    print(
        "# There is an issue serialised s3path should "
        "return path.as_uri() instead str(path)"
    )
    print("# you should probably add s3:// again on bucket path!")
    if arguments.parser == "yaml":
        print(safe_dump(safe_load(json_data), indent=2))
    else:
        print(json_data)
