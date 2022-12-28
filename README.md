# Nextcloud S3 Backup

<p align="center">
  <a href="https://github.com/petrus-v/nextcloud-s3-backup/actions?query=workflow%3ACI">
    <img src="https://img.shields.io/github/workflow/status/petrus-v/nextcloud-s3-backup/CI/main?label=CI&logo=github&style=flat-square" alt="CI Status" >
  </a>
  <a href="https://nextcloud-s3-backup.readthedocs.io">
    <img src="https://img.shields.io/readthedocs/nextcloud-s3-backup.svg?logo=read-the-docs&logoColor=fff&style=flat-square" alt="Documentation Status">
  </a>
  <a href="https://codecov.io/gh/petrus-v/nextcloud-s3-backup">
    <img src="https://img.shields.io/codecov/c/github/petrus-v/nextcloud-s3-backup.svg?logo=codecov&logoColor=fff&style=flat-square" alt="Test coverage percentage">
  </a>
</p>
<p align="center">
  <a href="https://python-poetry.org/">
    <img src="https://img.shields.io/badge/packaging-poetry-299bd7?style=flat-square&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAASCAYAAABrXO8xAAAACXBIWXMAAAsTAAALEwEAmpwYAAAAAXNSR0IArs4c6QAAAARnQU1BAACxjwv8YQUAAAJJSURBVHgBfZLPa1NBEMe/s7tNXoxW1KJQKaUHkXhQvHgW6UHQQ09CBS/6V3hKc/AP8CqCrUcpmop3Cx48eDB4yEECjVQrlZb80CRN8t6OM/teagVxYZi38+Yz853dJbzoMV3MM8cJUcLMSUKIE8AzQ2PieZzFxEJOHMOgMQQ+dUgSAckNXhapU/NMhDSWLs1B24A8sO1xrN4NECkcAC9ASkiIJc6k5TRiUDPhnyMMdhKc+Zx19l6SgyeW76BEONY9exVQMzKExGKwwPsCzza7KGSSWRWEQhyEaDXp6ZHEr416ygbiKYOd7TEWvvcQIeusHYMJGhTwF9y7sGnSwaWyFAiyoxzqW0PM/RjghPxF2pWReAowTEXnDh0xgcLs8l2YQmOrj3N7ByiqEoH0cARs4u78WgAVkoEDIDoOi3AkcLOHU60RIg5wC4ZuTC7FaHKQm8Hq1fQuSOBvX/sodmNJSB5geaF5CPIkUeecdMxieoRO5jz9bheL6/tXjrwCyX/UYBUcjCaWHljx1xiX6z9xEjkYAzbGVnB8pvLmyXm9ep+W8CmsSHQQY77Zx1zboxAV0w7ybMhQmfqdmmw3nEp1I0Z+FGO6M8LZdoyZnuzzBdjISicKRnpxzI9fPb+0oYXsNdyi+d3h9bm9MWYHFtPeIZfLwzmFDKy1ai3p+PDls1Llz4yyFpferxjnyjJDSEy9CaCx5m2cJPerq6Xm34eTrZt3PqxYO1XOwDYZrFlH1fWnpU38Y9HRze3lj0vOujZcXKuuXm3jP+s3KbZVra7y2EAAAAAASUVORK5CYII=" alt="Poetry">
  </a>
  <a href="https://github.com/ambv/black">
    <img src="https://img.shields.io/badge/code%20style-black-000000.svg?style=flat-square" alt="black">
  </a>
  <a href="https://github.com/pre-commit/pre-commit">
    <img src="https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white&style=flat-square" alt="pre-commit">
  </a>
</p>
<p align="center">
  <a href="https://pypi.org/project/nextcloud-s3-backup/">
    <img src="https://img.shields.io/pypi/v/nextcloud-s3-backup.svg?logo=python&logoColor=fff&style=flat-square" alt="PyPI Version">
  </a>
  <img src="https://img.shields.io/pypi/pyversions/nextcloud-s3-backup.svg?style=flat-square&logo=python&amp;logoColor=fff" alt="Supported Python versions">
  <img src="https://img.shields.io/pypi/l/nextcloud-s3-backup.svg?style=flat-square" alt="License">
</p>

Nextcloud S3 primary storage incremental backup.

This command line utility backup tool is designed to create incremental backup
from Nextcloud S3 primary storage.

When using Nextcloud S3 primary storage, files are stored with oid as reference likes `urn:oid:579`.

Backuping those files are not easy to restore particular files.

This tool is designed to:

- re-create file tree
- snapshot like
- multiple storage tree (to manage access write)

This tool is not designed for

- quick restaure the whole nextcloud

## Installation

Install this via pip (or your favourite package manager):

`pip install nextcloud-s3-backup`

## How it works

**Data structure**:

Once the script has been executed you will get the following tree:

```bash
.
├── BACKUP-NC
│   ├── .data                             # Directory hash file, here files are saved by hash (sha1 and/or md5)
│   │   ├── sha1                          # Source of true here files are stored by sha1 computed locally to ensure integrity
│   │   │   ├── fe                        # 2 first sha1 character to limit the number of files per directory
│   │   │   │   ├── e41dea13f...          # files are saved with there SHA1 and each day an hard link point on it
│   │   │   │   ├── e3fc696fe...          # this file is duplicated in the tree but saved only once here
│   │   │   │   └── e3f6d2149...          # this files is not use anymore by any snapshot and can be "garbage collected"
│   │   │   ├── ...
│   │   │   └── ff
│   │   ├── etag                          # Files created/copied/updated/moved through nextcloud
                                          # haven't any SHA1 nor valid md5 here we save etag hardlink to sha1 files
                                          # as we can retrieve etag from s3 without download files
                                          # file that have sha1 filled do not necessaraly have md5/etag
│   │   │   ├── aa                        # 2 first etag character to limit the number of files per directory.
│   │   │   │   ├── wxyz...abc-3          # file name is etag from remote and is an hardlink to sha1
│   │   │   │   ├── ...
│   │   │   │   └── abce...efg-2          # files not use anymore by any snapshot and can be "garbage collected"
│   │   │   ├── ...
│   │   │   └── ff
│   ├── snapshots                         # Snapshot directory
│   │   ├── 2022-11-17                    # snapshot date (can be configured from config file)
│   │   │   ├── user-nc-1                 # A string configured in mapping file (can be different from storage user)
│   │   │   │   ├── REP 1                 # Non empty directory
│   │   │   │   │   └── file2.ia          # hard link to fee41dea13f file
│   │   │   │   ├── file1.ia              # hard link to fee3fc696fe file
│   │   │   │   └──
│   │   │   ├── ...
│   │   │   └── user-nc-1
│   │   │       └── file1.copy.ia         # hard link to fee3fc696fe file (file is named differently but content is the same as file1.ia)
│   │   └── 2021-11-18
│   │   │   ├── user-nc-1                 # A string configured in mapping file (can be different from storage user)
│   │   │   │   ├── REP 1                 # Non empty directory
│   │   │   │   │   └── file2.ia          # hard link to fee41dea13f file
│   │   │   │   ├── file1.ia              # hard link to fee3fc696fe file
│   │   │   │   └──
│   │   │   ├── ...
│   │   │   └── user-nc-1
│   │   │       └── file1.copy.ia         # hard link to fee3fc696fe file (file is named differently but content is the same as file1.ia)
```

**Requirements**:

- First script needs to get access to `[oc_]filecache` table or a backup of it.
- User should provide a config file to tell what to backup where
- file system that allow hard link.

**Processus**:

- for each mapping defined in config file
  - for each files matching path and mimetype restrictions
    - if SHA1 present in filecache (file send with nextcloud client)
      - if SHA1 NOT present locally
        - retrieve file from S3 bucket in a temporary file (saved in expected sha1 from filecache table information)
        - compute SHA1 locally
        - rename file to the local sha1 computation
    - if SHA1 not defined (file created, updated, moved or copy from web interface)
      - retrieve etag information from s3 file
        - if remote etag not present locally
          - retrieve file from S3 bucket in a temporary file (saved in expected etag from s3 bucket)
          - compute SHA1 locally
          - if local sha1 present
            - remove downloaded file
          - elif local sha1 not present
            - rename file to the local sha1 computation
          - create hard link with etag to sha1 file
        - else get referred sha1
    - create hard link in the current snapshot directory to the sha1 file

> **Note**: ETag are not md5, in case of multipart upload etag is the md5 of all
> md5 parts + "-" + number of upload parts. Here we laverage the risk of integrity data
> by moving file after download is completed. This avoid to make a différences between
> multi part uploads files and one part upload (where we could check the md5 integrity
> but we won't it would be the donwlonding boto3 role).

## Usage

## Contributors ✨

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- prettier-ignore-start -->
<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tbody>
    <tr>
      <td align="center" valign="top" width="14.28%"><a href="http://pierre.verkest.fr/"><img src="https://avatars.githubusercontent.com/u/4328507?v=4?s=80" width="80px;" alt="Pierre Verkest"/><br /><sub><b>Pierre Verkest</b></sub></a><br /><a href="https://github.com/petrus-v/nextcloud-s3-backup/commits?author=petrus-v" title="Code">💻</a> <a href="#ideas-petrus-v" title="Ideas, Planning, & Feedback">🤔</a> <a href="https://github.com/petrus-v/nextcloud-s3-backup/commits?author=petrus-v" title="Documentation">📖</a></td>
    </tr>
  </tbody>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->
<!-- prettier-ignore-end -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!

## Credits

This package was created with
[Copier](https://copier.readthedocs.io/) and the
[browniebroke/pypackage-template](https://github.com/browniebroke/pypackage-template)
project template.
