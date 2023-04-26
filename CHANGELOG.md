# Changelog

## v0.2.1 (2023-04-26)

* improving perf while backup file without sha1

  for existing etag we ensure an hardlink sha1 with
  the same inode is present, this search was perform
  using `find -inum` this is done using python.
 
## v0.2.0 (2023-04-26)

* add **nextcloud-s3-backup-purge** command to free space under repo
  directory `.data/`
 
## v0.1.0 (2023-04-05)

* Bump pre-commit hooks versions
* Use mean to compute stats and add median
* Assume we could get inconsistency data between etag and sha1 ./data
  directory to manage wrongly synchronised directory without proper
  hard links.
