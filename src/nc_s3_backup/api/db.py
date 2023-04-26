import logging
from dataclasses import dataclass
from pathlib import PurePath
from typing import List

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_SERIALIZABLE

logger = logging.getLogger(__name__)
SAVEPOINT_NAME = "nc_s3_backup_save_point"


@dataclass
class NextcloudFile:
    """
    used? | Field name        | info                    | data example
    *     |  fileid           | oid, file stored in S3
                                bucket as urn:oid:579   | 579
    *     |  storage          | storage id  |2
    *     |  path             | path as seen by users   | files/test/test.txt
          |  path_hash        | hash of path field      | d2ecf433b...317757e331
          |  parent           | directory fileid in
                                this table              | 549
          |  name             | file name               | f2_fichier_modifie_sur_poste_1
          |  mimetype         | mimetype id             | 9
          |  mimepart         | first part mimetype id
                                text/md => text         | 3
          |  size             |                         | 67
          |  mtime            |                         | 1640342159
          |  storage_mtime    |                         | 1640342164
          |  encrypted        |                         | 0
          |  unencrypted_size |                         | 0
          |  etag             |                         | 61c5a2940ccc7
          |  permissions      |                         | 27
    *     |  checksum         | file hash               | SHA1:00dea...94d34d1623
    """

    fileid: int
    storage: int
    path: str
    checksum: str
    size: int

    @property
    def hash_path(self):
        """return relative path construct from file checksum

        in order to store data as
        hash_method / hash_begining / hash_end
        """
        method, hash_value = self.checksum.lower().split(":", 1)
        return PurePath(method, hash_value[:2], hash_value[2:])


class Dao:
    _cr = None
    _cnx = None
    _ids = 0
    _schema = None
    is_open = False

    @classmethod
    def open_cnx_cursor(cls, pg_url, schema="public"):
        if not Dao._cnx:
            Dao._cnx = psycopg2.connect(pg_url)
            Dao._cr = Dao._cnx.cursor()
            Dao._cnx.set_isolation_level(ISOLATION_LEVEL_SERIALIZABLE)
            Dao._schema = schema
            Dao.set_default_schema(Dao._schema)
        Dao._ids += 1
        cls.is_open = True

    def __init__(self, pg_url, schema="public"):
        self.open_cnx_cursor(pg_url, schema=schema)

    @classmethod
    def set_default_schema(cls, schema):
        Dao._cr.execute("SET search_path TO %s" % schema)

    @classmethod
    def commit(cls):
        Dao._cnx.commit()

    @classmethod
    def add_save_point(cls, name=SAVEPOINT_NAME):
        Dao._cr.execute("SAVEPOINT %s" % name)
        Dao.set_default_schema(cls._schema)

    @classmethod
    def rollback_to_savepoint(cls, name=SAVEPOINT_NAME):
        Dao._cr.execute("ROLLBACK TO SAVEPOINT %s" % name)

    @classmethod
    def rollback(cls):
        Dao._cnx.rollback()
        Dao.set_default_schema(cls._schema)

    @classmethod
    def close(cls):
        if cls.is_open:
            cls.is_open = False
            Dao._ids -= 1
            if Dao._ids == 0:
                Dao._cnx.close()
                Dao._cnx = None

    def __del__(self):
        try:
            self.close()
        except Exception as ex:
            logger.error(
                "Following exception occurred while trying to close connection: %s", ex
            )


class DaoNextcloudFiles(Dao):
    """Method to retrieve Nextcloud database information"""

    def get_nc_subtree(
        self, storage_id: int, root_path: str, excluded_mimetype: List[int]
    ) -> List[NextcloudFile]:
        search_path = root_path + "%"
        # TODO: manage checksum null or empty
        query = """
            SELECT fileid, storage, path, checksum, size
            FROM oc_filecache
            WHERE storage=%(storage_id)s
                AND path ILIKE %(path)s
                AND mimetype NOT IN %(excluded_mimetype)s
        """
        self._cr.execute(
            query,
            dict(
                storage_id=storage_id,
                path=search_path,
                excluded_mimetype=tuple(excluded_mimetype),
            ),
        )
        return [NextcloudFile(*r) for r in self._cr.fetchall()]
