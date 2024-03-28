######################################################################
#
# File: b2sdk/_internal/account_info/sqlite_account_info.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import stat
import sys
import threading

from .exception import CorruptAccountInfo, MissingAccountData
from .upload_url_pool import UrlPoolAccountInfo

logger = logging.getLogger(__name__)

B2_ACCOUNT_INFO_ENV_VAR = 'B2_ACCOUNT_INFO'
B2_ACCOUNT_INFO_DEFAULT_FILE = os.path.join('~', '.b2_account_info')
B2_ACCOUNT_INFO_PROFILE_FILE = os.path.join('~', '.b2db-{profile}.sqlite')
B2_ACCOUNT_INFO_PROFILE_NAME_REGEXP = re.compile(r'[a-zA-Z0-9_\-]{1,64}')
XDG_CONFIG_HOME_ENV_VAR = 'XDG_CONFIG_HOME'

DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE = 5000000  # this value is used ONLY in migrating db, and in v1 wrapper, it is not
# meant to be a default for other applications


class SqliteAccountInfo(UrlPoolAccountInfo):
    """
    Store account information in an `sqlite3 <https://www.sqlite.org>`_ database which is
    used to manage concurrent access to the data.

    The ``update_done`` table tracks the schema updates that have been
    completed.
    """

    def __init__(self, file_name=None, last_upgrade_to_run=None, profile: str | None = None):
        """
        Initialize SqliteAccountInfo.

        The exact algorithm used to determine the location of the database file is not API in any sense.
        If the location of the database file is required (for cleanup, etc), do not assume a specific resolution:
        instead, use ``self.filename`` to get the actual resolved location.

        SqliteAccountInfo currently checks locations in the following order:

        If ``profile`` arg is provided:

        * ``{B2_ACCOUNT_INFO_PROFILE_FILE}`` if it already exists
        * ``${XDG_CONFIG_HOME_ENV_VAR}/b2/db-<profile>.sqlite`` on XDG-compatible OSes (Linux, BSD)
        * ``{B2_ACCOUNT_INFO_PROFILE_FILE}``

        Otherwise:

        * ``file_name``, if truthy
        * ``{B2_ACCOUNT_INFO_ENV_VAR}`` env var's value, if set
        * ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``, if it already exists
        * ``${XDG_CONFIG_HOME_ENV_VAR}/b2/account_info`` on XDG-compatible OSes (Linux, BSD)
        * ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``, as default

        If the directory ``${XDG_CONFIG_HOME_ENV_VAR}/b2`` does not exist (and is needed), it is created.

        :param str file_name: The sqlite file to use; overrides the default.
        :param int last_upgrade_to_run: For testing only, override the auto-update on the db.
        """
        self.thread_local = threading.local()

        self.filename = self.get_user_account_info_path(file_name=file_name, profile=profile)
        logger.debug('%s file path to use: %s', self.__class__.__name__, self.filename)

        self._validate_database(last_upgrade_to_run)
        with self._get_connection() as conn:
            self._create_tables(conn, last_upgrade_to_run)
        super().__init__()

    # dirty trick to use parameters in the docstring
    if getattr(__init__, '__doc__', None):  # don't break when using `python -oo`
        __init__.__doc__ = __init__.__doc__.format(
            **dict(
                B2_ACCOUNT_INFO_ENV_VAR=B2_ACCOUNT_INFO_ENV_VAR,
                B2_ACCOUNT_INFO_DEFAULT_FILE=B2_ACCOUNT_INFO_DEFAULT_FILE,
                B2_ACCOUNT_INFO_PROFILE_FILE=B2_ACCOUNT_INFO_PROFILE_FILE,
                XDG_CONFIG_HOME_ENV_VAR=XDG_CONFIG_HOME_ENV_VAR,
            )
        )

    @classmethod
    def _get_xdg_config_path(cls) -> str | None:
        """
        Return XDG config path if the OS is XDG-compatible (Linux, BSD), None otherwise.

        If $XDG_CONFIG_HOME is empty but the OS is XDG compliant, fallback to ~/.config as expected by XDG standard.
        """
        xdg_config_home = os.getenv(XDG_CONFIG_HOME_ENV_VAR)
        if xdg_config_home or sys.platform not in ('win32', 'darwin'):
            return xdg_config_home or os.path.join(os.path.expanduser('~/.config'))
        return None

    @classmethod
    def get_user_account_info_path(cls, file_name: str | None = None, profile: str | None = None):
        if profile and not B2_ACCOUNT_INFO_PROFILE_NAME_REGEXP.match(profile):
            raise ValueError(f'Invalid profile name: {profile}')

        profile_file = B2_ACCOUNT_INFO_PROFILE_FILE.format(profile=profile) if profile else None
        xdg_config_path = cls._get_xdg_config_path()

        if file_name:
            if profile:
                raise ValueError('Provide either file_name or profile, not both')
            user_account_info_path = file_name
        elif B2_ACCOUNT_INFO_ENV_VAR in os.environ:
            if profile:
                raise ValueError(
                    f'Provide either {B2_ACCOUNT_INFO_ENV_VAR} env var or profile, not both'
                )
            user_account_info_path = os.environ[B2_ACCOUNT_INFO_ENV_VAR]
        elif not profile and os.path.exists(os.path.expanduser(B2_ACCOUNT_INFO_DEFAULT_FILE)):
            user_account_info_path = B2_ACCOUNT_INFO_DEFAULT_FILE
        elif profile and os.path.exists(profile_file):
            user_account_info_path = profile_file
        elif xdg_config_path:
            os.makedirs(os.path.join(xdg_config_path, 'b2'), mode=0o755, exist_ok=True)

            file_name = f'db-{profile}.sqlite' if profile else 'account_info'
            user_account_info_path = os.path.join(xdg_config_path, 'b2', file_name)
        elif profile:
            user_account_info_path = profile_file
        else:
            user_account_info_path = B2_ACCOUNT_INFO_DEFAULT_FILE

        return os.path.expanduser(user_account_info_path)

    def _validate_database(self, last_upgrade_to_run=None):
        """
        Make sure that the database is openable.  Removes the file if it's not.
        """
        # If there is no file there, that's fine.  It will get created when
        # we connect.
        if not os.path.exists(self.filename):
            self._create_database(last_upgrade_to_run)
            return

        # If we can connect to the database, and do anything, then all is good.
        try:
            with self._connect() as conn:
                self._create_tables(conn, last_upgrade_to_run)
                return
        except sqlite3.DatabaseError:
            pass  # fall through to next case

        # If the file contains JSON with the right stuff in it, convert from
        # the old representation.
        try:
            with open(self.filename, 'rb') as f:
                data = json.loads(f.read().decode('utf-8'))
                keys = [
                    'account_id', 'application_key', 'account_auth_token', 'api_url',
                    'download_url', 'minimum_part_size', 'realm'
                ]
            if all(k in data for k in keys):
                # remove the json file
                os.unlink(self.filename)
                # create a database
                self._create_database(last_upgrade_to_run)
                # add the data from the JSON file
                with self._connect() as conn:
                    self._create_tables(conn, last_upgrade_to_run)
                    insert_statement = """
                        INSERT INTO account
                        (account_id, application_key, account_auth_token, api_url, download_url, 
                         recommended_part_size, realm, absolute_minimum_part_size)
                        values (?, ?, ?, ?, ?, ?, ?, ?);
                    """
                    # Migrating from old schema is a little confusing, but the values change as:
                    # minimum_part_size -> recommended_part_size
                    # new column absolute_minimum_part_size = DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE
                    conn.execute(
                        insert_statement,
                        (*(data[k] for k in keys), DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE)
                    )
                # all is happy now
                return
        except ValueError:  # includes json.decoder.JSONDecodeError
            pass

        # Remove the corrupted file and create a new database
        raise CorruptAccountInfo(self.filename)

    def _get_connection(self):
        """
        Connections to sqlite cannot be shared across threads.
        """
        try:
            return self.thread_local.connection
        except AttributeError:
            self.thread_local.connection = self._connect()
            return self.thread_local.connection

    def _connect(self):
        return sqlite3.connect(self.filename, isolation_level='EXCLUSIVE')

    def _create_database(self, last_upgrade_to_run):
        """
        Make sure that the database is created and has appropriate file permissions.
        This should be done before storing any sensitive data in it.
        """
        # Prepare a file
        fd = os.open(
            self.filename,
            flags=os.O_RDWR | os.O_CREAT,
            mode=stat.S_IRUSR | stat.S_IWUSR,
        )
        os.close(fd)
        # Create the tables in the database
        conn = self._connect()
        try:
            with conn:
                self._create_tables(conn, last_upgrade_to_run)
        finally:
            conn.close()

    def _create_tables(self, conn, last_upgrade_to_run):
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS
            update_done (
                update_number INT NOT NULL
            );
        """
        )
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           account (
               account_id TEXT NOT NULL,
               application_key TEXT NOT NULL,
               account_auth_token TEXT NOT NULL,
               api_url TEXT NOT NULL,
               download_url TEXT NOT NULL,
               minimum_part_size INT NOT NULL,
               realm TEXT NOT NULL
           );
        """
        )
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           bucket (
               bucket_name TEXT NOT NULL,
               bucket_id TEXT NOT NULL
           );
        """
        )
        # This table is not used any more.  We may use it again
        # someday if we save upload URLs across invocations of
        # the command-line tool.
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           bucket_upload_url (
               bucket_id TEXT NOT NULL,
               upload_url TEXT NOT NULL,
               upload_auth_token TEXT NOT NULL
           );
        """
        )
        # By default, we run all the upgrades
        last_upgrade_to_run = 4 if last_upgrade_to_run is None else last_upgrade_to_run
        # Add the 'allowed' column if it hasn't been yet.
        if 1 <= last_upgrade_to_run:
            self._ensure_update(1, ['ALTER TABLE account ADD COLUMN allowed TEXT;'])
        # Add the 'account_id_or_app_key_id' column if it hasn't been yet
        if 2 <= last_upgrade_to_run:
            self._ensure_update(
                2, ['ALTER TABLE account ADD COLUMN account_id_or_app_key_id TEXT;']
            )
        # Add the 's3_api_url' column if it hasn't been yet
        if 3 <= last_upgrade_to_run:
            self._ensure_update(3, ['ALTER TABLE account ADD COLUMN s3_api_url TEXT;'])
        if 4 <= last_upgrade_to_run:
            self._ensure_update(
                4, [
                    """
                    CREATE TABLE
                    tmp_account (
                        account_id TEXT NOT NULL,
                        application_key TEXT NOT NULL,
                        account_auth_token TEXT NOT NULL,
                        api_url TEXT NOT NULL,
                        download_url TEXT NOT NULL,
                        absolute_minimum_part_size INT NOT NULL DEFAULT {},
                        recommended_part_size INT NOT NULL,
                        realm TEXT NOT NULL,
                        allowed TEXT,
                        account_id_or_app_key_id TEXT,
                        s3_api_url TEXT    
                    );
                    """.format(DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE),
                    """INSERT INTO tmp_account(
                        account_id,
                        application_key,
                        account_auth_token,
                        api_url,
                        download_url,
                        recommended_part_size,
                        realm,
                        allowed,
                        account_id_or_app_key_id,
                        s3_api_url
                    ) 
                    SELECT
                        account_id,
                        application_key,
                        account_auth_token,
                        api_url,
                        download_url,
                        minimum_part_size,
                        realm,
                        allowed,
                        account_id_or_app_key_id,
                        s3_api_url
                    FROM account;
                    """,
                    'DROP TABLE account;',
                    """
                    CREATE TABLE account (
                        account_id TEXT NOT NULL,
                        application_key TEXT NOT NULL,
                        account_auth_token TEXT NOT NULL,
                        api_url TEXT NOT NULL,
                        download_url TEXT NOT NULL,
                        absolute_minimum_part_size INT NOT NULL,
                        recommended_part_size INT NOT NULL,
                        realm TEXT NOT NULL,
                        allowed TEXT,
                        account_id_or_app_key_id TEXT,
                        s3_api_url TEXT    
                    );
                    """,
                    """INSERT INTO account(
                                    account_id,
                                    application_key,
                                    account_auth_token,
                                    api_url,
                                    download_url,
                                    absolute_minimum_part_size,
                                    recommended_part_size,
                                    realm,
                                    allowed,
                                    account_id_or_app_key_id,
                                    s3_api_url
                                ) 
                                SELECT
                                    account_id,
                                    application_key,
                                    account_auth_token,
                                    api_url,
                                    download_url,
                                    absolute_minimum_part_size,
                                    recommended_part_size,
                                    realm,
                                    allowed,
                                    account_id_or_app_key_id,
                                    s3_api_url
                                FROM tmp_account;
                                """,
                    'DROP TABLE tmp_account;',
                ]
            )

    def _ensure_update(self, update_number, update_commands: list[str]):
        """
        Run the update with the given number if it hasn't been done yet.

        Does the update and stores the number as a single transaction,
        so they will always be in sync.
        """
        with self._get_connection() as conn:
            conn.execute('BEGIN')
            cursor = conn.execute(
                'SELECT COUNT(*) AS count FROM update_done WHERE update_number = ?;',
                (update_number,)
            )
            update_count = cursor.fetchone()[0]
            if update_count == 0:
                for command in update_commands:
                    conn.execute(command)
                conn.execute(
                    'INSERT INTO update_done (update_number) VALUES (?);', (update_number,)
                )

    def clear(self):
        """
        Remove all info about accounts and buckets.
        """
        with self._get_connection() as conn:
            conn.execute('DELETE FROM account;')
            conn.execute('DELETE FROM bucket;')
            conn.execute('DELETE FROM bucket_upload_url;')

    def _set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        recommended_part_size,
        absolute_minimum_part_size,
        application_key,
        realm,
        s3_api_url,
        allowed,
        application_key_id,
    ):
        assert self.allowed_is_valid(allowed)
        with self._get_connection() as conn:
            conn.execute('DELETE FROM account;')
            conn.execute('DELETE FROM bucket;')
            conn.execute('DELETE FROM bucket_upload_url;')
            insert_statement = """
                INSERT INTO account
                (account_id, account_id_or_app_key_id, application_key, account_auth_token, api_url, download_url, 
                 recommended_part_size, absolute_minimum_part_size, realm, allowed, s3_api_url)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """

            conn.execute(
                insert_statement, (
                    account_id,
                    application_key_id,
                    application_key,
                    auth_token,
                    api_url,
                    download_url,
                    recommended_part_size,
                    absolute_minimum_part_size,
                    realm,
                    json.dumps(allowed),
                    s3_api_url,
                )
            )

    def set_auth_data_with_schema_0_for_test(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
    ):
        """
        Set authentication data for tests.

        :param str account_id: an account ID
        :param str auth_token: an authentication token
        :param str api_url: an API URL
        :param str download_url: a download URL
        :param int minimum_part_size: a minimum part size
        :param str application_key: an application key
        :param str realm: a realm to authorize account in
        """
        with self._get_connection() as conn:
            conn.execute('DELETE FROM account;')
            conn.execute('DELETE FROM bucket;')
            conn.execute('DELETE FROM bucket_upload_url;')
            insert_statement = """
                INSERT INTO account
                (account_id, application_key, account_auth_token, api_url, download_url, minimum_part_size, realm)
                values (?, ?, ?, ?, ?, ?, ?);
            """

            conn.execute(
                insert_statement, (
                    account_id,
                    application_key,
                    auth_token,
                    api_url,
                    download_url,
                    minimum_part_size,
                    realm,
                )
            )

    def get_application_key(self):
        return self._get_account_info_or_raise('application_key')

    def get_account_id(self):
        return self._get_account_info_or_raise('account_id')

    def get_application_key_id(self):
        """
        Return an application key ID.
        The 'account_id_or_app_key_id' column was not in the original schema, so it may be NULL.

        Nota bene - this is the only place where we are not renaming account_id_or_app_key_id to application_key_id
        because it requires a column change.

        application_key_id == account_id_or_app_key_id

        :rtype: str
        """
        result = self._get_account_info_or_raise('account_id_or_app_key_id')
        if result is None:
            return self.get_account_id()
        else:
            return result

    def get_api_url(self):
        return self._get_account_info_or_raise('api_url')

    def get_account_auth_token(self):
        return self._get_account_info_or_raise('account_auth_token')

    def get_download_url(self):
        return self._get_account_info_or_raise('download_url')

    def get_realm(self):
        return self._get_account_info_or_raise('realm')

    def get_recommended_part_size(self):
        return self._get_account_info_or_raise('recommended_part_size')

    def get_absolute_minimum_part_size(self):
        return self._get_account_info_or_raise('absolute_minimum_part_size')

    def get_allowed(self):
        """
        Return 'allowed' dictionary info.
        Example:

        .. code-block:: python

            {
                "bucketId": null,
                "bucketName": null,
                "capabilities": [
                    "listKeys",
                    "writeKeys"
                ],
                "namePrefix": null
            }

        The 'allowed' column was not in the original schema, so it may be NULL.

        :rtype: dict
        """
        allowed_json = self._get_account_info_or_raise('allowed')
        if allowed_json is None:
            return self.DEFAULT_ALLOWED
        else:
            return json.loads(allowed_json)

    def get_s3_api_url(self):
        result = self._get_account_info_or_raise('s3_api_url')
        if result is None:
            return ''
        else:
            return result

    def _get_account_info_or_raise(self, column_name):
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(f'SELECT {column_name} FROM account;')
                value = cursor.fetchone()[0]
                return value
        except Exception as e:
            logger.exception(
                '_get_account_info_or_raise encountered a problem while trying to retrieve "%s"',
                column_name
            )
            raise MissingAccountData(str(e))

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket;')
            for (bucket_name, bucket_id) in name_id_iterable:
                conn.execute(
                    'INSERT INTO bucket (bucket_name, bucket_id) VALUES (?, ?);',
                    (bucket_name, bucket_id)
                )

    def save_bucket(self, bucket):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket WHERE bucket_id = ?;', (bucket.id_,))
            conn.execute(
                'INSERT INTO bucket (bucket_id, bucket_name) VALUES (?, ?);',
                (bucket.id_, bucket.name)
            )

    def remove_bucket_name(self, bucket_name):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket WHERE bucket_name = ?;', (bucket_name,))

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        return self._safe_query(
            'SELECT bucket_id FROM bucket WHERE bucket_name = ?;', (bucket_name,)
        )

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        return self._safe_query('SELECT bucket_name FROM bucket WHERE bucket_id = ?;', (bucket_id,))

    def list_bucket_names_ids(self) -> list[tuple[str, str]]:
        with self._get_connection() as conn:
            cursor = conn.execute('SELECT bucket_name, bucket_id FROM bucket;')
            return cursor.fetchall()

    def _safe_query(self, query, params):
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(query, params)
                return cursor.fetchone()[0]
        except TypeError:  # TypeError: 'NoneType' object is unsubscriptable
            return None
        except sqlite3.Error:
            return None
