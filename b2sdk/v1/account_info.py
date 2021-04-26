######################################################################
#
# File: b2sdk/v1/account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import inspect
import threading
import os

from b2sdk import _v2 as v2
from b2sdk.account_info.sqlite_account_info import logger


class OldAccountInfoMethods:
    def set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
        allowed=None,
        application_key_id=None,
        s3_api_url=None,
    ):
        if 's3_api_url' in inspect.getfullargspec(self._set_auth_data).args:
            super().set_auth_data(
                account_id,
                auth_token,
                api_url,
                download_url,
                minimum_part_size,
                application_key,
                realm,
                s3_api_url,
                allowed=allowed,
                application_key_id=application_key_id
            )
        else:
            if allowed is None:
                allowed = self.DEFAULT_ALLOWED
            assert self.allowed_is_valid(allowed)

            self._set_auth_data(
                account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
                realm, allowed, application_key_id
            )


class AbstractAccountInfo(OldAccountInfoMethods, v2.AbstractAccountInfo):
    def get_s3_api_url(self):
        """
        Return s3_api_url or raises MissingAccountData exception.

        :rtype: str
        """
        # Removed @abstractmethod decorators


class InMemoryAccountInfo(v2.InMemoryAccountInfo, AbstractAccountInfo):
    pass


class UrlPoolAccountInfo(v2.UrlPoolAccountInfo, AbstractAccountInfo):
    pass


class SqliteAccountInfo(v2.SqliteAccountInfo, AbstractAccountInfo):
    def __init__(self, file_name=None, last_upgrade_to_run=None):
        """
        If ``file_name`` argument is empty or ``None``, path from ``B2_ACCOUNT_INFO`` environment variable is used. If that is not available, a default of ``~/.b2_account_info`` is used.

        :param str file_name: The sqlite file to use; overrides the default.
        :param int last_upgrade_to_run: For testing only, override the auto-update on the db.
        """
        # use legacy env var resolution, XDG not supported
        self.thread_local = threading.local()
        user_account_info_path = file_name or os.environ.get(
            v2.B2_ACCOUNT_INFO_ENV_VAR, v2.B2_ACCOUNT_INFO_DEFAULT_FILE
        )
        self.filename = file_name or os.path.expanduser(user_account_info_path)
        logger.debug('%s file path to use: %s', self.__class__.__name__, self.filename)
        self._validate_database()
        with self._get_connection() as conn:
            self._create_tables(conn, last_upgrade_to_run)
        super(v2.SqliteAccountInfo, self).__init__()


class StubAccountInfo(v2.StubAccountInfo, AbstractAccountInfo):
    pass
