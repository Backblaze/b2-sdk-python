######################################################################
#
# File: b2sdk/v1/account_info.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import abstractmethod
import inspect
import logging
import os

from b2sdk import v2
from b2sdk._internal.account_info.sqlite_account_info import DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE
from b2sdk._internal.utils import limit_trace_arguments

logger = logging.getLogger(__name__)


# Retain legacy get_minimum_part_size and facilitate for optional s3_api_url
class OldAccountInfoMethods:
    REALM_URLS = v2.REALM_URLS

    @limit_trace_arguments(
        only=[
            'self',
            'api_url',
            'download_url',
            'minimum_part_size',
            'realm',
            's3_api_url',
        ]
    )
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
            s3_kwargs = dict(s3_api_url=s3_api_url)
        else:
            s3_kwargs = {}
        if allowed is None:
            allowed = self.DEFAULT_ALLOWED
        assert self.allowed_is_valid(allowed)

        self._set_auth_data(
            account_id=account_id,
            auth_token=auth_token,
            api_url=api_url,
            download_url=download_url,
            minimum_part_size=minimum_part_size,
            application_key=application_key,
            realm=realm,
            allowed=allowed,
            application_key_id=application_key_id,
            **s3_kwargs,
        )


# translate legacy "minimum_part_size" to new style "recommended_part_size"
class MinimumPartSizeTranslator:
    def _set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
        s3_api_url=None,
        allowed=None,
        application_key_id=None
    ):
        if 's3_api_url' in inspect.getfullargspec(super()._set_auth_data).args:
            s3_kwargs = dict(s3_api_url=s3_api_url)
        else:
            s3_kwargs = {}
        return super()._set_auth_data(
            account_id=account_id,
            auth_token=auth_token,
            api_url=api_url,
            download_url=download_url,
            recommended_part_size=minimum_part_size,
            absolute_minimum_part_size=DEFAULT_ABSOLUTE_MINIMUM_PART_SIZE,
            application_key=application_key,
            realm=realm,
            allowed=allowed,
            application_key_id=application_key_id,
            **s3_kwargs,
        )

    def get_minimum_part_size(self):
        return self.get_recommended_part_size()


class AbstractAccountInfo(OldAccountInfoMethods, v2.AbstractAccountInfo):
    def get_s3_api_url(self):
        """
        Return s3_api_url or raises MissingAccountData exception.

        :rtype: str
        """
        # Removed @abstractmethod decorators

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> str | None:
        """
        Look up the bucket name for the given bucket id.
        """
        # Removed @abstractmethod decorator

    def get_recommended_part_size(self):
        """
        Return the recommended number of bytes in a part of a large file.

        :return: number of bytes
        :rtype: int
        """
        # Removed @abstractmethod decorator

    def get_absolute_minimum_part_size(self):
        """
        Return the absolute minimum number of bytes in a part of a large file.

        :return: number of bytes
        :rtype: int
        """
        # Removed @abstractmethod decorator

    @abstractmethod
    def get_minimum_part_size(self):
        """
        Return the minimum number of bytes in a part of a large file.

        :return: number of bytes
        :rtype: int
        """
        # This stays abstract in v1

    @abstractmethod
    def _set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm, s3_api_url, allowed, application_key_id
    ):
        """
        Actually store the auth data.  Can assume that 'allowed' is present and valid.

        All of the information returned by ``b2_authorize_account`` is saved, because all of it is
        needed at some point.
        """
        # Keep the old signature


class InMemoryAccountInfo(MinimumPartSizeTranslator, OldAccountInfoMethods, v2.InMemoryAccountInfo):
    pass


class UrlPoolAccountInfo(OldAccountInfoMethods, v2.UrlPoolAccountInfo):
    pass


class SqliteAccountInfo(MinimumPartSizeTranslator, OldAccountInfoMethods, v2.SqliteAccountInfo):
    def __init__(self, file_name=None, last_upgrade_to_run=None):
        """
        If ``file_name`` argument is empty or ``None``, path from ``B2_ACCOUNT_INFO`` environment variable is used. If that is not available, a default of ``~/.b2_account_info`` is used.

        :param str file_name: The sqlite file to use; overrides the default.
        :param int last_upgrade_to_run: For testing only, override the auto-update on the db.
        """
        # use legacy env var resolution, XDG not supported
        file_name = file_name or os.environ.get(
            v2.B2_ACCOUNT_INFO_ENV_VAR, v2.B2_ACCOUNT_INFO_DEFAULT_FILE
        )
        super().__init__(file_name=file_name, last_upgrade_to_run=last_upgrade_to_run)


class StubAccountInfo(MinimumPartSizeTranslator, OldAccountInfoMethods, v2.StubAccountInfo):
    REALM_URLS = {'production': 'http://production.example.com'}
