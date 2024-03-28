######################################################################
#
# File: b2sdk/v0/account_info.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk._internal import version_utils
from b2sdk import v1


class OldAccountInfoMethods:
    """ this class contains proxy methods for deprecated signatures renamed for consistency in mid-2019 """

    def get_account_id_or_app_key_id(self):
        """
        Return the application key ID used to authenticate.

        :rtype: str

        .. deprecated:: 0.1.6
           Use :func:`get_application_key_id` instead.
        """
        return self.get_application_key_id()

    @version_utils.rename_argument(
        'account_id_or_app_key_id',
        'application_key_id',
        '0.1.5',
        None,
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
        # we need to enumerate all the parameters and cannot just "*args, **kwargs" because
        # the deprecation decorator doesn't feel safe with the kwargs approach
        return super().set_auth_data(
            account_id,
            auth_token,
            api_url,
            download_url,
            minimum_part_size,
            application_key,
            realm,
            allowed,
            application_key_id,
            s3_api_url=s3_api_url,
        )


class AbstractAccountInfo(OldAccountInfoMethods, v1.AbstractAccountInfo):
    pass


class InMemoryAccountInfo(OldAccountInfoMethods, v1.InMemoryAccountInfo):
    pass


class UrlPoolAccountInfo(OldAccountInfoMethods, v1.UrlPoolAccountInfo):
    pass


class SqliteAccountInfo(OldAccountInfoMethods, v1.SqliteAccountInfo):
    pass
