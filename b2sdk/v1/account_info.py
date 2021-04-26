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
from typing import Optional

from b2sdk import _v2 as v2


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

    def get_bucket_name_or_none_from_bucket_id(self, bucket_id: str) -> Optional[str]:
        """
        Look up the bucket name for the given bucket id.
        """
        # Removed @abstractmethod decorator


class InMemoryAccountInfo(v2.InMemoryAccountInfo, AbstractAccountInfo):
    pass


class UrlPoolAccountInfo(v2.UrlPoolAccountInfo, AbstractAccountInfo):
    pass


class SqliteAccountInfo(v2.SqliteAccountInfo, AbstractAccountInfo):
    pass


class StubAccountInfo(v2.StubAccountInfo, AbstractAccountInfo):
    pass
