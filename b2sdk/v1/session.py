######################################################################
#
# File: b2sdk/v1/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v2
from b2sdk.v2.exception import InvalidArgument
from .account_info import SqliteAccountInfo


# Override to use legacy signature of account_info.set_auth_data, especially the minimum_part_size argument
# and to accept old-style raw_api argument
class B2Session(v2.B2Session):
    SQLITE_ACCOUNT_INFO_CLASS = staticmethod(SqliteAccountInfo)

    def __init__(
        self,
        account_info=None,
        cache=None,
        raw_api: v2.B2RawHTTPApi = None,
        api_config: v2.B2HttpApiConfig | None = None
    ):
        if raw_api is not None and api_config is not None:
            raise InvalidArgument(
                'raw_api,api_config', 'Provide at most one of: raw_api, api_config'
            )

        if api_config is None:
            api_config = v2.DEFAULT_HTTP_API_CONFIG
        super().__init__(account_info=account_info, cache=cache, api_config=api_config)
        if raw_api is not None:
            self.raw_api = raw_api

    def authorize_account(self, realm, application_key_id, application_key):
        """
        Perform account authorization.

        :param str realm: a realm to authorize account in (usually just "production")
        :param str application_key_id: :term:`application key ID`
        :param str application_key: user's :term:`application key`
        """
        # Authorize
        realm_url = self.account_info.REALM_URLS.get(realm, realm)
        response = self.raw_api.authorize_account(realm_url, application_key_id, application_key)
        account_id = response['accountId']
        allowed = response['allowed']

        # Clear the cache if new account has been used
        if not self.account_info.is_same_account(account_id, realm):
            self.cache.clear()

        # Store the auth data
        self.account_info.set_auth_data(
            account_id=account_id,
            auth_token=response['authorizationToken'],
            api_url=response['apiUrl'],
            download_url=response['downloadUrl'],
            minimum_part_size=response['recommendedPartSize'],
            application_key=application_key,
            realm=realm,
            s3_api_url=response['s3ApiUrl'],
            allowed=allowed,
            application_key_id=application_key_id
        )
