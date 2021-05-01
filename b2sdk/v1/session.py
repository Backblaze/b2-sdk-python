######################################################################
#
# File: b2sdk/v1/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v2 as v2
from .account_info import SqliteAccountInfo


# Override to use legacy signature of account_info.set_auth_data, especially the minimum_part_size argument
class B2Session(v2.B2Session):
    SQLITE_ACCOUNT_INFO_CLASS = staticmethod(SqliteAccountInfo)

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
