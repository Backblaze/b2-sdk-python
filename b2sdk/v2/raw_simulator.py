######################################################################
#
# File: b2sdk/v2/raw_simulator.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
import re
import time
from contextlib import suppress

from b2sdk import v3
from b2sdk.v2._compat import _file_infos_rename

from b2sdk.v2.exception import BadJson, NonExistentBucket, InvalidAuthToken, Unauthorized


class KeySimulator(v3.KeySimulator):
    """
    Hold information about one application key, which can be either
    a master application key, or one created with create_key().
    """

    def __init__(
        self,
        account_id,
        name,
        application_key_id,
        key,
        capabilities,
        expiration_timestamp_or_none,
        bucket_id_or_none,
        bucket_name_or_none,
        name_prefix_or_none,
    ):
        self.name = name
        self.account_id = account_id
        self.application_key_id = application_key_id
        self.key = key
        self.capabilities = capabilities
        self.expiration_timestamp_or_none = expiration_timestamp_or_none
        self.bucket_id_or_none = bucket_id_or_none
        self.bucket_name_or_none = bucket_name_or_none
        self.name_prefix_or_none = name_prefix_or_none

    def as_key(self):
        return dict(
            accountId=self.account_id,
            bucketId=self.bucket_id_or_none,
            applicationKeyId=self.application_key_id,
            capabilities=self.capabilities,
            expirationTimestamp=self.expiration_timestamp_or_none
            and self.expiration_timestamp_or_none * 1000,
            keyName=self.name,
            namePrefix=self.name_prefix_or_none,
        )

    def get_allowed(self):
        """
        Return the 'allowed' structure to include in the response from b2_authorize_account.
        """
        return dict(
            bucketId=self.bucket_id_or_none,
            bucketName=self.bucket_name_or_none,
            capabilities=self.capabilities,
            namePrefix=self.name_prefix_or_none,
        )


class BucketSimulator(v3.BucketSimulator):
    @_file_infos_rename
    def upload_file(
        self,
        upload_id: str,
        upload_auth_token: str,
        file_name: str,
        content_length: int,
        content_type: str,
        content_sha1: str,
        file_info: dict,
        data_stream,
        server_side_encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs,
    ):
        if cache_control is not None:
            file_info['b2-cache-control'] = cache_control
        return super().upload_file(
            upload_id,
            upload_auth_token,
            file_name,
            content_length,
            content_type,
            content_sha1,
            file_info,
            data_stream,
            server_side_encryption,
            file_retention,
            legal_hold,
            custom_upload_timestamp,
            *args,
            **kwargs,
        )


class RawSimulator(v3.RawSimulator):
    @classmethod
    @_file_infos_rename
    def get_upload_file_headers(
        cls,
        upload_auth_token: str,
        file_name: str,
        content_length: int,
        content_type: str,
        content_sha1: str,
        file_info: dict,
        server_side_encryption: v3.EncryptionSetting | None,
        file_retention: v3.FileRetentionSetting | None,
        legal_hold: v3.LegalHold | None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs,
    ) -> dict:
        if cache_control is not None:
            file_info['b2-cache-control'] = cache_control
        return super().get_upload_file_headers(
            upload_auth_token,
            file_name,
            content_length,
            content_type,
            content_sha1,
            file_info,
            server_side_encryption,
            file_retention,
            legal_hold,
            custom_upload_timestamp,
            *args,
            **kwargs,
        )

    @_file_infos_rename
    def upload_file(
        self,
        upload_url: str,
        upload_auth_token: str,
        file_name: str,
        content_length: int,
        content_type: str,
        content_sha1: str,
        file_info: dict,
        data_stream,
        server_side_encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs,
    ):
        if cache_control is not None:
            file_info['b2-cache-control'] = cache_control
        return super().upload_file(
            upload_url,
            upload_auth_token,
            file_name,
            content_length,
            content_type,
            content_sha1,
            file_info,
            data_stream,
            server_side_encryption,
            file_retention,
            legal_hold,
            custom_upload_timestamp,
            *args,
            **kwargs,
        )

    def create_account(self):
        """
        Simulate creating an account.

        Return (accountId, masterApplicationKey) for a newly created account.
        """
        # Pick the IDs for the account and the key
        account_id = 'account-%d' % (self.account_counter,)
        master_key = 'masterKey-%d' % (self.account_counter,)
        self.account_counter += 1

        # Create the key
        self.key_id_to_key[account_id] = KeySimulator(
            account_id=account_id,
            name='master',
            application_key_id=account_id,
            key=master_key,
            capabilities=v3.ALL_CAPABILITIES,
            expiration_timestamp_or_none=None,
            bucket_id_or_none=None,
            bucket_name_or_none=None,
            name_prefix_or_none=None,
        )

        # Return the info
        return (account_id, master_key)

    def create_key(
        self,
        api_url,
        account_auth_token,
        account_id,
        capabilities,
        key_name,
        valid_duration_seconds,
        bucket_id,
        name_prefix,
    ):
        if not re.match(r'^[A-Za-z0-9-]{1,100}$', key_name):
            raise BadJson('illegal key name: ' + key_name)
        if valid_duration_seconds is not None:
            if valid_duration_seconds < 1 or valid_duration_seconds > self.MAX_DURATION_IN_SECONDS:
                raise BadJson(
                    'valid duration must be greater than 0, and less than 1000 days in seconds'
                )
        self._assert_account_auth(api_url, account_auth_token, account_id, 'writeKeys')

        if valid_duration_seconds is None:
            expiration_timestamp_or_none = None
        else:
            expiration_timestamp_or_none = int(time.time() + valid_duration_seconds)

        index = self.app_key_counter
        self.app_key_counter += 1
        application_key_id = 'appKeyId%d' % (index,)
        app_key = 'appKey%d' % (index,)
        bucket_name_or_none = None
        if bucket_id is not None:
            # It is possible for bucketId to be filled and bucketName to be empty.
            # It can happen when the bucket was deleted.
            with suppress(NonExistentBucket):
                bucket_name_or_none = self._get_bucket_by_id(bucket_id).bucket_name

        key_sim = KeySimulator(
            account_id=account_id,
            name=key_name,
            application_key_id=application_key_id,
            key=app_key,
            capabilities=capabilities,
            expiration_timestamp_or_none=expiration_timestamp_or_none,
            bucket_id_or_none=bucket_id,
            bucket_name_or_none=bucket_name_or_none,
            name_prefix_or_none=name_prefix,
        )
        self.key_id_to_key[application_key_id] = key_sim
        self.all_application_keys.append(key_sim)
        return key_sim.as_created_key()

    def _assert_account_auth(
        self, api_url, account_auth_token, account_id, capability, bucket_id=None, file_name=None
    ):
        key_sim = self.auth_token_to_key.get(account_auth_token)
        assert key_sim is not None
        assert api_url == self.API_URL
        assert account_id == key_sim.account_id
        if account_auth_token in self.expired_auth_tokens:
            raise InvalidAuthToken('auth token expired', 'auth_token_expired')
        if capability not in key_sim.capabilities:
            raise Unauthorized('', 'unauthorized')
        if key_sim.bucket_id_or_none is not None and key_sim.bucket_id_or_none != bucket_id:
            raise Unauthorized('', 'unauthorized')
        if key_sim.name_prefix_or_none is not None:
            if file_name is not None and not file_name.startswith(key_sim.name_prefix_or_none):
                raise Unauthorized('', 'unauthorized')

    def authorize_account(self, realm_url, application_key_id, application_key):
        key_sim = self.key_id_to_key.get(application_key_id)
        if key_sim is None:
            raise InvalidAuthToken('application key ID not valid', 'unauthorized')
        if application_key != key_sim.key:
            raise InvalidAuthToken('secret key is wrong', 'unauthorized')
        auth_token = 'auth_token_%d' % (self.auth_token_counter,)
        self.current_token = auth_token
        self.auth_token_counter += 1
        self.auth_token_to_key[auth_token] = key_sim
        allowed = key_sim.get_allowed()
        bucketId = allowed.get('bucketId')
        if (bucketId is not None) and (bucketId in self.bucket_id_to_bucket):
            allowed['bucketName'] = self.bucket_id_to_bucket[bucketId].bucket_name
        else:
            allowed['bucketName'] = None
        return dict(
            accountId=key_sim.account_id,
            authorizationToken=auth_token,
            apiInfo=dict(
                groupsApi=dict(),
                storageApi=dict(
                    apiUrl=self.API_URL,
                    downloadUrl=self.DOWNLOAD_URL,
                    recommendedPartSize=self.MIN_PART_SIZE,
                    absoluteMinimumPartSize=self.MIN_PART_SIZE,
                    allowed=allowed,
                    s3ApiUrl=self.S3_API_URL,
                    bucketId=allowed['bucketId'],
                    bucketName=allowed['bucketName'],
                    capabilities=allowed['capabilities'],
                    namePrefix=allowed['namePrefix'],
                ),
            ),
        )
