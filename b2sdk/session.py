######################################################################
#
# File: b2sdk/session.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from functools import partial
from enum import Enum, unique
from typing import Any, Dict, Optional
import logging

from b2sdk.account_info.abstract import AbstractAccountInfo
from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo
from b2sdk.account_info.exception import MissingAccountData
from b2sdk.b2http import B2Http
from b2sdk.cache import AbstractCache, AuthInfoCache, DummyCache
from b2sdk.encryption.setting import EncryptionSetting
from b2sdk.exception import (InvalidAuthToken, Unauthorized)
from b2sdk.file_lock import BucketRetentionSetting, FileRetentionSetting, LegalHold
from b2sdk.raw_api import ALL_CAPABILITIES, REALM_URLS
from b2sdk.api_config import B2HttpApiConfig, DEFAULT_HTTP_API_CONFIG

logger = logging.getLogger(__name__)


@unique
class TokenType(Enum):
    API = 'api'
    API_TOKEN_ONLY = 'api_token_only'
    UPLOAD_PART = 'upload_part'
    UPLOAD_SMALL = 'upload_small'


class B2Session(object):
    """
        A facade that supplies the correct api_url and account_auth_token
        to methods of underlying raw_api and reauthorizes if necessary.
    """
    SQLITE_ACCOUNT_INFO_CLASS = staticmethod(SqliteAccountInfo)

    def __init__(
        self,
        account_info: Optional[AbstractAccountInfo] = None,
        cache: Optional[AbstractCache] = None,
        api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG
    ):
        """
        Initialize Session using given account info.

        :param account_info: an instance of :class:`~b2sdk.v1.UrlPoolAccountInfo`,
                      or any custom class derived from
                      :class:`~b2sdk.v1.AbstractAccountInfo`
                      To learn more about Account Info objects, see here
                      :class:`~b2sdk.v1.SqliteAccountInfo`

        :param cache: an instance of the one of the following classes:
                      :class:`~b2sdk.cache.DummyCache`, :class:`~b2sdk.cache.InMemoryCache`,
                      :class:`~b2sdk.cache.AuthInfoCache`,
                      or any custom class derived from :class:`~b2sdk.cache.AbstractCache`
                      It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk.cache.DummyCache`

        :param api_config
        """

        self.raw_api = api_config.raw_api_class(B2Http(api_config))
        if account_info is None:
            account_info = self.SQLITE_ACCOUNT_INFO_CLASS()
            if cache is None:
                cache = AuthInfoCache(account_info)
        if cache is None:
            cache = DummyCache()

        self.account_info = account_info
        self.cache = cache
        self._token_callbacks = {
            TokenType.API: self._api_token_callback,
            TokenType.API_TOKEN_ONLY: self._api_token_only_callback,
            TokenType.UPLOAD_SMALL: self._upload_small,
            TokenType.UPLOAD_PART: self._upload_part,
        }

    def authorize_automatically(self):
        """
        Perform automatic account authorization, retrieving all account data
        from account info object passed during initialization.
        """
        try:
            self.authorize_account(
                self.account_info.get_realm(),
                self.account_info.get_application_key_id(),
                self.account_info.get_application_key(),
            )
        except MissingAccountData:
            return False
        return True

    def authorize_account(self, realm, application_key_id, application_key):
        """
        Perform account authorization.

        :param str realm: a realm to authorize account in (usually just "production")
        :param str application_key_id: :term:`application key ID`
        :param str application_key: user's :term:`application key`
        """
        # Authorize
        realm_url = REALM_URLS.get(realm, realm)
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
            absolute_minimum_part_size=response['absoluteMinimumPartSize'],
            recommended_part_size=response['recommendedPartSize'],
            application_key=application_key,
            realm=realm,
            s3_api_url=response['s3ApiUrl'],
            allowed=allowed,
            application_key_id=application_key_id
        )

    def cancel_large_file(self, file_id):
        return self._wrap_default_token(self.raw_api.cancel_large_file, file_id)

    def create_bucket(
        self,
        account_id,
        bucket_name,
        bucket_type,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        default_server_side_encryption=None,
        is_file_lock_enabled: Optional[bool] = None,
    ):
        return self._wrap_default_token(
            self.raw_api.create_bucket,
            account_id,
            bucket_name,
            bucket_type,
            bucket_info=bucket_info,
            cors_rules=cors_rules,
            lifecycle_rules=lifecycle_rules,
            default_server_side_encryption=default_server_side_encryption,
            is_file_lock_enabled=is_file_lock_enabled,
        )

    def create_key(
        self, account_id, capabilities, key_name, valid_duration_seconds, bucket_id, name_prefix
    ):
        return self._wrap_default_token(
            self.raw_api.create_key,
            account_id,
            capabilities,
            key_name,
            valid_duration_seconds,
            bucket_id,
            name_prefix,
        )

    def delete_key(self, application_key_id):
        return self._wrap_default_token(self.raw_api.delete_key, application_key_id)

    def delete_bucket(self, account_id, bucket_id):
        return self._wrap_default_token(self.raw_api.delete_bucket, account_id, bucket_id)

    def delete_file_version(self, file_id, file_name):
        return self._wrap_default_token(self.raw_api.delete_file_version, file_id, file_name)

    def download_file_from_url(
        self, url, range_=None, encryption: Optional[EncryptionSetting] = None
    ):
        return self._wrap_token(
            self.raw_api.download_file_from_url,
            TokenType.API_TOKEN_ONLY,
            url,
            range_=range_,
            encryption=encryption,
        )

    def finish_large_file(self, file_id, part_sha1_array):
        return self._wrap_default_token(self.raw_api.finish_large_file, file_id, part_sha1_array)

    def get_download_authorization(self, bucket_id, file_name_prefix, valid_duration_in_seconds):
        return self._wrap_default_token(
            self.raw_api.get_download_authorization, bucket_id, file_name_prefix,
            valid_duration_in_seconds
        )

    def get_file_info_by_id(self, file_id: str) -> Dict[str, Any]:
        return self._wrap_default_token(self.raw_api.get_file_info_by_id, file_id)

    def get_file_info_by_name(self, bucket_name: str, file_name: str) -> Dict[str, Any]:
        return self._wrap_default_token(self.raw_api.get_file_info_by_name, bucket_name, file_name)

    def get_upload_url(self, bucket_id):
        return self._wrap_default_token(self.raw_api.get_upload_url, bucket_id)

    def get_upload_part_url(self, file_id):
        return self._wrap_default_token(self.raw_api.get_upload_part_url, file_id)

    def hide_file(self, bucket_id, file_name):
        return self._wrap_default_token(self.raw_api.hide_file, bucket_id, file_name)

    def list_buckets(self, account_id, bucket_id=None, bucket_name=None):
        return self._wrap_default_token(
            self.raw_api.list_buckets,
            account_id,
            bucket_id=bucket_id,
            bucket_name=bucket_name,
        )

    def list_file_names(
        self,
        bucket_id,
        start_file_name=None,
        max_file_count=None,
        prefix=None,
    ):
        return self._wrap_default_token(
            self.raw_api.list_file_names,
            bucket_id,
            start_file_name=start_file_name,
            max_file_count=max_file_count,
            prefix=prefix,
        )

    def list_file_versions(
        self,
        bucket_id,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        return self._wrap_default_token(
            self.raw_api.list_file_versions,
            bucket_id,
            start_file_name=start_file_name,
            start_file_id=start_file_id,
            max_file_count=max_file_count,
            prefix=prefix,
        )

    def list_keys(self, account_id, max_key_count=None, start_application_key_id=None):
        return self._wrap_default_token(
            self.raw_api.list_keys,
            account_id,
            max_key_count=max_key_count,
            start_application_key_id=start_application_key_id,
        )

    def list_parts(self, file_id, start_part_number, max_part_count):
        return self._wrap_default_token(
            self.raw_api.list_parts, file_id, start_part_number, max_part_count
        )

    def list_unfinished_large_files(
        self,
        bucket_id,
        start_file_id=None,
        max_file_count=None,
        prefix=None,
    ):
        return self._wrap_default_token(
            self.raw_api.list_unfinished_large_files,
            bucket_id,
            start_file_id=start_file_id,
            max_file_count=max_file_count,
            prefix=prefix,
        )

    def start_large_file(
        self,
        bucket_id,
        file_name,
        content_type,
        file_info,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        return self._wrap_default_token(
            self.raw_api.start_large_file,
            bucket_id,
            file_name,
            content_type,
            file_info,
            server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def update_bucket(
        self,
        account_id,
        bucket_id,
        bucket_type=None,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules=None,
        if_revision_is=None,
        default_server_side_encryption: Optional[EncryptionSetting] = None,
        default_retention: Optional[BucketRetentionSetting] = None,
    ):
        return self._wrap_default_token(
            self.raw_api.update_bucket,
            account_id,
            bucket_id,
            bucket_type=bucket_type,
            bucket_info=bucket_info,
            cors_rules=cors_rules,
            lifecycle_rules=lifecycle_rules,
            if_revision_is=if_revision_is,
            default_server_side_encryption=default_server_side_encryption,
            default_retention=default_retention,
        )

    def upload_file(
        self,
        bucket_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_infos,
        data_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        return self._wrap_token(
            self.raw_api.upload_file,
            TokenType.UPLOAD_SMALL,
            bucket_id,
            file_name,
            content_length,
            content_type,
            content_sha1,
            file_infos,
            data_stream,
            server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def upload_part(
        self,
        file_id,
        part_number,
        content_length,
        sha1_sum,
        input_stream,
        server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        return self._wrap_token(
            self.raw_api.upload_part,
            TokenType.UPLOAD_PART,
            file_id,
            part_number,
            content_length,
            sha1_sum,
            input_stream,
            server_side_encryption,
        )

    def get_download_url_by_id(self, file_id):
        return self.raw_api.get_download_url_by_id(self.account_info.get_download_url(), file_id)

    def get_download_url_by_name(self, bucket_name, file_name):
        return self.raw_api.get_download_url_by_name(
            self.account_info.get_download_url(), bucket_name, file_name
        )

    def copy_file(
        self,
        source_file_id,
        new_file_name,
        bytes_range=None,
        metadata_directive=None,
        content_type=None,
        file_info=None,
        destination_bucket_id=None,
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
        file_retention: Optional[FileRetentionSetting] = None,
        legal_hold: Optional[LegalHold] = None,
    ):
        return self._wrap_default_token(
            self.raw_api.copy_file,
            source_file_id,
            new_file_name,
            bytes_range=bytes_range,
            metadata_directive=metadata_directive,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id,
            destination_server_side_encryption=destination_server_side_encryption,
            source_server_side_encryption=source_server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
        )

    def copy_part(
        self,
        source_file_id,
        large_file_id,
        part_number,
        bytes_range=None,
        destination_server_side_encryption: Optional[EncryptionSetting] = None,
        source_server_side_encryption: Optional[EncryptionSetting] = None,
    ):
        return self._wrap_default_token(
            self.raw_api.copy_part,
            source_file_id,
            large_file_id,
            part_number,
            bytes_range=bytes_range,
            destination_server_side_encryption=destination_server_side_encryption,
            source_server_side_encryption=source_server_side_encryption,
        )

    def _wrap_default_token(self, raw_api_method, *args, **kwargs):
        return self._wrap_token(raw_api_method, TokenType.API, *args, **kwargs)

    def _wrap_token(self, raw_api_method, token_type, *args, **kwargs):
        callback = self._token_callbacks[token_type]
        partial_callback = partial(callback, raw_api_method, *args, **kwargs)

        return self._reauthorization_loop(partial_callback)

    def _api_token_callback(self, raw_api_method, *args, **kwargs):
        api_url = self.account_info.get_api_url()
        account_auth_token = self.account_info.get_account_auth_token()
        return raw_api_method(api_url, account_auth_token, *args, **kwargs)

    def _api_token_only_callback(self, raw_api_method, *args, **kwargs):
        account_auth_token = self.account_info.get_account_auth_token()
        return raw_api_method(account_auth_token, *args, **kwargs)

    def _reauthorization_loop(self, callback):
        auth_failure_encountered = False
        while 1:
            try:
                return callback()
            except InvalidAuthToken:
                if not auth_failure_encountered:
                    auth_failure_encountered = True
                    reauthorization_success = self.authorize_automatically()
                    if reauthorization_success:
                        continue
                raise
            except Unauthorized as e:
                raise self._add_app_key_info_to_unauthorized(e)

    def _add_app_key_info_to_unauthorized(self, unauthorized):
        """
        Take an Unauthorized error and adds information from the application key
        about why it might have failed.
        """
        # What's allowed?
        allowed = self.account_info.get_allowed()
        capabilities = allowed['capabilities']
        bucket_name = allowed['bucketName']
        name_prefix = allowed['namePrefix']

        # Make a list of messages about the application key restrictions
        key_messages = []
        if set(capabilities) != set(ALL_CAPABILITIES):
            key_messages.append("with capabilities '" + ','.join(capabilities) + "'")
        if bucket_name is not None:
            key_messages.append("restricted to bucket '" + bucket_name + "'")
        if name_prefix is not None:
            key_messages.append("restricted to files that start with '" + name_prefix + "'")
        if not key_messages:
            key_messages.append('with no restrictions')

        # Make a new message
        new_message = unauthorized.message
        if new_message == '':
            new_message = 'unauthorized'
        new_message += ' for application key ' + ', '.join(key_messages)

        return Unauthorized(new_message, unauthorized.code)

    def _get_upload_data(self, bucket_id):
        """
        Take ownership of an upload URL / auth token for the bucket and
        return it.
        """
        account_info = self.account_info
        upload_url, upload_auth_token = account_info.take_bucket_upload_url(bucket_id)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.get_upload_url(bucket_id)
        return response['uploadUrl'], response['authorizationToken']

    def _get_upload_part_data(self, file_id):
        """
        Make sure that we have an upload URL and auth token for the given bucket and
        return it.
        """
        account_info = self.account_info
        upload_url, upload_auth_token = account_info.take_large_file_upload_url(file_id)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.get_upload_part_url(file_id)
        return response['uploadUrl'], response['authorizationToken']

    def _upload_small(self, f, bucket_id, *args, **kwargs):
        upload_url, upload_auth_token = self._get_upload_data(bucket_id)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self.account_info.put_bucket_upload_url(bucket_id, upload_url, upload_auth_token)
        return response

    def _upload_part(self, f, file_id, *args, **kwargs):
        upload_url, upload_auth_token = self._get_upload_part_data(file_id)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self.account_info.put_large_file_upload_url(file_id, upload_url, upload_auth_token)
        return response

    def update_file_retention(
        self,
        file_id,
        file_name,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ):
        return self._wrap_default_token(
            self.raw_api.update_file_retention,
            file_id,
            file_name,
            file_retention,
            bypass_governance,
        )

    def update_file_legal_hold(
        self,
        file_id,
        file_name,
        legal_hold: LegalHold,
    ):
        return self._wrap_default_token(
            self.raw_api.update_file_legal_hold,
            file_id,
            file_name,
            legal_hold,
        )
