######################################################################
#
# File: b2sdk/_internal/session.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
from contextvars import ContextVar
from enum import Enum, unique
from functools import partial
from typing import Any

from b2sdk._internal.account_info.abstract import AbstractAccountInfo
from b2sdk._internal.account_info.exception import MissingAccountData
from b2sdk._internal.account_info.sqlite_account_info import SqliteAccountInfo
from b2sdk._internal.api_config import DEFAULT_HTTP_API_CONFIG, B2HttpApiConfig
from b2sdk._internal.b2http import B2Http
from b2sdk._internal.cache import AbstractCache, AuthInfoCache, DummyCache
from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.exception import InvalidAuthToken, Unauthorized
from b2sdk._internal.file_lock import BucketRetentionSetting, FileRetentionSetting, LegalHold
from b2sdk._internal.raw_api import ALL_CAPABILITIES, REALM_URLS, LifecycleRule
from b2sdk._internal.replication.setting import ReplicationConfiguration

logger = logging.getLogger(__name__)

_upload_url_request = ContextVar('upload_url_request', default=None)


@unique
class TokenType(Enum):
    API = 'api'
    API_TOKEN_ONLY = 'api_token_only'
    UPLOAD_PART = 'upload_part'
    UPLOAD_SMALL = 'upload_small'


class B2Session:
    """
    A facade that supplies the correct api_url and account_auth_token
    to methods of underlying raw_api and reauthorizes if necessary.
    """

    SQLITE_ACCOUNT_INFO_CLASS = staticmethod(SqliteAccountInfo)
    B2HTTP_CLASS = staticmethod(B2Http)

    def __init__(
        self,
        account_info: AbstractAccountInfo | None = None,
        cache: AbstractCache | None = None,
        api_config: B2HttpApiConfig = DEFAULT_HTTP_API_CONFIG,
    ):
        """
        Initialize Session using given account info.

        :param account_info: an instance of :class:`~b2sdk.v2.UrlPoolAccountInfo`,
                      or any custom class derived from
                      :class:`~b2sdk.v2.AbstractAccountInfo`
                      To learn more about Account Info objects, see here
                      :class:`~b2sdk.v2.SqliteAccountInfo`

        :param cache: an instance of the one of the following classes:
                      :class:`~b2sdk._internal.cache.DummyCache`, :class:`~b2sdk._internal.cache.InMemoryCache`,
                      :class:`~b2sdk._internal.cache.AuthInfoCache`,
                      or any custom class derived from :class:`~b2sdk._internal.cache.AbstractCache`
                      It is used by B2Api to cache the mapping between bucket name and bucket ids.
                      default is :class:`~b2sdk._internal.cache.DummyCache`

        :param api_config
        """

        self.raw_api = api_config.raw_api_class(self.B2HTTP_CLASS(api_config))
        if account_info is None:
            account_info = self.SQLITE_ACCOUNT_INFO_CLASS()
        if cache is None:
            if account_info:
                cache = AuthInfoCache(account_info)
            else:
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
        storage_api_info = response['apiInfo']['storageApi']

        allowed = self._construct_allowed_dict(storage_api_info)

        # Clear the cache if new account has been used
        if not self.account_info.is_same_account(account_id, realm):
            self.cache.clear()

        # Store the auth data
        self.account_info.set_auth_data(
            account_id=account_id,
            auth_token=response['authorizationToken'],
            api_url=storage_api_info['apiUrl'],
            download_url=storage_api_info['downloadUrl'],
            absolute_minimum_part_size=storage_api_info['absoluteMinimumPartSize'],
            recommended_part_size=storage_api_info['recommendedPartSize'],
            application_key=application_key,
            realm=realm,
            s3_api_url=storage_api_info['s3ApiUrl'],
            allowed=allowed,
            application_key_id=application_key_id,
        )

    def _construct_allowed_dict(self, storage_api_info):
        # `allowed` object has been deprecated in the v3 of the API, but we still
        # construct it artificially to avoid changes in all the reliant parts.

        return storage_api_info['allowed']

    def cancel_large_file(self, file_id):
        return self._wrap_default_token(self.raw_api.cancel_large_file, file_id)

    def create_bucket(
        self,
        account_id,
        bucket_name,
        bucket_type,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules: list[LifecycleRule] | None = None,
        default_server_side_encryption=None,
        is_file_lock_enabled: bool | None = None,
        replication: ReplicationConfiguration | None = None,
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
            replication=replication,
        )

    def create_key(
        self, account_id, capabilities, key_name, valid_duration_seconds, bucket_ids, name_prefix
    ):
        return self._wrap_default_token(
            self.raw_api.create_key,
            account_id,
            capabilities,
            key_name,
            valid_duration_seconds,
            bucket_ids,
            name_prefix,
        )

    def delete_key(self, application_key_id):
        return self._wrap_default_token(self.raw_api.delete_key, application_key_id)

    def delete_bucket(self, account_id, bucket_id):
        return self._wrap_default_token(self.raw_api.delete_bucket, account_id, bucket_id)

    def delete_file_version(self, file_id, file_name, bypass_governance: bool = False):
        return self._wrap_default_token(
            self.raw_api.delete_file_version, file_id, file_name, bypass_governance
        )

    def download_file_from_url(
        self,
        url: str,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
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
            self.raw_api.get_download_authorization,
            bucket_id,
            file_name_prefix,
            valid_duration_in_seconds,
        )

    def get_file_info_by_id(self, file_id: str) -> dict[str, Any]:
        return self._wrap_default_token(self.raw_api.get_file_info_by_id, file_id)

    def get_file_info_by_name(self, bucket_name: str, file_name: str) -> dict[str, Any]:
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
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
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
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def update_bucket(
        self,
        account_id,
        bucket_id,
        bucket_type=None,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules: list[LifecycleRule] | None = None,
        if_revision_is=None,
        default_server_side_encryption: EncryptionSetting | None = None,
        default_retention: BucketRetentionSetting | None = None,
        replication: ReplicationConfiguration | None = None,
        is_file_lock_enabled: bool | None = None,
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
            replication=replication,
            is_file_lock_enabled=is_file_lock_enabled,
        )

    def upload_file(
        self,
        bucket_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
        data_stream,
        server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        return self._wrap_token(
            self.raw_api.upload_file,
            TokenType.UPLOAD_SMALL,
            bucket_id,
            file_name,
            content_length,
            content_type,
            content_sha1,
            file_info,
            data_stream,
            server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def upload_part(
        self,
        file_id,
        part_number,
        content_length,
        sha1_sum,
        input_stream,
        server_side_encryption: EncryptionSetting | None = None,
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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
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
        destination_server_side_encryption: EncryptionSetting | None = None,
        source_server_side_encryption: EncryptionSetting | None = None,
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

        return self._execute_with_auth_retry(partial_callback)

    def _api_token_callback(self, raw_api_method, *args, **kwargs):
        request = _upload_url_request.get()
        if request is not None and request['method'] == raw_api_method:
            # Snapshot the generation with the credentials, never across network I/O.
            with self.account_info._upload_pool_context() as generation:
                api_url = self.account_info.get_api_url()
                account_auth_token = self.account_info.get_account_auth_token()
                request['generation'] = generation
        else:
            api_url = self.account_info.get_api_url()
            account_auth_token = self.account_info.get_account_auth_token()
        return raw_api_method(api_url, account_auth_token, *args, **kwargs)

    def _api_token_only_callback(self, raw_api_method, *args, **kwargs):
        account_auth_token = self.account_info.get_account_auth_token()
        return raw_api_method(account_auth_token, *args, **kwargs)

    def _execute_with_auth_retry(self, callback, first_attempt: bool = True):
        try:
            return callback()
        except InvalidAuthToken:
            if first_attempt and self.authorize_automatically():
                return self._execute_with_auth_retry(callback, first_attempt=False)
            raise
        except Unauthorized as exc:
            # When performing HEAD requests, the api can return an empty response
            # with a 401 status code, which may or may not be related to expired
            # token, thus we also try to re-authorize if explicit `unauthorized`
            # code is missing
            if not exc.code and first_attempt and self.authorize_automatically():
                return self._execute_with_auth_retry(callback, first_attempt=False)
            raise self._add_app_key_info_to_unauthorized(exc)

    def _add_app_key_info_to_unauthorized(self, unauthorized):
        """
        Take an Unauthorized error and adds information from the application key
        about why it might have failed.
        """
        # What's allowed?
        allowed = self.account_info.get_allowed()
        capabilities = allowed['capabilities']
        name_prefix = allowed['namePrefix']

        # Make a list of messages about the application key restrictions
        key_messages = []
        if set(capabilities) != set(ALL_CAPABILITIES):
            key_messages.append("with capabilities '" + ','.join(capabilities) + "'")

        allowed_buckets_msg = self._get_allowed_buckets_message(allowed)
        if allowed_buckets_msg:
            key_messages.append(allowed_buckets_msg)

        if name_prefix is not None:
            key_messages.append("restricted to files that start with '" + name_prefix + "'")
        if not key_messages:
            key_messages.append('with no restrictions')

        # Make a new message
        new_message = unauthorized.message or 'unauthorized'
        new_message += ' for application key ' + ', '.join(key_messages)

        return Unauthorized(new_message, unauthorized.code)

    def _get_allowed_buckets_message(self, allowed) -> str | None:
        buckets = allowed['buckets']
        if not buckets:
            return None

        bucket_names = [b['name'] for b in buckets]

        return f'restricted to buckets {bucket_names}'

    def _get_new_upload_data(self, get_upload_url, raw_api_method, resource_id):
        # Keep public getter overrides active. A ContextVar isolates concurrent and
        # nested requests; auth retries replace the snapshot with the attempt used.
        request = {
            'method': raw_api_method,
            'generation': self.account_info._get_upload_pool_generation(),
        }
        context_token = _upload_url_request.set(request)
        try:
            response = get_upload_url(resource_id)
        finally:
            _upload_url_request.reset(context_token)
        return response['uploadUrl'], response['authorizationToken'], request['generation']

    def _get_upload_data(self, bucket_id, generation=None):
        """Take an upload URL and its acquisition generation for the bucket."""
        upload_url, upload_auth_token, generation = self.account_info._take_upload_url_tagged(
            bucket_id, generation
        )
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token, generation
        return self._get_new_upload_data(
            self.get_upload_url, self.raw_api.get_upload_url, bucket_id
        )

    def _get_upload_part_data(self, file_id, generation=None):
        """Take an upload URL and its acquisition generation for a large file."""
        upload_url, upload_auth_token, generation = self.account_info._take_upload_url_tagged(
            file_id, generation, large_file=True
        )
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token, generation
        return self._get_new_upload_data(
            self.get_upload_part_url, self.raw_api.get_upload_part_url, file_id
        )

    def _upload_small(self, f, bucket_id, *args, **kwargs):
        generation = self.account_info._get_upload_pool_generation()
        upload_url, upload_auth_token, generation = self._get_upload_data(bucket_id, generation)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self.account_info._put_upload_url_tagged(
            bucket_id, upload_url, upload_auth_token, generation
        )
        return response

    def _upload_part(self, f, file_id, *args, **kwargs):
        generation = self.account_info._get_upload_pool_generation()
        upload_url, upload_auth_token, generation = self._get_upload_part_data(file_id, generation)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self.account_info._put_upload_url_tagged(
            file_id, upload_url, upload_auth_token, generation, large_file=True
        )
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

    def get_bucket_notification_rules(self, bucket_id):
        return self._wrap_default_token(self.raw_api.get_bucket_notification_rules, bucket_id)

    def set_bucket_notification_rules(self, bucket_id, rules):
        return self._wrap_default_token(
            self.raw_api.set_bucket_notification_rules, bucket_id, rules
        )
