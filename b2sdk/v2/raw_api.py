######################################################################
#
# File: b2sdk/v2/raw_api.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations
from abc import abstractmethod

from b2sdk import v3
from b2sdk.v2._compat import _file_infos_rename

API_VERSION = 'v3'


class _OldRawAPI:
    """RawAPI compatibility layer"""

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

    def unprintable_to_hex(self, s):
        return v3.unprintable_to_hex(s)

    @_file_infos_rename
    def upload_file(
        self,
        upload_url,
        upload_auth_token,
        file_name,
        content_length,
        content_type,
        content_sha1,
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

    def get_download_url_by_id(self, download_url, file_id):
        return f'{download_url}/b2api/{API_VERSION}/b2_download_file_by_id?fileId={file_id}'


class AbstractRawApi(_OldRawAPI, v3.AbstractRawApi):
    @abstractmethod
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
        pass


class B2RawHTTPApi(_OldRawAPI, v3.B2RawHTTPApi):
    API_VERSION = API_VERSION

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
        return self._post_json(
            api_url,
            'b2_create_key',
            account_auth_token,
            accountId=account_id,
            capabilities=capabilities,
            keyName=key_name,
            validDurationInSeconds=valid_duration_seconds,
            bucketId=bucket_id,
            namePrefix=name_prefix,
        )
