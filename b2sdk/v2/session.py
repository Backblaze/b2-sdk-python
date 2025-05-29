######################################################################
#
# File: b2sdk/v2/session.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v3
from .b2http import B2Http

from .account_info import SqliteAccountInfo
from ._compat import _file_infos_rename
from . import api_config as _api_config
from .._internal import cache as _cache
from .._internal.account_info import abstract as _abstract


# Override to use legacy B2Http
class B2Session(v3.B2Session):
    SQLITE_ACCOUNT_INFO_CLASS = staticmethod(SqliteAccountInfo)
    B2HTTP_CLASS = staticmethod(B2Http)

    def __init__(
        self,
        account_info: _abstract.AbstractAccountInfo | None = None,
        cache: _cache.AbstractCache | None = None,
        api_config: _api_config.B2HttpApiConfig = _api_config.DEFAULT_HTTP_API_CONFIG,
    ):
        if account_info is not None and cache is None:
            # preserve legacy behavior https://github.com/Backblaze/b2-sdk-python/issues/497#issuecomment-2147461352
            cache = _cache.DummyCache()

        super().__init__(account_info, cache, api_config)

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

    @_file_infos_rename
    def upload_file(
        self,
        bucket_id,
        file_name,
        content_length,
        content_type,
        content_sha1,
        file_info,
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
            bucket_id,
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

    def _construct_allowed_dict(self, storage_api_info):
        return {
            'bucketId': storage_api_info['bucketId'],
            'bucketName': storage_api_info['bucketName'],
            'capabilities': storage_api_info['capabilities'],
            'namePrefix': storage_api_info['namePrefix'],
        }

    def _get_allowed_buckets_message(self, allowed) -> str | None:
        bucket_name = allowed['bucketName']
        if bucket_name is None:
            return None

        return "restricted to bucket '" + bucket_name + "'"
