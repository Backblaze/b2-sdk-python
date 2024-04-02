######################################################################
#
# File: b2sdk/v2/bucket.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import typing

from b2sdk import _v3 as v3
from b2sdk._v3.exception import BucketIdNotFound as v3BucketIdNotFound
from b2sdk.v2._compat import _file_infos_rename
from .exception import BucketIdNotFound
from .file_version import FileVersionFactory

if typing.TYPE_CHECKING:
    from b2sdk._internal.utils import Sha1HexDigest


# Overridden to raise old style BucketIdNotFound exception
class Bucket(v3.Bucket):

    FILE_VERSION_FACTORY_CLASS = staticmethod(FileVersionFactory)

    def get_fresh_state(self) -> Bucket:
        try:
            return super().get_fresh_state()
        except v3BucketIdNotFound as e:
            raise BucketIdNotFound(e.bucket_id)

    @_file_infos_rename
    def upload_bytes(
        self,
        data_bytes,
        file_name,
        content_type=None,
        file_info: dict | None = None,
        progress_listener=None,
        encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs
    ):
        return super().upload_bytes(
            data_bytes,
            file_name,
            content_type,
            file_info,
            progress_listener,
            encryption,
            file_retention,
            legal_hold,
            large_file_sha1,
            custom_upload_timestamp,
            cache_control,
            *args,
            **kwargs,
        )

    @_file_infos_rename
    def upload_local_file(
        self,
        local_file,
        file_name,
        content_type=None,
        file_info: dict | None = None,
        sha1_sum=None,
        min_part_size=None,
        progress_listener=None,
        encryption: v3.EncryptionSetting | None = None,
        file_retention: v3.FileRetentionSetting | None = None,
        legal_hold: v3.LegalHold | None = None,
        upload_mode: v3.UploadMode = v3.UploadMode.FULL,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        *args,
        **kwargs
    ):
        return super().upload_local_file(
            local_file,
            file_name,
            content_type,
            file_info,
            sha1_sum,
            min_part_size,
            progress_listener,
            encryption,
            file_retention,
            legal_hold,
            upload_mode,
            custom_upload_timestamp,
            cache_control,
            *args,
            **kwargs,
        )


# Overridden to use old style Bucket
class BucketFactory(v3.BucketFactory):
    BUCKET_CLASS = staticmethod(Bucket)
