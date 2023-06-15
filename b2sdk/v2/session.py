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

from b2sdk import _v3 as v3
from .b2http import B2Http

from ._compat import _file_infos_rename


# Override to use legacy B2Http
class B2Session(v3.B2Session):
    B2HTTP_CLASS = staticmethod(B2Http)

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
        **kwargs
    ):
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
            cache_control,
            *args,
            **kwargs,
        )
