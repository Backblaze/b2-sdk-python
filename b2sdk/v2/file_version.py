######################################################################
#
# File: b2sdk/v2/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from typing import TYPE_CHECKING

from b2sdk.v2 import EncryptionSetting
from b2sdk.v2 import NO_RETENTION_FILE_SETTING, FileRetentionSetting, LegalHold
from b2sdk.v2 import ReplicationStatus

from b2sdk import _v3 as v3

if TYPE_CHECKING:
    from .api import B2Api

UNVERIFIED_CHECKSUM_PREFIX = 'unverified:'


class FileVersion(v3.FileVersion):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.id_: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar ~.size: size in bytes, can be ``None`` (unknown)
    :ivar str ~.content_type: RFC 822 content type, for example ``"application/octet-stream"``
    :ivar ~.upload_timestamp: in milliseconds since :abbr:`epoch (1970-01-01 00:00:00)`. Can be ``None`` (unknown).
    :ivar str ~.action: ``"upload"``, ``"hide"`` or ``"delete"``
    """

    __slots__ = ['cache_control']

    def __init__(
        self,
        api: B2Api,
        id_: str,
        file_name: str,
        size: int | None | str,
        content_type: str | None,
        content_sha1: str | None,
        file_info: dict[str, str],
        upload_timestamp: int,
        account_id: str,
        bucket_id: str,
        action: str,
        content_md5: str | None,
        server_side_encryption: EncryptionSetting,
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
        replication_status: ReplicationStatus | None = None,
        cache_control: str | None = None,
    ):
        self.cache_control = cache_control
        if self.cache_control is None:
            self.cache_control = (file_info or {}).get('b2-cache-control')

        super().__init__(
            api=api,
            id_=id_,
            file_name=file_name,
            size=size,
            content_type=content_type,
            content_sha1=content_sha1,
            file_info=file_info,
            upload_timestamp=upload_timestamp,
            account_id=account_id,
            bucket_id=bucket_id,
            action=action,
            content_md5=content_md5,
            server_side_encryption=server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            replication_status=replication_status,
        )

    def as_dict(self):
        result = super().as_dict()
        if self.cache_control is not None:
            result['cacheControl'] = self.cache_control
        return result


class FileVersionFactory(v3.FileVersionFactory):
    FILE_VERSION_CLASS = FileVersion
