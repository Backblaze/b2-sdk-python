######################################################################
#
# File: b2sdk/_internal/large_file/unfinished_large_file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime as dt

from b2sdk._internal.encryption.setting import EncryptionSettingFactory
from b2sdk._internal.file_lock import FileRetentionSetting, LegalHold
from b2sdk._internal.utils.http_date import parse_http_date


class UnfinishedLargeFile:
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.file_id: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar str ~.account_id: account ID
    :ivar str ~.bucket_id: bucket ID
    :ivar str ~.content_type: :rfc:`822` content type, for example ``"application/octet-stream"``
    :ivar dict ~.file_info: file info dict
    """

    def __init__(self, file_dict):
        """
        Initialize from one file returned by ``b2_start_large_file`` or ``b2_list_unfinished_large_files``.
        """
        self.file_id = file_dict['fileId']
        self.file_name = file_dict['fileName']
        self.account_id = file_dict['accountId']
        self.bucket_id = file_dict['bucketId']
        self.content_type = file_dict['contentType']
        self.file_info = file_dict['fileInfo']
        self.encryption = EncryptionSettingFactory.from_file_version_dict(file_dict)
        self.file_retention = FileRetentionSetting.from_file_version_dict(file_dict)
        self.legal_hold = LegalHold.from_file_version_dict(file_dict)

    @property
    def cache_control(self) -> str | None:
        return (self.file_info or {}).get('b2-cache-control')

    @property
    def expires(self) -> str | None:
        return (self.file_info or {}).get('b2-expires')

    def expires_parsed(self) -> dt.datetime | None:
        """Return the expiration date as a datetime object, or None if there is no expiration date.
        Raise ValueError if `expires` property is not a valid HTTP-date."""

        if self.expires is None:
            return None
        return parse_http_date(self.expires)

    @property
    def content_disposition(self) -> str | None:
        return (self.file_info or {}).get('b2-content-disposition')

    @property
    def content_encoding(self) -> str | None:
        return (self.file_info or {}).get('b2-content-encoding')

    @property
    def content_language(self) -> str | None:
        return (self.file_info or {}).get('b2-content-language')

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.bucket_id} {self.file_name}>'

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self == other)
