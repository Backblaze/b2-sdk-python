######################################################################
#
# File: b2sdk/_internal/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime as dt
import re
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from .encryption.setting import EncryptionSetting, EncryptionSettingFactory
from .file_lock import NO_RETENTION_FILE_SETTING, FileRetentionSetting, LegalHold
from .http_constants import FILE_INFO_HEADER_PREFIX_LOWER, LARGE_FILE_SHA1, SRC_LAST_MODIFIED_MILLIS
from .progress import AbstractProgressListener
from .replication.types import ReplicationStatus
from .utils import Sha1HexDigest, b2_url_decode
from .utils.http_date import parse_http_date
from .utils.range_ import EMPTY_RANGE, Range

if TYPE_CHECKING:
    from .api import B2Api
    from .transfer.inbound.downloaded_file import DownloadedFile

UNVERIFIED_CHECKSUM_PREFIX = 'unverified:'


class BaseFileVersion:
    """
    Base class for representing file metadata in B2 cloud.

    :ivar size - size of the whole file (for "upload" markers)
    """
    __slots__ = [
        'id_',
        'api',
        'file_name',
        'size',
        'content_type',
        'content_sha1',
        'content_sha1_verified',
        'file_info',
        'upload_timestamp',
        'server_side_encryption',
        'legal_hold',
        'file_retention',
        'mod_time_millis',
        'replication_status',
    ]
    _TYPE_MATCHER = re.compile('[a-z0-9]+_[a-z0-9]+_f([0-9]).*')
    _FILE_TYPE = {
        1: 'small',
        2: 'large',
        3: 'part',
        4: 'tiny',
    }

    def __init__(
        self,
        api: B2Api,
        id_: str,
        file_name: str,
        size: int,
        content_type: str | None,
        content_sha1: str | None,
        file_info: dict[str, str] | None,
        upload_timestamp: int,
        server_side_encryption: EncryptionSetting,
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
        replication_status: ReplicationStatus | None = None,
    ):
        self.api = api
        self.id_ = id_
        self.file_name = file_name
        self.size = size
        self.content_type = content_type
        self.content_sha1, self.content_sha1_verified = self._decode_content_sha1(content_sha1)
        self.file_info = file_info or {}
        self.upload_timestamp = upload_timestamp
        self.server_side_encryption = server_side_encryption
        self.file_retention = file_retention
        self.legal_hold = legal_hold
        self.replication_status = replication_status

        if SRC_LAST_MODIFIED_MILLIS in self.file_info:
            self.mod_time_millis = int(self.file_info[SRC_LAST_MODIFIED_MILLIS])
        else:
            self.mod_time_millis = self.upload_timestamp

    @classmethod
    def _decode_content_sha1(cls, content_sha1):
        if content_sha1.startswith(UNVERIFIED_CHECKSUM_PREFIX):
            return content_sha1[len(UNVERIFIED_CHECKSUM_PREFIX):], False
        return content_sha1, True

    @classmethod
    def _encode_content_sha1(cls, content_sha1, content_sha1_verified):
        if not content_sha1_verified:
            return f'{UNVERIFIED_CHECKSUM_PREFIX}{content_sha1}'
        return content_sha1

    def _clone(self, **new_attributes: Any):
        """
        Create new instance based on the old one, overriding attributes with :code:`new_attributes`
        (only applies to arguments passed to __init__)
        """
        args = self._get_args_for_clone()
        return self.__class__(**{**args, **new_attributes})

    def _get_args_for_clone(self):
        return {
            'api': self.api,
            'id_': self.id_,
            'file_name': self.file_name,
            'size': self.size,
            'content_type': self.content_type,
            'content_sha1': self._encode_content_sha1(self.content_sha1, self.content_sha1_verified),
            'file_info': self.file_info,
            'upload_timestamp': self.upload_timestamp,
            'server_side_encryption': self.server_side_encryption,
            'file_retention': self.file_retention,
            'legal_hold': self.legal_hold,
            'replication_status': self.replication_status,
        }  # yapf: disable

    def as_dict(self):
        """ represents the object as a dict which looks almost exactly like the raw api output for upload/list """
        result = {
            'fileId': self.id_,
            'fileName': self.file_name,
            'fileInfo': self.file_info,
            'serverSideEncryption': self.server_side_encryption.as_dict(),
            'legalHold': self.legal_hold.value,
            'fileRetention': self.file_retention.as_dict(),
        }

        if self.size is not None:
            result['size'] = self.size
        if self.upload_timestamp is not None:
            result['uploadTimestamp'] = self.upload_timestamp
        if self.content_type is not None:
            result['contentType'] = self.content_type
        if self.content_sha1 is not None:
            result['contentSha1'] = self._encode_content_sha1(
                self.content_sha1, self.content_sha1_verified
            )
        result['replicationStatus'] = self.replication_status and self.replication_status.value

        return result

    def __eq__(self, other):
        sentry = object()
        for attr in self._all_slots():
            if getattr(self, attr) != getattr(other, attr, sentry):
                return False
        return True

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join(repr(getattr(self, attr)) for attr in self._all_slots())
        )

    def _all_slots(self):
        """Return all slots for an object (for it's class and all parent classes). Useful in auxiliary methods."""
        all_slots = []
        for klass in self.__class__.__mro__[-1::-1]:
            all_slots.extend(getattr(klass, '__slots__', []))
        return all_slots

    def delete(self, bypass_governance: bool = False) -> FileIdAndName:
        """Delete this file version. bypass_governance must be set to true if deleting a file version protected by
        Object Lock governance mode retention settings (unless its retention period expired)"""
        return self.api.delete_file_version(self.id_, self.file_name, bypass_governance)

    def update_legal_hold(self, legal_hold: LegalHold) -> BaseFileVersion:
        legal_hold = self.api.update_file_legal_hold(self.id_, self.file_name, legal_hold)
        return self._clone(legal_hold=legal_hold)

    def update_retention(
        self,
        file_retention: FileRetentionSetting,
        bypass_governance: bool = False,
    ) -> BaseFileVersion:
        file_retention = self.api.update_file_retention(
            self.id_, self.file_name, file_retention, bypass_governance
        )
        return self._clone(file_retention=file_retention)

    def _type(self):
        """
        FOR TEST PURPOSES ONLY
        not guaranteed to work for perpetuity (using undocumented server behavior)
        """
        m = self._TYPE_MATCHER.match(self.id_)
        assert m, self.id_
        return self._FILE_TYPE[int(m.group(1))]

    def get_content_sha1(self) -> Sha1HexDigest | None:
        """
        Get the file's content SHA1 hex digest from the header or, if its absent,
        from the file info.  If both are missing, return None.
        """
        if self.content_sha1 and self.content_sha1 != "none":
            return self.content_sha1
        elif LARGE_FILE_SHA1 in self.file_info:
            return Sha1HexDigest(self.file_info[LARGE_FILE_SHA1])
        # content SHA1 unknown
        return None


class FileVersion(BaseFileVersion):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.id_: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar ~.size: size in bytes, can be ``None`` (unknown)
    :ivar str ~.content_type: RFC 822 content type, for example ``"application/octet-stream"``
    :ivar ~.upload_timestamp: in milliseconds since :abbr:`epoch (1970-01-01 00:00:00)`. Can be ``None`` (unknown).
    :ivar str ~.action: ``"upload"``, ``"hide"`` or ``"delete"``
    """

    __slots__ = [
        'account_id',
        'bucket_id',
        'content_md5',
        'action',
    ]

    # defined at https://www.backblaze.com/b2/docs/files.html#httpHeaderSizeLimit
    DEFAULT_HEADERS_LIMIT = 7000
    ADVANCED_HEADERS_LIMIT = 2048

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
    ):
        self.account_id = account_id
        self.bucket_id = bucket_id
        self.content_md5 = content_md5
        self.action = action

        super().__init__(
            api=api,
            id_=id_,
            file_name=file_name,
            size=size,
            content_type=content_type,
            content_sha1=content_sha1,
            file_info=file_info,
            upload_timestamp=upload_timestamp,
            server_side_encryption=server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            replication_status=replication_status,
        )

    @property
    def cache_control(self) -> str | None:
        return self.file_info.get('b2-cache-control')

    @property
    def expires(self) -> str | None:
        return self.file_info.get('b2-expires')

    def expires_parsed(self) -> dt.datetime | None:
        """Return the expiration date as a datetime object, or None if there is no expiration date.
        Raise ValueError if `expires` property is not a valid HTTP-date."""

        if self.expires is None:
            return None
        return parse_http_date(self.expires)

    @property
    def content_disposition(self) -> str | None:
        return self.file_info.get('b2-content-disposition')

    @property
    def content_encoding(self) -> str | None:
        return self.file_info.get('b2-content-encoding')

    @property
    def content_language(self) -> str | None:
        return self.file_info.get('b2-content-language')

    def _get_args_for_clone(self):
        args = super()._get_args_for_clone()
        args.update(
            {
                'account_id': self.account_id,
                'bucket_id': self.bucket_id,
                'action': self.action,
                'content_md5': self.content_md5,
            }
        )
        return args

    def as_dict(self):
        result = super().as_dict()
        result['accountId'] = self.account_id
        result['bucketId'] = self.bucket_id

        if self.action is not None:
            result['action'] = self.action
        if self.content_md5 is not None:
            result['contentMd5'] = self.content_md5

        return result

    def get_fresh_state(self) -> FileVersion:
        """
        Fetch all the information about this file version and return a new FileVersion object.
        This method does NOT change the object it is called on.
        """
        return self.api.get_file_info(self.id_)

    def download(
        self,
        progress_listener: AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ) -> DownloadedFile:
        return self.api.download_file_by_id(
            self.id_,
            progress_listener=progress_listener,
            range_=range_,
            encryption=encryption,
        )

    def _get_upload_headers(self) -> bytes:
        """
        Return encoded http headers, as when sending an upload request to b2 http api.
        WARNING: the headers do not contain newlines between headers and spaces between
        key and value. This implementation is in par with ADVANCED_HEADERS_LIMIT
        and is reasonable only for `has_large_header` method
        """

        # sometimes secret is not available, but we want to calculate headers
        # size anyway; to bypass this, we use a fake encryption setting
        # with a fake key
        sse = self.server_side_encryption
        if sse and sse.key and sse.key.secret is None:
            sse = deepcopy(sse)
            sse.key.secret = b'*' * sse.algorithm.get_length()

        headers = self.api.raw_api.get_upload_file_headers(
            upload_auth_token=self.api.account_info.get_account_auth_token(),
            file_name=self.file_name,
            content_length=self.size,
            content_type=self.content_type,
            content_sha1=self.content_sha1,
            file_info=self.file_info,
            server_side_encryption=sse,
            file_retention=self.file_retention,
            legal_hold=self.legal_hold,
        )

        headers_str = ''.join(
            f'{key}{value}' for key, value in headers.items() if value is not None
        )
        return headers_str.encode('utf8')

    @property
    def has_large_header(self) -> bool:
        """
        Determine whether FileVersion's info fits header size limit defined by B2.
        This function makes sense only for "advanced" buckets, i.e. those which
        have Server-Side Encryption or File Lock enabled.

        See https://www.backblaze.com/b2/docs/files.html#httpHeaderSizeLimit.
        """
        return len(self._get_upload_headers()) > self.ADVANCED_HEADERS_LIMIT


class DownloadVersion(BaseFileVersion):
    """
    A structure which represents metadata of an initialized download
    """
    __slots__ = [
        'range_',
        'content_disposition',
        'content_length',
        'content_language',
        'expires',
        'cache_control',
        'content_encoding',
    ]

    def __init__(
        self,
        api: B2Api,
        id_: str,
        file_name: str,
        size: int,
        content_type: str | None,
        content_sha1: str | None,
        file_info: dict[str, str],
        upload_timestamp: int,
        server_side_encryption: EncryptionSetting,
        range_: Range,
        content_disposition: str | None,
        content_length: int,
        content_language: str | None,
        expires: str | None,
        cache_control: str | None,
        content_encoding: str | None,
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
        replication_status: ReplicationStatus | None = None,
    ):
        self.range_ = range_
        self.content_disposition = content_disposition
        self.content_length = content_length
        self.content_language = content_language
        self.expires = expires
        self.cache_control = cache_control
        self.content_encoding = content_encoding

        super().__init__(
            api=api,
            id_=id_,
            file_name=file_name,
            size=size,
            content_type=content_type,
            content_sha1=content_sha1,
            file_info=file_info,
            upload_timestamp=upload_timestamp,
            server_side_encryption=server_side_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            replication_status=replication_status,
        )

    def expires_parsed(self) -> dt.datetime | None:
        """Return the expiration date as a datetime object, or None if there is no expiration date.
        Raise ValueError if `expires` property is not a valid HTTP-date."""

        if self.expires is None:
            return None
        return parse_http_date(self.expires)

    def as_dict(self) -> dict:
        result = super().as_dict()
        if self.cache_control is not None:
            result['cacheControl'] = self.cache_control
        if self.expires is not None:
            result['expires'] = self.expires
        if self.content_disposition is not None:
            result['contentDisposition'] = self.content_disposition
        if self.content_encoding is not None:
            result['contentEncoding'] = self.content_encoding
        if self.content_language is not None:
            result['contentLanguage'] = self.content_language
        return result

    def _get_args_for_clone(self):
        args = super()._get_args_for_clone()
        args.update(
            {
                'range_': self.range_,
                'content_disposition': self.content_disposition,
                'content_length': self.content_length,
                'content_language': self.content_language,
                'expires': self.expires,
                'cache_control': self.cache_control,
                'content_encoding': self.content_encoding,
            }
        )
        return args


class FileVersionFactory:
    """
    Construct :py:class:`b2sdk.v2.FileVersion` objects from api responses.
    """

    FILE_VERSION_CLASS = FileVersion

    def __init__(self, api: B2Api):
        self.api = api

    def from_api_response(self, file_version_dict, force_action=None):
        """
        Turn this:

        .. code-block:: python

           {
               "action": "hide",
               "fileId": "4_zBucketName_f103b7ca31313c69c_d20151230_m030117_c001_v0001015_t0000",
               "fileName": "randomdata",
               "size": 0,
               "uploadTimestamp": 1451444477000,
               "replicationStatus": "pending"
           }

        or this:

        .. code-block:: python

           {
               "accountId": "4aa9865d6f00",
               "bucketId": "547a2a395826655d561f0010",
               "contentLength": 1350,
               "contentSha1": "753ca1c2d0f3e8748320b38f5da057767029a036",
               "contentType": "application/octet-stream",
               "fileId": "4_z547a2a395826655d561f0010_f106d4ca95f8b5b78_d20160104_m003906_c001_v0001013_t0005",
               "fileInfo": {},
               "fileName": "randomdata",
               "serverSideEncryption": {"algorithm": "AES256", "mode": "SSE-B2"},
               "replicationStatus": "completed"
           }

        into a :py:class:`b2sdk.v2.FileVersion` object.

        """
        assert file_version_dict.get('action') is None or force_action is None, \
            'action was provided by both info_dict and function argument'
        action = file_version_dict.get('action') or force_action
        file_name = file_version_dict['fileName']
        id_ = file_version_dict['fileId']
        if 'size' in file_version_dict:
            size = file_version_dict['size']
        elif 'contentLength' in file_version_dict:
            size = file_version_dict['contentLength']
        else:
            raise ValueError('no size or contentLength')
        upload_timestamp = file_version_dict.get('uploadTimestamp')
        content_type = file_version_dict.get('contentType')
        content_sha1 = file_version_dict.get('contentSha1')
        content_md5 = file_version_dict.get('contentMd5')
        file_info = file_version_dict.get('fileInfo')
        server_side_encryption = EncryptionSettingFactory.from_file_version_dict(file_version_dict)
        file_retention = FileRetentionSetting.from_file_version_dict(file_version_dict)

        legal_hold = LegalHold.from_file_version_dict(file_version_dict)
        replication_status_value = file_version_dict.get('replicationStatus')
        replication_status = replication_status_value and ReplicationStatus[
            replication_status_value.upper()]

        return self.FILE_VERSION_CLASS(
            self.api,
            id_,
            file_name,
            size,
            content_type,
            content_sha1,
            file_info,
            upload_timestamp,
            file_version_dict['accountId'],
            file_version_dict['bucketId'],
            action,
            content_md5,
            server_side_encryption,
            file_retention,
            legal_hold,
            replication_status,
        )


class DownloadVersionFactory:
    """
    Construct :py:class:`b2sdk.v2.DownloadVersion` objects from download headers.
    """

    def __init__(self, api: B2Api):
        self.api = api

    @classmethod
    def range_and_size_from_header(cls, header: str) -> tuple[Range, int]:
        range_, size = Range.from_header_with_size(header)
        assert size is not None, 'Total length was expected in Content-Range header'
        return range_, size

    @classmethod
    def file_info_from_headers(cls, headers: dict) -> dict:
        file_info = {}
        prefix_len = len(FILE_INFO_HEADER_PREFIX_LOWER)
        for header_name, header_value in headers.items():
            if header_name[:prefix_len].lower() == FILE_INFO_HEADER_PREFIX_LOWER:
                file_info_key = header_name[prefix_len:]
                file_info[file_info_key] = b2_url_decode(header_value)
        return file_info

    def from_response_headers(self, headers):
        file_info = self.file_info_from_headers(headers)

        content_range_header_value = headers.get('Content-Range')
        if content_range_header_value:
            range_, size = self.range_and_size_from_header(content_range_header_value)
            content_length = int(headers['Content-Length'])
        else:
            size = content_length = int(headers['Content-Length'])
            range_ = Range(0, size - 1) if size else EMPTY_RANGE

        return DownloadVersion(
            api=self.api,
            id_=headers['x-bz-file-id'],
            file_name=b2_url_decode(headers['x-bz-file-name']),
            size=size,
            content_type=headers['content-type'],
            content_sha1=headers['x-bz-content-sha1'],
            file_info=file_info,
            upload_timestamp=int(headers['x-bz-upload-timestamp']),
            server_side_encryption=EncryptionSettingFactory.from_response_headers(headers),
            range_=range_,
            content_disposition=headers.get('Content-Disposition'),
            content_length=content_length,
            content_language=headers.get('Content-Language'),
            expires=headers.get('Expires'),
            cache_control=headers.get('Cache-Control'),
            content_encoding=headers.get('Content-Encoding'),
            file_retention=FileRetentionSetting.from_response_headers(headers),
            legal_hold=LegalHold.from_response_headers(headers),
            replication_status=ReplicationStatus.from_response_headers(headers),
        )


class FileIdAndName:
    """
    A structure which represents a B2 cloud file with just `file_name` and `fileId` attributes.

    Used to return data from calls to b2_delete_file_version and b2_cancel_large_file.
    """

    def __init__(self, file_id: str, file_name: str):
        self.file_id = file_id
        self.file_name = file_name

    @classmethod
    def from_cancel_or_delete_response(cls, response):
        return cls(response['fileId'], response['fileName'])

    def as_dict(self):
        """ represents the object as a dict which looks almost exactly like the raw api output for delete_file_version """
        return {'action': 'delete', 'fileId': self.file_id, 'fileName': self.file_name}

    def __eq__(self, other):
        return (self.file_id == other.file_id and self.file_name == other.file_name)

    def __repr__(self):
        return f'{self.__class__.__name__}({repr(self.file_id)}, {repr(self.file_name)})'
