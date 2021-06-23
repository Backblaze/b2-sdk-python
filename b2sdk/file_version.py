######################################################################
#
# File: b2sdk/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Dict, Optional, Union, TYPE_CHECKING

from .encryption.setting import EncryptionSetting, EncryptionSettingFactory
from .http_constants import FILE_INFO_HEADER_PREFIX_LOWER, SRC_LAST_MODIFIED_MILLIS
from .file_lock import FileRetentionSetting, LegalHold, NO_RETENTION_FILE_SETTING
from .utils.range_ import Range

if TYPE_CHECKING:
    from .api import B2Api


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
        'file_info',
        'upload_timestamp',
        'server_side_encryption',
        'legal_hold',
        'file_retention',
        'mod_time_millis',
    ]

    def __init__(
        self,
        api: 'B2Api',
        id_: str,
        file_name: str,
        size: int,
        content_type: Optional[str],
        content_sha1: Optional[str],
        file_info: Dict[str, str],
        upload_timestamp: int,
        server_side_encryption: EncryptionSetting,
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
    ):
        self.api = api
        self.id_ = id_
        self.file_name = file_name
        self.size = size
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.file_info = file_info or {}
        self.upload_timestamp = upload_timestamp
        self.server_side_encryption = server_side_encryption
        self.legal_hold = legal_hold
        self.file_retention = file_retention

        if SRC_LAST_MODIFIED_MILLIS in self.file_info:
            self.mod_time_millis = int(self.file_info[SRC_LAST_MODIFIED_MILLIS])
        else:
            self.mod_time_millis = self.upload_timestamp

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
            result['contentSha1'] = self.content_sha1

        return result

    def __eq__(self, other):
        sentry = object()
        for attr in self._all_slots():
            if getattr(self, attr) != getattr(other, attr, sentry):
                return False
        return True

    def __repr__(self):
        return '%s(%s)' % (
            self.__class__.__name__,
            ', '.join(repr(getattr(self, attr)) for attr in self._all_slots())
        )

    def _all_slots(self):
        """Return all slots for an object (for it's class and all parent classes). Useful in auxiliary methods."""
        all_slots = []
        for klass in self.__class__.__mro__[-1::-1]:
            all_slots.extend(getattr(klass, '__slots__', []))
        return all_slots


class FileVersion(BaseFileVersion):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.id\_: ``fileId``
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

    def __init__(
        self,
        api: 'B2Api',
        id_: str,
        file_name: str,
        size: Union[int, None, str],
        content_type: Optional[str],
        content_sha1: Optional[str],
        file_info: Dict[str, str],
        upload_timestamp: int,
        account_id: str,
        bucket_id: str,
        action: str,
        content_md5: Optional[str],
        server_side_encryption: EncryptionSetting,
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
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
        )

    def as_dict(self):
        result = super().as_dict()
        result['accountId'] = self.account_id
        result['bucketId'] = self.bucket_id

        if self.action is not None:
            result['action'] = self.action
        if self.content_md5 is not None:
            result['contentMd5'] = self.content_md5

        return result

    def get_fresh_state(self) -> 'FileVersion':
        """
        Fetch all the information about this file version and return a new FileVersion object.
        This method does NOT change the object it is called on.
        """
        return self.api.get_file_info(self.id_)


class DownloadVersion(BaseFileVersion):
    """
    A structure which represents metadata of an initialized download
    """
    __slots__ = [
        'range_',
        'content_disposition',
        'content_length',
        'content_language',
        '_expires',
        '_cache_control',
        'content_encoding',
    ]

    def __init__(
        self,
        api: 'B2Api',
        id_: str,
        file_name: str,
        size: int,
        content_type: Optional[str],
        content_sha1: Optional[str],
        file_info: Dict[str, str],
        upload_timestamp: int,
        server_side_encryption: EncryptionSetting,
        range_: Range,
        content_disposition: Optional[str],
        content_length: int,
        content_language: Optional[str],
        expires,
        cache_control,
        content_encoding: Optional[str],
        file_retention: FileRetentionSetting = NO_RETENTION_FILE_SETTING,
        legal_hold: LegalHold = LegalHold.UNSET,
    ):
        self.range_ = range_
        self.content_disposition = content_disposition
        self.content_length = content_length
        self.content_language = content_language
        self._expires = expires  # TODO: parse the string representation of this timestamp to datetime in DownloadVersionFactory
        self._cache_control = cache_control  # TODO: parse the string representation of this mapping to dict in DownloadVersionFactory
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
        )


class FileVersionFactory(object):
    """
    Construct :py:class:`b2sdk.v1.FileVersion` objects from api responses.
    """

    def __init__(self, api: 'B2Api'):
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
               "uploadTimestamp": 1451444477000
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
               "serverSideEncryption": {"algorithm": "AES256", "mode": "SSE-B2"}
           }

        into a :py:class:`b2sdk.v1.FileVersionInfo` object.

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

        return FileVersion(
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
        )


class DownloadVersionFactory(object):
    """
    Construct :py:class:`b2sdk.v1.DownloadVersion` objects from download headers.
    """

    def __init__(self, api: 'B2Api'):
        self.api = api

    @classmethod
    def range_and_size_from_header(cls, header):
        raw_range, raw_size = header.split('/')
        range_ = Range.from_header(raw_range)
        size = int(raw_size)

        return range_, size

    @classmethod
    def file_info_from_headers(cls, headers: dict) -> dict:
        file_info = {}
        prefix_len = len(FILE_INFO_HEADER_PREFIX_LOWER)
        for header_name, header_value in headers.items():
            if header_name[:prefix_len].lower() == FILE_INFO_HEADER_PREFIX_LOWER:
                file_info_key = header_name[prefix_len:]
                file_info[file_info_key] = header_value
        return file_info

    def from_response_headers(self, headers):
        file_info = self.file_info_from_headers(headers)
        if 'Content-Range' in headers:
            range_, size = self.range_and_size_from_header(headers['Content-Range'])
            content_length = int(headers['Content-Length'])
        else:
            size = content_length = int(headers['Content-Length'])
            range_ = Range(0, max(size - 1, 0))

        return DownloadVersion(
            api=self.api,
            id_=headers['x-bz-file-id'],
            file_name=headers['x-bz-file-name'],
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
        )


class FileIdAndName(object):
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
        return '%s(%s, %s)' % (self.__class__.__name__, repr(self.file_id), repr(self.file_name))
