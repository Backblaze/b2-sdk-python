######################################################################
#
# File: b2sdk/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional

from .encryption.setting import EncryptionSetting, EncryptionSettingFactory
from .file_lock import FileRetentionSetting, LegalHold


class FileVersionInfo(object):
    """
    A structure which represents a version of a file (in B2 cloud).

    :ivar str ~.id\_: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    :ivar ~.size: size in bytes, can be ``None`` (unknown)
    :vartype ~.size: int or None
    :ivar str ~.content_type: RFC 822 content type, for example ``"application/octet-stream"``
    :ivar ~.content_sha1: sha1 checksum of the entire file, can be ``None`` (unknown) if it is a large file uploaded by a client which did not provide it
    :vartype ~.content_sha1: str or None
    :ivar ~.content_md5: md5 checksum of the file, can be ``None`` (unknown)
    :vartype ~.content_md5: str or None
    :ivar dict ~.file_info: file info dict
    :ivar ~.upload_timestamp: in milliseconds since :abbr:`epoch (1970-01-01 00:00:00)`. Can be ``None`` (unknown).
    :vartype ~.upload_timestamp: int or None
    :ivar str ~.action: ``"upload"``, ``"hide"`` or ``"delete"``
    """

    __slots__ = [
        'id_',
        'file_name',
        'size',
        'content_type',
        'content_sha1',
        'content_md5',
        'file_info',
        'upload_timestamp',
        'action',
        'server_side_encryption',
        'legal_hold',
        'file_retention',
    ]

    def __init__(
        self,
        id_,
        file_name,
        size,
        content_type,
        content_sha1,
        file_info,
        upload_timestamp,
        action,
        content_md5=None,
        server_side_encryption: Optional[EncryptionSetting] = None,  # TODO: make it mandatory in v2
        file_retention: Optional[
            FileRetentionSetting
        ] = None,  # TODO: in v2 change the default value to NO_RETENTION_FILE_SETTING
        legal_hold: Optional[LegalHold
                            ] = None,  # TODO: in v2 change the default value to LegalHold.UNSET
    ):
        self.id_ = id_
        self.file_name = file_name
        self.size = size
        self.content_type = content_type
        self.content_sha1 = content_sha1
        self.content_md5 = content_md5
        self.file_info = file_info or {}
        self.upload_timestamp = upload_timestamp
        self.action = action
        self.server_side_encryption = server_side_encryption
        self.legal_hold = legal_hold
        self.file_retention = file_retention

    def as_dict(self):
        """ represents the object as a dict which looks almost exactly like the raw api output for upload/list """
        result = {
            'fileId': self.id_,
            'fileName': self.file_name,
            'fileInfo': self.file_info,
            'legalHold': self.legal_hold.to_dict_repr() if self.legal_hold is not None else None,
        }

        if self.size is not None:
            result['size'] = self.size
        if self.upload_timestamp is not None:
            result['uploadTimestamp'] = self.upload_timestamp
        if self.action is not None:
            result['action'] = self.action
        if self.content_type is not None:
            result['contentType'] = self.content_type
        if self.content_sha1 is not None:
            result['contentSha1'] = self.content_sha1
        if self.content_md5 is not None:
            result['contentMd5'] = self.content_md5
        if self.server_side_encryption is not None:  # this is for backward compatibility of interface only, b2sdk always sets it
            result['serverSideEncryption'] = self.server_side_encryption.as_dict()
        if self.file_retention is not None:  # this is for backward compatibility of interface only, b2sdk always sets it
            result['fileRetention'] = self.file_retention.as_dict()
        return result

    def __eq__(self, other):
        sentry = object()
        for attr in self.__slots__:
            if getattr(self, attr) != getattr(other, attr, sentry):
                return False
        return True


class FileVersionInfoFactory(object):
    """
    Construct :py:class:`b2sdk.v1.FileVersionInfo` objects from various structures.
    """

    @classmethod
    def from_api_response(cls, file_info_dict, force_action=None):
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
        assert file_info_dict.get('action') is None or force_action is None, \
            'action was provided by both info_dict and function argument'
        action = file_info_dict.get('action') or force_action
        file_name = file_info_dict['fileName']
        id_ = file_info_dict['fileId']
        if 'size' in file_info_dict:
            size = file_info_dict['size']
        elif 'contentLength' in file_info_dict:
            size = file_info_dict['contentLength']
        else:
            raise ValueError('no size or contentLength')
        upload_timestamp = file_info_dict.get('uploadTimestamp')
        content_type = file_info_dict.get('contentType')
        content_sha1 = file_info_dict.get('contentSha1')
        content_md5 = file_info_dict.get('contentMd5')
        file_info = file_info_dict.get('fileInfo')
        server_side_encryption = EncryptionSettingFactory.from_file_version_dict(file_info_dict)
        file_retention = FileRetentionSetting.from_file_version_dict(file_info_dict)

        legal_hold = LegalHold.from_file_version_dict(file_info_dict)

        return FileVersionInfo(
            id_,
            file_name,
            size,
            content_type,
            content_sha1,
            file_info,
            upload_timestamp,
            action,
            content_md5,
            server_side_encryption,
            file_retention,
            legal_hold,
        )

    @classmethod
    def from_cancel_large_file_response(cls, response):
        return FileVersionInfo(
            response['fileId'],
            response['fileName'],
            0,  # size
            'unknown',
            'none',
            {},
            0,  # upload timestamp
            'cancel'
        )

    @classmethod
    def from_response_headers(cls, headers):
        return FileVersionInfo(
            id_=headers.get('x-bz-file-id'),
            file_name=headers.get('x-bz-file-name'),
            size=headers.get('content-length'),
            content_type=headers.get('content-type'),
            content_sha1=headers.get('x-bz-content-sha1'),
            file_info=None,
            upload_timestamp=headers.get('x-bz-upload-timestamp'),
            action=None,
            server_side_encryption=EncryptionSettingFactory.from_response_headers(headers),
            file_retention=FileRetentionSetting.from_response_headers(headers),
            legal_hold=LegalHold.from_response_headers(headers),
        )


class FileIdAndName(object):
    """
    A structure which represents a B2 cloud file with just `file_name` and `fileId` attributes.

    Used to return data from calls to :py:meth:`b2sdk.v1.Bucket.delete_file_version`.

    :ivar str ~.file_id: ``fileId``
    :ivar str ~.file_name: full file name (with path)
    """

    def __init__(self, file_id, file_name):
        self.file_id = file_id
        self.file_name = file_name

    def as_dict(self):
        """ represents the object as a dict which looks almost exactly like the raw api output for delete_file_version """
        return {'action': 'delete', 'fileId': self.file_id, 'fileName': self.file_name}
