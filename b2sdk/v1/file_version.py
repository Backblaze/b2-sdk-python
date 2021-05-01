######################################################################
#
# File: b2sdk/v1/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional
from b2sdk import _v2 as v2


# Override to retain legacy class name, __init__ signature and slots
class FileVersionInfo(v2.FileVersion):
    __slots__ = [
        'id_', 'file_name', 'size', 'content_type', 'content_sha1', 'content_md5', 'file_info',
        'upload_timestamp', 'action', 'server_side_encryption'
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
        server_side_encryption: Optional[v2.EncryptionSetting] = None,
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


def file_version_info_from_id_and_name(file_id_and_name: v2.FileIdAndName):
    return FileVersionInfo(
        id_=file_id_and_name.file_id,
        file_name=file_id_and_name.file_name,
        size=0,
        content_type='unknown',
        content_sha1='none',
        file_info={},
        upload_timestamp=0,
        action='cancel',
    )


def file_version_info_from_new_file_version(file_version: v2.FileVersion):
    return FileVersionInfo(
        **{att_name: getattr(file_version, att_name)
           for att_name in FileVersionInfo.__slots__}
    )


# override to return old style FileVersionInfo. The signature changes (an ignored "api" argument is added), but
# this class is not part of the public api
class FileVersionInfoFactory(object):
    """
    Construct :py:class:`b2sdk.v1.FileVersionInfo` objects from various structures.
    """

    @classmethod
    def from_api_response(cls, api, file_info_dict, force_action=None):
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
        server_side_encryption = v2.EncryptionSettingFactory.from_file_version_dict(file_info_dict)

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
        )

    @classmethod
    def from_cancel_large_file_response(cls, api, response):
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
    def from_response_headers(cls, api, headers):
        return FileVersionInfo(
            id_=headers.get('x-bz-file-id'),
            file_name=headers.get('x-bz-file-name'),
            size=headers.get('content-length'),
            content_type=headers.get('content-type'),
            content_sha1=headers.get('x-bz-content-sha1'),
            file_info=None,
            upload_timestamp=headers.get('x-bz-upload-timestamp'),
            action=None,
            server_side_encryption=v2.EncryptionSettingFactory.from_response_headers(headers),
        )
