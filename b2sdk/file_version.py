######################################################################
#
# File: b2sdk/file_version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import datetime


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
    LS_ENTRY_TEMPLATE = '%83s  %6s  %10s  %8s  %9d  %s'  # order is file_id, action, date, time, size, name

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

    def as_dict(self):
        """ represents the object as a dict which looks almost exactly like the raw api output for upload/list """
        result = {
            'fileId': self.id_,
            'fileName': self.file_name,
            'fileInfo': self.file_info,
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
        return result

    def format_ls_entry(self):
        """ legacy method, to be removed in v2: formats a `ls` entry for b2 command line tool """
        dt = datetime.datetime.utcfromtimestamp(self.upload_timestamp / 1000)
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M:%S')
        size = self.size or 0  # required if self.action == 'hide'
        return self.LS_ENTRY_TEMPLATE % (
            self.id_,
            self.action,
            date_str,
            time_str,
            size,
            self.file_name,
        )

    @classmethod
    def format_folder_ls_entry(cls, name):
        """ legacy method, to be removed in v2: formats a `ls` "folder" consistently with format_ls_entry() """
        return cls.LS_ENTRY_TEMPLATE % ('-', '-', '-', '-', 0, name)


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
               "fileName": "randomdata"
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
            action=None
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
