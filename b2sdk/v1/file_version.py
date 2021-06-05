######################################################################
#
# File: b2sdk/v1/file_version.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from typing import Optional
import datetime
import functools

from b2sdk import _v2 as v2
from ..raw_api import SRC_LAST_MODIFIED_MILLIS
from . import api as v1api


# Override to retain legacy class name, __init__ signature, slots
# and old formatting methods
class FileVersionInfo(v2.FileVersion):
    __slots__ = ['_api']

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
        server_side_encryption: Optional[v2.EncryptionSetting] = None,
        file_retention: Optional[v2.FileRetentionSetting] = None,
        legal_hold: Optional[v2.LegalHold] = None,
        api: Optional['v1api.B2Api'] = None,
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
        self._api = api

        if SRC_LAST_MODIFIED_MILLIS in self.file_info:
            self.mod_time_millis = int(self.file_info[SRC_LAST_MODIFIED_MILLIS])
        else:
            self.mod_time_millis = self.upload_timestamp

    @property
    def api(self):
        if self._api is None:
            raise ValueError('"api" not set')
        return self._api

    def format_ls_entry(self):
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
        return cls.LS_ENTRY_TEMPLATE % ('-', '-', '-', '-', 0, name)


def file_version_info_from_new_file_version(file_version: v2.FileVersion) -> FileVersionInfo:
    return FileVersionInfo(
        **{
            att_name: getattr(file_version, att_name)
            for att_name in [
                'id_',
                'file_name',
                'size',
                'content_type',
                'content_sha1',
                'file_info',
                'upload_timestamp',
                'action',
                'content_md5',
                'server_side_encryption',
                'legal_hold',
                'file_retention',
                'api',
            ]
        }
    )


def translate_single_file_version(func):
    @functools.wraps(func)
    def inner(*a, **kw):
        return file_version_info_from_new_file_version(func(*a, **kw))

    return inner


# override to return old style FileVersionInfo
class FileVersionInfoFactory(v2.FileVersionFactory):

    from_api_response = translate_single_file_version(v2.FileVersionFactory.from_api_response)
    from_response_headers = translate_single_file_version(
        v2.FileVersionFactory.from_response_headers
    )


def file_version_info_from_id_and_name(file_id_and_name: v2.FileIdAndName, api: 'v1api.B2Api'):
    return FileVersionInfo(
        id_=file_id_and_name.file_id,
        file_name=file_id_and_name.file_name,
        size=0,
        content_type='unknown',
        content_sha1='none',
        file_info={},
        upload_timestamp=0,
        action='cancel',
        api=api,
    )
