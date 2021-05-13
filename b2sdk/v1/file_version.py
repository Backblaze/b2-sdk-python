######################################################################
#
# File: b2sdk/v1/file_version.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import datetime
import functools

from b2sdk import _v2 as v2


# override to retain old formatting methods
class FileVersionInfo(v2.FileVersionInfo):
    LS_ENTRY_TEMPLATE = '%83s  %6s  %10s  %8s  %9d  %s'  # order is file_id, action, date, time, size, name

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


def file_version_info_from_new_file_version_info(
    file_version: v2.FileVersionInfo
) -> FileVersionInfo:
    return FileVersionInfo(
        **{att_name: getattr(file_version, att_name)
           for att_name in FileVersionInfo.__slots__}
    )


def translate_single_file_version(func):
    @functools.wraps(func)
    def inner(*a, **kw):
        return file_version_info_from_new_file_version_info(func(*a, **kw))

    return inner


# override to return old style FileVersionInfo
class FileVersionInfoFactory(v2.FileVersionInfoFactory):

    from_api_response = translate_single_file_version(v2.FileVersionInfoFactory.from_api_response)
    from_cancel_large_file_response = translate_single_file_version(
        v2.FileVersionInfoFactory.from_cancel_large_file_response
    )
    from_response_headers = translate_single_file_version(
        v2.FileVersionInfoFactory.from_response_headers
    )
