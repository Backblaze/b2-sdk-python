######################################################################
#
# File: b2sdk/v1/sync/file.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.v1 import FileVersionInfo
from b2sdk._internal.http_constants import SRC_LAST_MODIFIED_MILLIS


# This whole module is here to retain legacy classes so they can be used in retained legacy exception
class File:
    """
    Hold information about one file in a folder.

    The name is relative to the folder in all cases.

    Files that have multiple versions (which only happens
    in B2, not in local folders) include information about
    all of the versions, most recent first.
    """

    __slots__ = ['name', 'versions']

    def __init__(self, name, versions: list[FileVersion]):
        """
        :param str name: a relative file name
        :param List[FileVersion] versions: a list of file versions
        """
        self.name = name
        self.versions = versions

    def latest_version(self) -> FileVersion:
        """
        Return the latest file version.
        """
        return self.versions[0]

    def __repr__(self):
        return '{}({}, [{}])'.format(
            self.__class__.__name__, self.name, ', '.join(repr(v) for v in self.versions)
        )


class B2File(File):
    """
    Hold information about one file in a folder in B2 cloud.
    """

    __slots__ = ['name', 'versions']

    def __init__(self, name, versions: list[FileVersion]):
        """
        :param str name: a relative file name
        :param List[FileVersion] versions: a list of file versions
        """
        super().__init__(name, versions)

    def latest_version(self) -> FileVersion:
        return super().latest_version()


class FileVersion:
    """
    Hold information about one version of a file.
    """

    __slots__ = ['id_', 'name', 'mod_time', 'action', 'size']

    def __init__(self, id_, file_name, mod_time, action, size):
        """
        :param id_: the B2 file id, or the local full path name
        :type id_: str
        :param file_name: a relative file name
        :type file_name: str
        :param mod_time: modification time, in milliseconds, to avoid rounding issues
                         with millisecond times from B2
        :type mod_time: int
        :param action: "hide" or "upload" (never "start")
        :type action: str
        :param size: a file size
        :type size: int
        """
        self.id_ = id_
        self.name = file_name
        self.mod_time = mod_time
        self.action = action
        self.size = size

    def __repr__(self):
        return '{}({}, {}, {}, {})'.format(
            self.__class__.__name__,
            repr(self.id_),
            repr(self.name),
            repr(self.mod_time),
            repr(self.action),
        )


class B2FileVersion(FileVersion):
    __slots__ = [
        'file_version_info'
    ]  # in a typical use case there is a lot of these object in memory, hence __slots__

    # and properties

    def __init__(self, file_version_info: FileVersionInfo):
        self.file_version_info = file_version_info

    @property
    def id_(self):
        return self.file_version_info.id_

    @property
    def name(self):
        return self.file_version_info.file_name

    @property
    def mod_time(self):
        if SRC_LAST_MODIFIED_MILLIS in self.file_version_info.file_info:
            return int(self.file_version_info.file_info[SRC_LAST_MODIFIED_MILLIS])
        return self.file_version_info.upload_timestamp

    @property
    def action(self):
        return self.file_version_info.action

    @property
    def size(self):
        return self.file_version_info.size
