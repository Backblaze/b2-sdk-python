######################################################################
#
# File: b2sdk/sync/file.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import ABC, abstractmethod
from typing import List

from ..file_version import FileVersion
from ..raw_api import SRC_LAST_MODIFIED_MILLIS


class AbstractFile(ABC):
    """
    Hold information about one file in a folder.

    The name is relative to the folder in all cases.

    Files that have multiple versions (which only happens
    in B2, not in local folders) include information about
    all of the versions, most recent first.
    """

    __slots__ = ['name', 'versions']

    def __repr__(self):
        return '%s(%s, [%s])' % (
            self.__class__.__name__, self.name, ', '.join(repr(v) for v in self.versions)
        )

    @abstractmethod
    def latest_version(self) -> 'AbstractSyncFileVersion':
        """
        Return the latest file version.
        """


class LocalFile(AbstractFile):
    """
    Hold information about one file in a local folder.
    """

    def __init__(self, name, versions: List['LocalSyncFileVersion']):
        """
        :param str name: a relative file name
        :param List[LocalSyncFileVersion] versions: a list of file versions
        """
        self.name = name
        self.versions = versions

    def latest_version(self) -> 'LocalSyncFileVersion':
        return self.versions[0]


class B2File(LocalFile):
    """
    Hold information about one file in a folder in B2 cloud.
    """

    def __init__(self, name, versions: List['B2SyncFileVersion']):
        """
        :param str name: a relative file name
        :param List[B2SyncFileVersion] versions: a list of file versions
        """
        super().__init__(name, versions)

    def latest_version(self) -> 'B2SyncFileVersion':
        return super().latest_version()


class AbstractSyncFileVersion(ABC):
    def __repr__(self):
        return '%s(%s, %s, %s, %s)' % (
            self.__class__.__name__,
            repr(self.id_),
            repr(self.name),
            repr(self.mod_time),
            repr(self.action),
        )

    @property
    @abstractmethod
    def id_(self):
        pass

    @property
    @abstractmethod
    def name(self):
        pass

    @property
    @abstractmethod
    def mod_time(self):
        pass

    @property
    @abstractmethod
    def action(self):
        pass

    @property
    @abstractmethod
    def size(self):
        pass


class LocalSyncFileVersion(AbstractSyncFileVersion):
    """
    Hold information about one version of a file.
    """

    __slots__ = ['_id', '_name', '_mod_time', '_action', '_size']

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
        self._id = id_
        self._name = file_name
        self._mod_time = mod_time
        self._action = action
        self._size = size

    @property
    def id_(self):
        return self._id

    @property
    def name(self):
        return self._name

    @property
    def mod_time(self):
        return self._mod_time

    @property
    def action(self):
        return self._action

    @property
    def size(self):
        return self._size


class B2SyncFileVersion(AbstractSyncFileVersion):
    __slots__ = [
        'file_version'
    ]  # in a typical use case there is a lot of these object in memory, hence __slots__

    # and properties

    def __init__(self, file_version: FileVersion):
        self.file_version = file_version

    @property
    def id_(self):
        return self.file_version.id_

    @property
    def name(self):
        return self.file_version.file_name

    @property
    def mod_time(self):
        if SRC_LAST_MODIFIED_MILLIS in self.file_version.file_info:
            return int(self.file_version.file_info[SRC_LAST_MODIFIED_MILLIS])
        return self.file_version.upload_timestamp

    @property
    def action(self):
        return self.file_version.action

    @property
    def size(self):
        return self.file_version.size
