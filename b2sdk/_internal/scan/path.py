######################################################################
#
# File: b2sdk/_internal/scan/path.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABC, abstractmethod

from ..file_version import FileVersion


class AbstractPath(ABC):
    """
    Represent a path in a source or destination folder - be it B2 or local
    """

    def __init__(self, relative_path: str, mod_time: int, size: int):
        self.relative_path = relative_path
        self.mod_time = mod_time
        self.size = size

    @abstractmethod
    def is_visible(self) -> bool:
        """Is the path visible/not deleted on it's storage"""

    def __repr__(self):
        return '{}({}, {}, {})'.format(
            self.__class__.__name__, repr(self.relative_path), repr(self.mod_time), repr(self.size)
        )


class LocalPath(AbstractPath):
    __slots__ = ['absolute_path', 'relative_path', 'mod_time', 'size']

    def __init__(self, absolute_path: str, relative_path: str, mod_time: int, size: int):
        self.absolute_path = absolute_path
        super().__init__(relative_path, mod_time, size)

    def is_visible(self) -> bool:
        return True

    def __eq__(self, other):
        return (
            self.absolute_path == other.absolute_path and
            self.relative_path == other.relative_path and self.mod_time == other.mod_time and
            self.size == other.size
        )


class B2Path(AbstractPath):
    __slots__ = ['relative_path', 'selected_version', 'all_versions']

    def __init__(
        self, relative_path: str, selected_version: FileVersion, all_versions: list[FileVersion]
    ):
        self.selected_version = selected_version
        self.all_versions = all_versions
        self.relative_path = relative_path

    def is_visible(self) -> bool:
        return self.selected_version.action != 'hide'

    @property
    def mod_time(self) -> int:
        return self.selected_version.mod_time_millis

    @property
    def size(self) -> int:
        return self.selected_version.size

    def __repr__(self):
        return '{}({}, [{}])'.format(
            self.__class__.__name__, self.relative_path, ', '.join(
                f'({repr(fv.id_)}, {repr(fv.mod_time_millis)}, {repr(fv.action)})'
                for fv in self.all_versions
            )
        )

    def __eq__(self, other):
        return (
            self.relative_path == other.relative_path and
            self.selected_version == other.selected_version and
            self.all_versions == other.all_versions
        )
