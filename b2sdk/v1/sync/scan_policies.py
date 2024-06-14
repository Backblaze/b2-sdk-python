######################################################################
#
# File: b2sdk/v1/sync/scan_policies.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import re
from typing import Iterable

from .file import B2FileVersion
from ..file_version import FileVersionInfo
from .file_to_path_translator import _translate_local_path_to_file
from b2sdk import v2
from b2sdk.v2 import exception as v2_exception  # noqa


# Override to retain old exceptions in __init__
# and to provide interface for new should_exclude_* methods
class ScanPoliciesManager(v2.ScanPoliciesManager):
    """
    Policy object used when scanning folders for syncing, used to decide
    which files to include in the list of files to be synced.

    Code that scans through files should at least use should_exclude_file()
    to decide whether each file should be included; it will check include/exclude
    patterns for file names, as well as patterns for excluding directories.

    Code that scans may optionally use should_exclude_directory() to test whether
    it can skip a directory completely and not bother listing the files and
    sub-directories in it.
    """

    def __init__(
        self,
        exclude_dir_regexes: Iterable[str | re.Pattern] = tuple(),
        exclude_file_regexes: Iterable[str | re.Pattern] = tuple(),
        include_file_regexes: Iterable[str | re.Pattern] = tuple(),
        exclude_all_symlinks: bool = False,
        exclude_modified_before: int | None = None,
        exclude_modified_after: int | None = None,
        exclude_uploaded_before: int | None = None,
        exclude_uploaded_after: int | None = None,
    ):
        """
        :param exclude_dir_regexes: regexes to exclude directories
        :param exclude_file_regexes: regexes to exclude files
        :param include_file_regexes: regexes to include files
        :param exclude_all_symlinks: if True, exclude all symlinks
        :param exclude_modified_before: optionally exclude file versions (both local and b2) modified before (in millis)
        :param exclude_modified_after: optionally exclude file versions (both local and b2) modified after (in millis)
        :param exclude_uploaded_before: optionally exclude b2 file versions uploaded before (in millis)
        :param exclude_uploaded_after: optionally exclude b2 file versions uploaded after (in millis)

        The regex matching priority for a given path is:
        1) the path is always excluded if it's dir matches `exclude_dir_regexes`, if not then
        2) the path is always included if it matches `include_file_regexes`, if not then
        3) the path is excluded if it matches `exclude_file_regexes`, if not then
        4) the path is included
        """
        if include_file_regexes and not exclude_file_regexes:
            raise v2_exception.InvalidArgument(
                'include_file_regexes',
                'cannot be used without exclude_file_regexes at the same time'
            )

        self._exclude_dir_set = v2.RegexSet(exclude_dir_regexes)
        self._exclude_file_because_of_dir_set = v2.RegexSet(
            map(v2.convert_dir_regex_to_dir_prefix_regex, exclude_dir_regexes)
        )
        self._exclude_file_set = v2.RegexSet(exclude_file_regexes)
        self._include_file_set = v2.RegexSet(include_file_regexes)
        self.exclude_all_symlinks = exclude_all_symlinks
        self._include_mod_time_range = v2.IntegerRange(
            exclude_modified_before, exclude_modified_after
        )
        with v2_exception.check_invalid_argument(
            'exclude_uploaded_before,exclude_uploaded_after', '', ValueError
        ):
            self._include_upload_time_range = v2.IntegerRange(
                exclude_uploaded_before, exclude_uploaded_after
            )

    def should_exclude_file(self, file_path):
        """
        Given the full path of a file, decide if it should be excluded from the scan.

        :param file_path: the path of the file, relative to the root directory
                          being scanned.
        :type: str
        :return: True if excluded.
        :rtype: bool
        """
        if self._exclude_file_because_of_dir_set.matches(file_path):
            return True
        if self._include_file_set.matches(file_path):
            return False
        return self._exclude_file_set.matches(file_path)

    def should_exclude_file_version(self, file_version):
        """
        Given the modification time of a file version,
        decide if it should be excluded from the scan.

        :param file_version: the file version object
        :type: b2sdk.v1.FileVersion
        :return: True if excluded.
        :rtype: bool
        """
        return file_version.mod_time not in self._include_mod_time_range

    def should_exclude_directory(self, dir_path):
        """
        Given the full path of a directory, decide if all of the files in it should be
        excluded from the scan.

        :param dir_path: the path of the directory, relative to the root directory
                         being scanned.  The path will never end in '/'.
        :type dir_path: str
        :return: True if excluded.
        """
        return self._exclude_dir_set.matches(dir_path)


class ScanPoliciesManagerWrapper(v2.ScanPoliciesManager):
    def __init__(self, scan_policies_manager: ScanPoliciesManager):
        self.scan_policies_manager = scan_policies_manager
        self.exclude_all_symlinks = scan_policies_manager.exclude_all_symlinks

    def __repr__(self):
        return f"{self.__class__.__name__}({self.scan_policies_manager})"

    def should_exclude_relative_path(self, relative_path: str):
        self.scan_policies_manager.should_exclude_file(relative_path)

    def should_exclude_local_path(self, local_path: v2.LocalSyncPath):
        if self.scan_policies_manager.should_exclude_file_version(
            _translate_local_path_to_file(local_path).latest_version()
        ):
            return True
        return self.scan_policies_manager.should_exclude_file(local_path.relative_path)

    def should_exclude_b2_file_version(self, file_version: FileVersionInfo, relative_path: str):
        if self.scan_policies_manager.should_exclude_file_version(B2FileVersion(file_version)):
            return True
        return self.scan_policies_manager.should_exclude_file(relative_path)

    def should_exclude_b2_directory(self, dir_path):
        return self.scan_policies_manager.should_exclude_directory(dir_path)

    def should_exclude_local_directory(self, dir_path):
        return self.scan_policies_manager.should_exclude_directory(dir_path)


def wrap_if_necessary(scan_policies_manager):
    if hasattr(scan_policies_manager, 'should_exclude_file'):
        return ScanPoliciesManagerWrapper(scan_policies_manager)
    return scan_policies_manager


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
