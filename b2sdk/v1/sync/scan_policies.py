######################################################################
#
# File: b2sdk/v1/sync/scan_policies.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import _v2 as v2
from b2sdk._v2 import exception as v2_exception  # noqa


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
        exclude_dir_regexes=tuple(),
        exclude_file_regexes=tuple(),
        include_file_regexes=tuple(),
        exclude_all_symlinks=False,
        exclude_modified_before=None,
        exclude_modified_after=None,
    ):
        """
        :param exclude_dir_regexes: a tuple of regexes to exclude directories
        :type exclude_dir_regexes: tuple
        :param exclude_file_regexes: a tuple of regexes to exclude files
        :type exclude_file_regexes: tuple
        :param include_file_regexes: a tuple of regexes to include files
        :type include_file_regexes: tuple
        :param exclude_all_symlinks: if True, exclude all symlinks
        :type exclude_all_symlinks: bool
        :param exclude_modified_before: optionally exclude file versions modified before (in millis)
        :type exclude_modified_before: int, optional
        :param exclude_modified_after: optionally exclude file versions modified after (in millis)
        :type exclude_modified_after: int, optional
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


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
