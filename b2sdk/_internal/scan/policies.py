######################################################################
#
# File: b2sdk/_internal/scan/policies.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import re
from typing import Iterable

from ..file_version import FileVersion
from .exception import InvalidArgument, check_invalid_argument
from .path import LocalPath

logger = logging.getLogger(__name__)


class RegexSet:
    """
    Hold a (possibly empty) set of regular expressions and know how to check
    whether a string matches any of them.
    """

    def __init__(self, regex_iterable):
        """
        :param regex_iterable: an interable which yields regexes
        """
        self._compiled_list = [re.compile(r) for r in regex_iterable]

    def matches(self, s):
        """
        Check whether a string matches any of regular expressions.

        :param s: a string to check
        :type s: str
        :rtype: bool
        """
        return any(c.match(s) is not None for c in self._compiled_list)


def convert_dir_regex_to_dir_prefix_regex(dir_regex: str | re.Pattern) -> str:
    """
    The patterns used to match directory names (and file names) are allowed
    to match a prefix of the name.  This 'feature' was unintentional, but is
    being retained for compatibility.

    This means that a regex that matches a directory name can't be used directly
    to match against a file name and test whether the file should be excluded
    because it matches the directory.

    The pattern 'photos' will match directory names 'photos' and 'photos2',
    and should exclude files 'photos/kitten.jpg', and 'photos2/puppy.jpg'.
    It should not exclude 'photos.txt', because there is no directory name
    that matches.

    On the other hand, the pattern 'photos$' should match 'photos/kitten.jpg',
    but not 'photos2/puppy.jpg', nor 'photos.txt'

    If the original regex is valid, there are only two cases to consider:
    either the regex ends in '$' or does not.

    :param dir_regex: a regular expression string or literal
    :return: a regular expression string which matches the directory prefix
    """
    if isinstance(dir_regex, re.Pattern):
        dir_regex = dir_regex.pattern
    if dir_regex.endswith('$'):
        return dir_regex[:-1] + r'/'
    else:
        return dir_regex + r'.*?/'


class IntegerRange:
    """
    Hold a range of two integers. If the range value is None, it indicates that
    the value should be treated as -Inf (for begin) or +Inf (for end).
    """

    def __init__(self, begin, end):
        """
        :param begin: begin position of the range (included)
        :type begin: int
        :param end: end position of the range (included)
        :type end: int
        """
        self._begin = begin
        self._end = end

        if self._begin and self._begin < 0:
            raise ValueError('begin time can not be less than 0, use None for the infinity')

        if self._end and self._end < 0:
            raise ValueError('end time can not be less than 0, use None for the infinity')

    def __contains__(self, item):
        ge_begin, le_end = True, True

        if self._begin is not None:
            ge_begin = item >= self._begin
        if self._end is not None:
            le_end = item <= self._end

        return ge_begin and le_end


class ScanPoliciesManager:
    """
    Policy object used when scanning folders, used to decide
    which files to include in the list of files.

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
            raise InvalidArgument(
                'include_file_regexes',
                'cannot be used without exclude_file_regexes at the same time'
            )

        with check_invalid_argument(
            'exclude_dir_regexes', 'wrong regex was given for excluding directories', re.error
        ):
            self._exclude_dir_set = RegexSet(exclude_dir_regexes)
            self._exclude_file_because_of_dir_set = RegexSet(
                map(convert_dir_regex_to_dir_prefix_regex, exclude_dir_regexes)
            )
        with check_invalid_argument(
            'exclude_file_regexes', 'wrong regex was given for excluding files', re.error
        ):
            self._exclude_file_set = RegexSet(exclude_file_regexes)
        with check_invalid_argument(
            'include_file_regexes', 'wrong regex was given for including files', re.error
        ):
            self._include_file_set = RegexSet(include_file_regexes)
        self.exclude_all_symlinks = exclude_all_symlinks
        with check_invalid_argument(
            'exclude_modified_before,exclude_modified_after', '', ValueError
        ):
            self._include_mod_time_range = IntegerRange(
                exclude_modified_before, exclude_modified_after
            )
        with check_invalid_argument(
            'exclude_uploaded_before,exclude_uploaded_after', '', ValueError
        ):
            self._include_upload_time_range = IntegerRange(
                exclude_uploaded_before, exclude_uploaded_after
            )

    def should_exclude_relative_path(self, relative_path: str):
        if self._include_file_set.matches(relative_path):
            return False
        return self._exclude_file_set.matches(relative_path)

    def should_exclude_local_path(self, local_path: LocalPath):
        """
        Whether a local path should be excluded from the scan or not.

        This method assumes that the directory holding the `path_` has already been checked for exclusion.
        """
        if local_path.mod_time not in self._include_mod_time_range:
            return True
        return self.should_exclude_relative_path(local_path.relative_path)

    def should_exclude_b2_file_version(self, file_version: FileVersion, relative_path: str):
        """
        Whether a b2 file version should be excluded from the scan or not.

        This method assumes that the directory holding the `path_` has already been checked for exclusion.
        """
        if file_version.upload_timestamp not in self._include_upload_time_range:
            return True
        if file_version.mod_time_millis not in self._include_mod_time_range:
            return True
        return self.should_exclude_relative_path(relative_path)

    def should_exclude_b2_directory(self, dir_path: str):
        """
        Given the path of a directory, relative to the scan point,
        decide if all of the files in it should be excluded from the scan.
        """
        return self._exclude_dir_set.matches(dir_path)

    def should_exclude_local_directory(self, dir_path: str):
        """
        Given the path of a directory, relative to the scan point,
        decide if all of the files in it should be excluded from the scan.
        """
        return self._exclude_dir_set.matches(dir_path)


DEFAULT_SCAN_MANAGER = ScanPoliciesManager()
