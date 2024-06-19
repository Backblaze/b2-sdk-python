######################################################################
#
# File: b2sdk/_internal/scan/folder.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import os
import platform
import re
import sys
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Iterator

from ..utils import fix_windows_path_limit, get_file_mtime, validate_b2_file_name
from .exception import (
    EmptyDirectory,
    EnvironmentEncodingError,
    NotADirectory,
    UnableToCreateDirectory,
    UnsupportedFilename,
)
from .path import AbstractPath, B2Path, LocalPath
from .policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from .report import ProgressReport

DRIVE_MATCHER = re.compile(r"^([A-Za-z]):([/\\])")
ABSOLUTE_PATH_MATCHER = re.compile(r"^(/)|^(\\)")
RELATIVE_PATH_MATCHER = re.compile(
                           # "abc" and "xyz" represent anything, including "nothing"
    r"^(\.\.[/\\])|" +     # ../abc or ..\abc
    r"^(\.[/\\])|" +       # ./abc or .\abc
    r"([/\\]\.\.[/\\])|" + # abc/../xyz or abc\..\xyz or abc\../xyz or abc/..\xyz
    r"([/\\]\.[/\\])|" +   # abc/./xyz or abc\.\xyz or abc\./xyz or abc/.\xyz
    r"([/\\]\.\.)$|" +     # abc/.. or abc\..
    r"([/\\]\.)$|" +       # abc/. or abc\.
    r"^(\.\.)$|" +         # just ".."
    r"([/\\][/\\])|" +     # abc\/xyz or abc/\xyz or abc//xyz or abc\\xyz
    r"^(\.)$"              # just "."
)  # yapf: disable

logger = logging.getLogger(__name__)


class AbstractFolder(metaclass=ABCMeta):
    """
    Interface to a folder full of files, which might be a B2 bucket,
    a virtual folder in a B2 bucket, or a directory on a local file
    system.

    Files in B2 may have multiple versions, while files in local
    folders have just one.
    """

    @abstractmethod
    def all_files(self, reporter: ProgressReport | None,
                  policies_manager=DEFAULT_SCAN_MANAGER) -> Iterator[AbstractPath]:
        """
        Return an iterator over all of the files in the folder, in
        the order that B2 uses (lexicographic by object path).

        It also performs filtering using policies manager.

        No matter what the folder separator on the local file system
        is, "/" is used in the returned file names.

        If a file is found, but does not exist (for example due to
        a broken symlink or a race), reporter will be informed about
        each such problem.

        :param reporter: a place to report errors
        :param policies_manager: a policies manager object
        """

    @abstractmethod
    def folder_type(self):
        """
        Return one of:  'b2', 'local'.

        :rtype: str
        """

    @abstractmethod
    def make_full_path(self, file_name):
        """
        Return the full path to the file.

        :param file_name: a file name
        :type file_name: str
        :rtype: str
        """


def join_b2_path(relative_dir_path: str | Path, file_name: str):
    """
    Like os.path.join, but for B2 file names where the root directory is called ''.
    """
    relative_dir_path = str(relative_dir_path)
    if relative_dir_path in ('', '.'):
        return file_name
    else:
        return relative_dir_path + '/' + file_name


class LocalFolder(AbstractFolder):
    """
    Folder interface to a directory on the local machine.
    """

    def __init__(self, root: str | Path):
        """
        Initialize a new folder.

        :param root: path to the root of the local folder.  Must be unicode.
        """
        if isinstance(root, Path):
            root = str(root)
        if not isinstance(root, str):
            raise ValueError('folder path should be str or pathlib.Path: %s' % repr(root))
        self.root = fix_windows_path_limit(os.path.abspath(root))

    def folder_type(self):
        """
        Return folder type.

        :rtype: str
        """
        return 'local'

    def all_files(self, reporter: ProgressReport | None,
                  policies_manager=DEFAULT_SCAN_MANAGER) -> Iterator[LocalPath]:
        """
        Yield all files.

        Yield a File object for each of the files anywhere under this folder, in the
        order they would appear in B2, unless the path is excluded by policies manager.

        :param reporter: a place to report errors
        :param policies_manager: a policy manager object, default is DEFAULT_SCAN_MANAGER
        :return: an iterator over all files in the folder in the order they would appear in B2
        """
        root_path = Path(self.root)

        local_paths = self._walk_relative_paths(root_path, Path(''), reporter, policies_manager)
        # Crucial to return the "correct" order of the files
        yield from sorted(local_paths, key=lambda lp: lp.relative_path)

    def make_full_path(self, file_name):
        """
        Convert a file name into an absolute path, ensure it is not outside self.root

        :param file_name: a file name
        :type file_name: str
        """
        # Fix OS path separators
        file_name = file_name.replace('/', os.path.sep)

        # Generate the full path to the file
        full_path = os.path.normpath(os.path.join(self.root, file_name))

        # Get the common prefix between the new full_path and self.root
        common_prefix = os.path.commonprefix([full_path, self.root])

        # Ensure the new full_path is inside the self.root directory
        if common_prefix != self.root:
            raise UnsupportedFilename("illegal file name", full_path)

        return full_path

    def ensure_present(self):
        """
        Make sure that the directory exists.
        """
        if not os.path.exists(self.root):
            try:
                os.mkdir(self.root)
            except OSError:
                raise UnableToCreateDirectory(self.root)
        elif not os.path.isdir(self.root):
            raise NotADirectory(self.root)

    def ensure_non_empty(self):
        """
        Make sure that the directory exists and is non-empty.
        """
        self.ensure_present()

        if not os.listdir(self.root):
            raise EmptyDirectory(self.root)

    def _walk_relative_paths(
        self,
        local_dir: Path,
        relative_dir_path: Path,
        reporter: ProgressReport,
        policies_manager: ScanPoliciesManager,
        visited_symlinks: set[int] | None = None,
    ):
        """
        Yield a File object for each of the files anywhere under this folder,
        unless the path is excluded by policies manager.

        :param local_dir: the path to the local directory that we are currently inspecting
        :param relative_dir_path: the path of this dir relative to the scan point, or Path('') if at scan point
        :param reporter: a reporter object to report errors and warnings
        :param policies_manager: a policies manager object
        :param visited_symlinks: a set of paths to symlinks that have already been visited. Using inode numbers to reduce memory usage
        """

        # Collect the names.  We do this before returning any results, because
        # directories need to sort as if their names end in '/'.
        #
        # With a directory containing 'a', 'a.txt', and 'a0.txt', with 'a' being
        # a directory containing 'b.txt', and 'c.txt', the results returned
        # should be:
        #
        #    a.txt
        #    a/b.txt
        #    a/c.txt
        #    a0.txt
        #
        # This is because in Unicode '.' comes before '/', which comes before '0'.
        visited_symlinks = visited_symlinks or set()

        if local_dir.is_symlink():
            inode_number = local_dir.resolve().stat().st_ino
            if inode_number in visited_symlinks:
                if reporter:
                    reporter.circular_symlink_skipped(str(local_dir))
                return  # Skip if symlink already visited
            visited_symlinks.add(inode_number)

        for local_path in sorted(local_dir.iterdir()):
            name = local_path.name
            relative_file_path = join_b2_path(relative_dir_path, name)

            if policies_manager.exclude_all_symlinks and local_path.is_symlink():
                if reporter is not None:
                    reporter.symlink_skipped(str(local_path))
                continue
            try:
                validate_b2_file_name(name)
            except ValueError as e:
                if reporter is not None:
                    reporter.invalid_name(str(local_path), str(e))
                continue

            if local_path.is_dir():
                if policies_manager.should_exclude_local_directory(str(relative_file_path)):
                    continue  # Skip excluded directories
                # Recurse into directories
                yield from self._walk_relative_paths(
                    local_path, relative_file_path, reporter, policies_manager, visited_symlinks
                )
            else:
                if policies_manager.should_exclude_relative_path(relative_file_path):
                    continue  # Skip excluded files
                try:
                    file_mod_time = get_file_mtime(str(local_path))
                    file_size = local_path.stat().st_size
                except OSError:
                    if reporter is not None:
                        reporter.local_access_error(str(local_path))
                        continue

                local_scan_path = LocalPath(
                    absolute_path=self.make_full_path(str(relative_file_path)),
                    relative_path=str(relative_file_path),
                    mod_time=file_mod_time,
                    size=file_size
                )
                if policies_manager.should_exclude_local_path(local_scan_path):
                    continue  # Skip excluded files

                if not os.access(local_path, os.R_OK):
                    if reporter is not None:
                        reporter.local_permission_error(str(local_path))
                        continue

                yield local_scan_path

    @classmethod
    def _handle_non_unicode_file_name(cls, name):
        """
        Decide what to do with a name returned from os.listdir()
        that isn't unicode.  We think that this only happens when
        the file name can't be decoded using the file system
        encoding.  Just in case that's not true, we'll allow all-ascii
        names.
        """
        # if it's all ascii, allow it
        if all(b <= 127 for b in name):
            return name
        raise EnvironmentEncodingError(repr(name), sys.getfilesystemencoding())

    def __repr__(self):
        return f'LocalFolder({self.root})'


def b2_parent_dir(file_name):
    # Various Parent dir getting method have been tested, and this one seems to be the faste
    # After dropping python 3.9 support: refactor this use the "match" syntax
    try:
        dir_name, _ = file_name.rsplit('/', 1)
    except ValueError:
        return ''
    return dir_name


class B2Folder(AbstractFolder):
    """
    Folder interface to b2.
    """

    def __init__(self, bucket_name, folder_name, api):
        """
        :param bucket_name: a name of the bucket
        :type bucket_name: str
        :param folder_name: a folder name
        :type folder_name: str
        :param api: an API object
        :type api: b2sdk._internal.api.B2Api
        """
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.bucket = api.get_bucket_by_name(bucket_name)
        self.api = api
        self.prefix = self.folder_name
        if self.prefix and self.prefix[-1] != '/':
            self.prefix += '/'

    def all_files(
        self,
        reporter: ProgressReport | None,
        policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER
    ) -> Iterator[B2Path]:
        """
        Yield all files.
        """
        current_name = None
        last_ignored_dir = None
        current_versions = []
        current_file_version = None
        for file_version in self.get_file_versions():
            if current_file_version is None:
                current_file_version = file_version

            assert file_version.file_name.startswith(self.prefix)
            if file_version.action == 'start':
                continue
            file_name = file_version.file_name[len(self.prefix):]
            if last_ignored_dir is not None and file_name.startswith(last_ignored_dir):
                continue

            dir_name = b2_parent_dir(file_name)

            if policies_manager.should_exclude_b2_directory(dir_name):
                last_ignored_dir = dir_name + '/'
                continue
            else:
                last_ignored_dir = None

            if policies_manager.should_exclude_b2_file_version(file_version, file_name):
                continue

            self._validate_file_name(file_name)

            if current_name != file_name and current_name is not None and current_versions:
                yield B2Path(
                    relative_path=current_name,
                    selected_version=current_versions[0],
                    all_versions=current_versions
                )
                current_versions = []

            current_name = file_name
            current_versions.append(file_version)

        if current_name is not None and current_versions:
            yield B2Path(
                relative_path=current_name,
                selected_version=current_versions[0],
                all_versions=current_versions
            )

    def get_file_versions(self):
        for file_version, _ in self.bucket.ls(
            self.folder_name,
            latest_only=False,
            recursive=True,
        ):
            yield file_version

    def _validate_file_name(self, file_name):
        # Do not allow relative paths in file names
        if RELATIVE_PATH_MATCHER.search(file_name):
            raise UnsupportedFilename(
                "scan does not support file names that include relative paths", file_name
            )
        # Do not allow absolute paths in file names
        if ABSOLUTE_PATH_MATCHER.search(file_name):
            raise UnsupportedFilename(
                "scan does not support file names with absolute paths", file_name
            )
        # On Windows, do not allow drive letters in file names
        if platform.system() == "Windows" and DRIVE_MATCHER.search(file_name):
            raise UnsupportedFilename(
                "scan does not support file names with drive letters", file_name
            )

    def folder_type(self):
        """
        Return folder type.

        :rtype: str
        """
        return 'b2'

    def make_full_path(self, file_name):
        """
        Make an absolute path from a file name.

        :param file_name: a file name
        :type file_name: str
        """
        if self.folder_name == '':
            return file_name
        else:
            return self.folder_name + '/' + file_name

    def __str__(self):
        return f'B2Folder({self.bucket_name}, {self.folder_name})'
