######################################################################
#
# File: b2sdk/sync/folder.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import os
import platform
import re
import sys

from abc import ABCMeta, abstractmethod
from .exception import EmptyDirectory, EnvironmentEncodingError, UnSyncableFilename, NotADirectory, UnableToCreateDirectory
from .path import B2SyncPath, LocalSyncPath
from .report import SyncReport
from .scan_policies import DEFAULT_SCAN_MANAGER, ScanPoliciesManager
from ..utils import fix_windows_path_limit, get_file_mtime, is_file_readable

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
    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        """
        Return an iterator over all of the files in the folder, in
        the order that B2 uses.

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


def join_b2_path(relative_dir_path: str, file_name: str):
    """
    Like os.path.join, but for B2 file names where the root directory is called ''.
    """
    if relative_dir_path == '':
        return file_name
    else:
        return relative_dir_path + '/' + file_name


class LocalFolder(AbstractFolder):
    """
    Folder interface to a directory on the local machine.
    """

    def __init__(self, root):
        """
        Initialize a new folder.

        :param root: path to the root of the local folder.  Must be unicode.
        :type root: str
        """
        if not isinstance(root, str):
            raise ValueError('folder path should be unicode: %s' % repr(root))
        self.root = fix_windows_path_limit(os.path.abspath(root))

    def folder_type(self):
        """
        Return folder type.

        :rtype: str
        """
        return 'local'

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        """
        Yield all files.

        :param reporter: a place to report errors
        :param policies_manager: a policy manager object, default is DEFAULT_SCAN_MANAGER
        """
        yield from self._walk_relative_paths(self.root, '', reporter, policies_manager)

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
            raise UnSyncableFilename("illegal file name", full_path)

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
        self, local_dir: str, relative_dir_path: str, reporter: SyncReport,
        policies_manager: ScanPoliciesManager
    ):
        """
        Yield a File object for each of the files anywhere under this folder, in the
        order they would appear in B2, unless the path is excluded by policies manager.

        :param relative_dir_path: the path of this dir relative to the sync point, or '' if at sync point
        """
        if not isinstance(local_dir, str):
            raise ValueError('folder path should be unicode: %s' % repr(local_dir))

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
        names = []  # list of (name, local_path, relative_file_path)
        for name in os.listdir(local_dir):
            # We expect listdir() to return unicode if dir_path is unicode.
            # If the file name is not valid, based on the file system
            # encoding, then listdir() will return un-decoded str/bytes.
            if not isinstance(name, str):
                name = self._handle_non_unicode_file_name(name)

            if '/' in name:
                raise UnSyncableFilename(
                    "sync does not support file names that include '/'",
                    "%s in dir %s" % (name, local_dir)
                )

            local_path = os.path.join(local_dir, name)
            relative_file_path = join_b2_path(
                relative_dir_path, name
            )  # file path relative to the sync point

            # Skip broken symlinks or other inaccessible files
            if not is_file_readable(local_path, reporter):
                continue

            if policies_manager.exclude_all_symlinks and os.path.islink(local_path):
                if reporter is not None:
                    reporter.symlink_skipped(local_path)
                continue

            if os.path.isdir(local_path):
                name += '/'
                if policies_manager.should_exclude_local_directory(relative_file_path):
                    continue

            names.append((name, local_path, relative_file_path))

        # Yield all of the answers.
        #
        # Sorting the list of triples puts them in the right order because 'name',
        # the sort key, is the first thing in the triple.
        for (name, local_path, relative_file_path) in sorted(names):
            if name.endswith('/'):
                for subdir_file in self._walk_relative_paths(
                    local_path, relative_file_path, reporter, policies_manager
                ):
                    yield subdir_file
            else:
                # Check that the file still exists and is accessible, since it can take a long time
                # to iterate through large folders
                if is_file_readable(local_path, reporter):
                    file_mod_time = get_file_mtime(local_path)
                    file_size = os.path.getsize(local_path)

                    local_sync_path = LocalSyncPath(
                        absolute_path=self.make_full_path(relative_file_path),
                        relative_path=relative_file_path,
                        mod_time=file_mod_time,
                        size=file_size,
                    )

                    if policies_manager.should_exclude_local_path(local_sync_path):
                        continue

                    yield local_sync_path

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
        return 'LocalFolder(%s)' % (self.root,)


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
        :type api: b2sdk.api.B2Api
        """
        self.bucket_name = bucket_name
        self.folder_name = folder_name
        self.bucket = api.get_bucket_by_name(bucket_name)
        self.api = api
        self.prefix = '' if self.folder_name == '' else self.folder_name + '/'

    def all_files(
        self, reporter: SyncReport, policies_manager: ScanPoliciesManager = DEFAULT_SCAN_MANAGER
    ):
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
                yield B2SyncPath(
                    relative_path=current_name,
                    selected_version=current_versions[0],
                    all_versions=current_versions
                )
                current_versions = []

            current_name = file_name
            current_versions.append(file_version)

        if current_name is not None and current_versions:
            yield B2SyncPath(
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
            raise UnSyncableFilename(
                "sync does not support file names that include relative paths", file_name
            )
        # Do not allow absolute paths in file names
        if ABSOLUTE_PATH_MATCHER.search(file_name):
            raise UnSyncableFilename(
                "sync does not support file names with absolute paths", file_name
            )
        # On Windows, do not allow drive letters in file names
        if platform.system() == "Windows" and DRIVE_MATCHER.search(file_name):
            raise UnSyncableFilename(
                "sync does not support file names with drive letters", file_name
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
        return 'B2Folder(%s, %s)' % (self.bucket_name, self.folder_name)
