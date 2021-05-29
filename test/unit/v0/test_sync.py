######################################################################
#
# File: test/unit/v0/test_sync.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import concurrent.futures as futures
import os
import platform
import sys
import threading
import time
import unittest
from unittest.mock import MagicMock, ANY

import pytest

from ..test_base import TestBase

from .deps_exception import UnSyncableFilename, NotADirectory, UnableToCreateDirectory, EmptyDirectory, InvalidArgument, CommandError
from .deps import FileVersionInfo
from .deps import AbstractFolder, B2Folder, LocalFolder
from .deps import LocalSyncPath, B2SyncPath
from .deps import ScanPoliciesManager, DEFAULT_SCAN_MANAGER
from .deps import BoundedQueueExecutor, make_folder_sync_actions, zip_folders
from .deps import parse_sync_folder
from .deps import TempDir

DAY = 86400000  # milliseconds
TODAY = DAY * 100  # an arbitrary reference time for testing


def write_file(path, contents):
    parent = os.path.dirname(path)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    with open(path, 'wb') as f:
        f.write(contents)


class TestSync(TestBase):
    def setUp(self):
        self.reporter = MagicMock()


class TestFolder(TestSync):
    __test__ = False

    NAMES = [
        '.dot_file',
        'hello.',
        os.path.join('hello', 'a', '1'),
        os.path.join('hello', 'a', '2'),
        os.path.join('hello', 'b'),
        'hello0',
        os.path.join('inner', 'a.bin'),
        os.path.join('inner', 'a.txt'),
        os.path.join('inner', 'b.bin'),
        os.path.join('inner', 'b.txt'),
        os.path.join('inner', 'more', 'a.bin'),
        os.path.join('inner', 'more', 'a.txt'),
        '\u81ea\u7531',
    ]

    MOD_TIMES = {'.dot_file': TODAY - DAY, 'hello.': TODAY - DAY}

    def setUp(self):
        super(TestFolder, self).setUp()

        self.root_dir = ''

    def prepare_folder(
        self,
        prepare_files=True,
        broken_symlink=False,
        invalid_permissions=False,
        use_file_versions_info=False
    ):
        raise NotImplementedError

    def all_files(self, policies_manager):
        return list(self.prepare_folder().all_files(self.reporter, policies_manager))

    def assert_filtered_files(self, scan_results, expected_scan_results):
        self.assertEqual(expected_scan_results, list(f.relative_path for f in scan_results))
        self.reporter.local_access_error.assert_not_called()

    def test_exclusions(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello/a/1',
            'hello/a/2',
            'hello/b',
            'hello0',
            'inner/a.txt',
            'inner/b.txt',
            'inner/more/a.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=('.*\\.bin',))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_all(self):
        expected_list = []
        polices_manager = ScanPoliciesManager(exclude_file_regexes=('.*',))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclusions_inclusions(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello/a/1',
            'hello/a/2',
            'hello/b',
            'hello0',
            'inner/a.bin',
            'inner/a.txt',
            'inner/b.txt',
            'inner/more/a.bin',
            'inner/more/a.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(
            exclude_file_regexes=('.*\\.bin',),
            include_file_regexes=('.*a\\.bin',),
        )
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_matches_prefix(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello/b',
            'hello0',
            'inner/b.bin',
            'inner/b.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=('.*a',))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_directory(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello0',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(
            exclude_dir_regexes=('hello', 'more', 'hello0'), exclude_file_regexes=('inner',)
        )
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_directory2(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello0',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_dir_regexes=('hello$', 'inner'))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_directory_trailing_slash_does_not_match(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello0',
            'inner/a.bin',
            'inner/a.txt',
            'inner/b.bin',
            'inner/b.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_dir_regexes=('hello$', 'inner/'))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclusion_with_exact_match(self):
        expected_list = [
            '.dot_file',
            'hello.',
            'hello/a/1',
            'hello/a/2',
            'hello/b',
            'inner/a.bin',
            'inner/a.txt',
            'inner/b.bin',
            'inner/b.txt',
            'inner/more/a.bin',
            'inner/more/a.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_file_regexes=('hello0',))
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_modified_before_in_range(self):
        expected_list = [
            'hello/a/1',
            'hello/a/2',
            'hello/b',
            'hello0',
            'inner/a.bin',
            'inner/a.txt',
            'inner/b.bin',
            'inner/b.txt',
            'inner/more/a.bin',
            'inner/more/a.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_modified_before=TODAY - 100)
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_modified_before_exact(self):
        expected_list = [
            'hello/a/1',
            'hello/a/2',
            'hello/b',
            'hello0',
            'inner/a.bin',
            'inner/a.txt',
            'inner/b.bin',
            'inner/b.txt',
            'inner/more/a.bin',
            'inner/more/a.txt',
            '\u81ea\u7531',
        ]
        polices_manager = ScanPoliciesManager(exclude_modified_before=TODAY)
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_modified_after_in_range(self):
        expected_list = ['.dot_file', 'hello.']
        polices_manager = ScanPoliciesManager(exclude_modified_after=TODAY - 100)
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)

    def test_exclude_modified_after_exact(self):
        expected_list = ['.dot_file', 'hello.']
        polices_manager = ScanPoliciesManager(exclude_modified_after=TODAY - DAY)
        files = self.all_files(polices_manager)
        self.assert_filtered_files(files, expected_list)


class TestLocalFolder(TestFolder):
    __test__ = True

    def setUp(self):
        super(TestLocalFolder, self).setUp()

        self.temp_dir = TempDir()
        self.root_dir = self.temp_dir.__enter__()

    def tearDown(self):
        self.temp_dir.__exit__(*sys.exc_info())

    def prepare_folder(
        self,
        prepare_files=True,
        broken_symlink=False,
        invalid_permissions=False,
        use_file_versions_info=False
    ):
        assert not (broken_symlink and invalid_permissions)

        if platform.system() == 'Windows':
            pytest.skip('on Windows there are some environment issues with test directory creation')

        if prepare_files:
            for relative_path in self.NAMES:
                self.prepare_file(relative_path)

        if broken_symlink:
            os.symlink(
                os.path.join(self.root_dir, 'non_existant_file'),
                os.path.join(self.root_dir, 'bad_symlink')
            )
        elif invalid_permissions:
            os.chmod(os.path.join(self.root_dir, self.NAMES[0]), 0)

        return LocalFolder(self.root_dir)

    def prepare_file(self, relative_path):
        path = os.path.join(self.root_dir, relative_path)
        write_file(path, b'')

        if relative_path in self.MOD_TIMES:
            mod_time = int(round(self.MOD_TIMES[relative_path] / 1000))
        else:
            mod_time = int(round(TODAY / 1000))
        os.utime(path, (mod_time, mod_time))

    def test_slash_sorting(self):
        # '/' should sort between '.' and '0'
        folder = self.prepare_folder()
        self.assertEqual(self.NAMES, list(f.relative_path for f in folder.all_files(self.reporter)))
        self.reporter.local_access_error.assert_not_called()

    def test_broken_symlink(self):
        folder = self.prepare_folder(broken_symlink=True)
        self.assertEqual(self.NAMES, list(f.relative_path for f in folder.all_files(self.reporter)))
        self.reporter.local_access_error.assert_called_once_with(
            os.path.join(self.root_dir, 'bad_symlink')
        )

    def test_invalid_permissions(self):
        folder = self.prepare_folder(invalid_permissions=True)
        # tests differ depending on the user running them. "root" will
        # succeed in os.access(path, os.R_OK) even if the permissions of
        # the file are 0 as implemented on self._prepare_folder().
        # use-case: running test suite inside a docker container
        if not os.access(os.path.join(self.root_dir, self.NAMES[0]), os.R_OK):
            self.assertEqual(
                self.NAMES[1:], list(f.relative_path for f in folder.all_files(self.reporter))
            )
            self.reporter.local_permission_error.assert_called_once_with(
                os.path.join(self.root_dir, self.NAMES[0])
            )
        else:
            self.assertEqual(
                self.NAMES, list(f.relative_path for f in folder.all_files(self.reporter))
            )

    def test_syncable_paths(self):
        syncable_paths = (
            ('test.txt', 'test.txt'), ('./a/test.txt', 'a/test.txt'),
            ('./a/../test.txt', 'test.txt')
        )

        folder = self.prepare_folder(prepare_files=False)
        for syncable_path, norm_syncable_path in syncable_paths:
            expected = os.path.join(self.root_dir, norm_syncable_path.replace('/', os.path.sep))
            self.assertEqual(expected, folder.make_full_path(syncable_path))

    def test_unsyncable_paths(self):
        unsyncable_paths = ('../test.txt', 'a/../../test.txt', '/a/test.txt')

        folder = self.prepare_folder(prepare_files=False)
        for unsyncable_path in unsyncable_paths:
            with self.assertRaises(UnSyncableFilename):
                folder.make_full_path(unsyncable_path)


class TestB2Folder(TestFolder):
    __test__ = True

    FILE_VERSION_INFOS = {
        os.path.join('inner', 'a.txt'):
            [
                (
                    FileVersionInfo(
                        'a2', 'inner/a.txt', 200, 'text/plain', 'sha1', {}, 2000, 'upload'
                    ), ''
                ),
                (
                    FileVersionInfo(
                        'a1', 'inner/a.txt', 100, 'text/plain', 'sha1', {}, 1000, 'upload'
                    ), ''
                )
            ],
        os.path.join('inner', 'b.txt'):
            [
                (
                    FileVersionInfo(
                        'b2', 'inner/b.txt', 200, 'text/plain', 'sha1', {}, 1999, 'upload'
                    ), ''
                ),
                (
                    FileVersionInfo(
                        'bs', 'inner/b.txt', 150, 'text/plain', 'sha1', {}, 1500, 'start'
                    ), ''
                ),
                (
                    FileVersionInfo(
                        'b1', 'inner/b.txt', 100, 'text/plain', 'sha1',
                        {'src_last_modified_millis': 1001}, 6666, 'upload'
                    ), ''
                )
            ]
    }

    def setUp(self):
        super(TestB2Folder, self).setUp()
        self.bucket = MagicMock()
        self.bucket.ls.return_value = []
        self.api = MagicMock()
        self.api.get_bucket_by_name.return_value = self.bucket

    def prepare_folder(
        self,
        prepare_files=True,
        broken_symlink=False,
        invalid_permissions=False,
        use_file_versions_info=False
    ):
        if prepare_files:
            for relative_path in self.NAMES:
                self.prepare_file(relative_path, use_file_versions_info)

        return B2Folder('bucket-name', self.root_dir, self.api)

    def prepare_file(self, relative_path, use_file_versions_info=False):
        if use_file_versions_info and relative_path in self.FILE_VERSION_INFOS:
            self.bucket.ls.return_value.extend(self.FILE_VERSION_INFOS[relative_path])
            return

        if platform.system() == 'Windows':
            relative_path = relative_path.replace(os.sep, '/')
        if relative_path in self.MOD_TIMES:
            self.bucket.ls.return_value.append(
                (
                    FileVersionInfo(
                        relative_path, relative_path, 100, 'text/plain', 'sha1', {},
                        self.MOD_TIMES[relative_path], 'upload'
                    ), self.root_dir
                )
            )
        else:
            self.bucket.ls.return_value.append(
                (
                    FileVersionInfo(
                        relative_path, relative_path, 100, 'text/plain', 'sha1', {}, TODAY, 'upload'
                    ), self.root_dir
                )
            )

    def test_empty(self):
        folder = self.prepare_folder(prepare_files=False)
        self.assertEqual([], list(folder.all_files(self.reporter)))

    def test_multiple_versions(self):
        # Test two files, to cover the yield within the loop, and the yield without.
        folder = self.prepare_folder(use_file_versions_info=True)

        self.assertEqual(
            [
                "B2SyncPath(inner/a.txt, [('a2', 2000, 'upload'), ('a1', 1000, 'upload')])",
                "B2SyncPath(inner/b.txt, [('b2', 1999, 'upload'), ('b1', 1001, 'upload')])"
            ], [
                str(f) for f in folder.all_files(self.reporter)
                if f.relative_path in ('inner/a.txt', 'inner/b.txt')
            ]
        )

    def test_exclude_modified_multiple_versions(self):
        polices_manager = ScanPoliciesManager(
            exclude_modified_before=1001, exclude_modified_after=1999
        )
        folder = self.prepare_folder(use_file_versions_info=True)
        self.assertEqual(
            ["B2SyncPath(inner/b.txt, [('b2', 1999, 'upload'), ('b1', 1001, 'upload')])"], [
                str(f) for f in folder.all_files(self.reporter, policies_manager=polices_manager)
                if f.relative_path in ('inner/a.txt', 'inner/b.txt')
            ]
        )

    def test_exclude_modified_all_versions(self):
        polices_manager = ScanPoliciesManager(
            exclude_modified_before=1500, exclude_modified_after=1500
        )
        folder = self.prepare_folder(use_file_versions_info=True)
        self.assertEqual(
            [], list(folder.all_files(self.reporter, policies_manager=polices_manager))
        )

    # Path names not allowed to be sync'd on Windows
    NOT_SYNCD_ON_WINDOWS = [
        'Z:/windows/system32/drivers/etc/hosts',
        'a:/Users/.default/test',
        r'C:\Windows\system32\drivers\mstsc.sys',
    ]

    def test_unsyncable_filenames(self):
        b2_folder = B2Folder('bucket-name', '', self.api)

        # Test a list of unsyncable file names
        filenames_to_test = [
            '/',  # absolute root path
            '//',
            '///',
            '/..',
            '/../',
            '/.../',
            '/../.',
            '/../..',
            '/a.txt',
            '/folder/a.txt',
            './folder/a.txt',  # current dir relative path
            'folder/./a.txt',
            'folder/folder/.',
            'a//b/',  # double-slashes
            'a///b',
            'a////b',
            '../test',  # start with parent dir
            '../../test',
            '../../abc/../test',
            '../../abc/../test/',
            '../../abc/../.test',
            'a/b/c/../d',  # parent dir embedded
            'a//..//b../..c/',
            '..a/b../..c/../d..',
            'a/../',
            'a/../../../../../',
            'a/b/c/..',
            r'\\',  # backslash filenames
            r'\z',
            r'..\\',
            r'..\..',
            r'\..\\',
            r'\\..\\..',
            r'\\',
            r'\\\\',
            r'\\\\server\\share\\dir\\file',
            r'\\server\share\dir\file',
            r'\\?\C\Drive\temp',
            r'.\\//',
            r'..\\..//..\\\\',
            r'.\\a\\..\\b',
            r'a\\.\\b',
        ]

        if platform.system() == "Windows":
            filenames_to_test.extend(self.NOT_SYNCD_ON_WINDOWS)

        for filename in filenames_to_test:
            self.bucket.ls.return_value = [
                (FileVersionInfo('a1', filename, 1, 'text/plain', 'sha1', {}, 1000, 'upload'), '')
            ]
            try:
                list(b2_folder.all_files(self.reporter))
                self.fail("should have thrown UnSyncableFilename for: '%s'" % filename)
            except UnSyncableFilename as e:
                self.assertTrue(filename in str(e))

    def test_syncable_filenames(self):
        b2_folder = B2Folder('bucket-name', '', self.api)

        # Test a list of syncable file names
        filenames_to_test = [
            '',
            ' ',
            ' / ',
            ' ./. ',
            ' ../.. ',
            '.. / ..',
            r'.. \ ..',
            'file.txt',
            '.folder/',
            '..folder/',
            '..file',
            r'file/ and\ folder',
            'file..',
            '..file..',
            'folder/a.txt..',
            '..a/b../c../..d/e../',
            r'folder\test',
            r'folder\..f..\..f\..f',
            r'mix/and\match/',
            r'a\b\c\d',
        ]

        # filenames not permitted on Windows *should* be allowed on Linux
        if platform.system() != "Windows":
            filenames_to_test.extend(self.NOT_SYNCD_ON_WINDOWS)

        for filename in filenames_to_test:
            self.bucket.ls.return_value = [
                (FileVersionInfo('a1', filename, 1, 'text/plain', 'sha1', {}, 1000, 'upload'), '')
            ]
            list(b2_folder.all_files(self.reporter))


class FakeLocalFolder(LocalFolder):
    def __init__(self, local_sync_paths):
        super().__init__('folder')
        self.local_sync_paths = local_sync_paths

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        for single_path in self.local_sync_paths:
            if single_path.relative_path.endswith('/'):
                if policies_manager.should_exclude_b2_directory(single_path.relative_path):
                    continue
            else:
                if policies_manager.should_exclude_local_path(single_path):
                    continue
            yield single_path

    def make_full_path(self, name):
        return '/dir/' + name


class FakeB2Folder(B2Folder):
    def __init__(self, test_files):
        self.file_versions = []
        for test_file in test_files:
            self.file_versions.extend(self.file_versions_from_file_tuples(*test_file))
        super().__init__('test-bucket', 'folder', MagicMock())

    def get_file_versions(self):
        yield from iter(self.file_versions)

    @classmethod
    def file_versions_from_file_tuples(cls, name, mod_times, size=10):
        """
        Makes FileVersion objects.

        Positive modification times are uploads, and negative modification
        times are hides.  It's a hack, but it works.

        """
        try:
            mod_times = iter(mod_times)
        except TypeError:
            mod_times = [mod_times]
        return [
            FileVersionInfo(
                id_='id_%s_%d' % (name[0], abs(mod_time)),
                file_name='folder/' + name,
                upload_timestamp=abs(mod_time),
                action='upload' if 0 < mod_time else 'hide',
                size=size,
                file_info={'in_b2': 'yes'},
                content_type='text/plain',
                content_sha1='content_sha1',
            ) for mod_time in mod_times
        ]  # yapf disable

    @classmethod
    def sync_path_from_file_tuple(cls, name, mod_times, size=10):
        file_versions = cls.file_versions_from_file_tuples(name, mod_times, size)
        return B2SyncPath(name, file_versions[0], file_versions)


class TestParseSyncFolder(TestBase):
    def test_b2_double_slash(self):
        self._check_one('B2Folder(my-bucket, folder/path)', 'b2://my-bucket/folder/path')

    def test_b2_no_double_slash(self):
        self._check_one('B2Folder(my-bucket, folder/path)', 'b2:my-bucket/folder/path')

    def test_b2_trailing_slash(self):
        self._check_one('B2Folder(my-bucket, a)', 'b2://my-bucket/a/')

    def test_b2_no_folder(self):
        self._check_one('B2Folder(my-bucket, )', 'b2://my-bucket')
        self._check_one('B2Folder(my-bucket, )', 'b2://my-bucket/')

    def test_local(self):
        if platform.system() == 'Windows':
            drive, _ = os.path.splitdrive(os.getcwd())
            expected = 'LocalFolder(\\\\?\\%s\\foo)' % (drive,)
        else:
            expected = 'LocalFolder(/foo)'
        self._check_one(expected, '/foo')

    def test_local_trailing_slash(self):
        if platform.system() == 'Windows':
            drive, _ = os.path.splitdrive(os.getcwd())
            expected = 'LocalFolder(\\\\?\\%s\\foo)' % (drive,)
        else:
            expected = 'LocalFolder(/foo)'
        self._check_one(expected, '/foo/')

    def _check_one(self, expected, to_parse):
        api = MagicMock()
        self.assertEqual(expected, str(parse_sync_folder(str(to_parse), api)))


class TestFolderExceptions:
    """There is an exact copy of this class in unit/v1/test_sync.py - TODO: leave only one when migrating tests to
       sync-like structure.
    """

    @pytest.mark.parametrize(
        'exception,msg',
        [
            pytest.param(NotADirectory, '.*', marks=pytest.mark.apiver(from_ver=2)),
            pytest.param(Exception, 'is not a directory', marks=pytest.mark.apiver(to_ver=1)),
        ],
    )
    def test_ensure_present_not_a_dir(self, exception, msg):
        with TempDir() as path:
            file = os.path.join(path, 'clearly_a_file')
            with open(file, 'w') as f:
                f.write(' ')
            folder = parse_sync_folder(file, MagicMock())
            with pytest.raises(exception, match=msg):
                folder.ensure_present()

    @pytest.mark.parametrize(
        'exception,msg',
        [
            pytest.param(UnableToCreateDirectory, '.*', marks=pytest.mark.apiver(from_ver=2)),
            pytest.param(
                Exception, 'unable to create directory', marks=pytest.mark.apiver(to_ver=1)
            ),
        ],
    )
    def test_ensure_present_unable_to_create(self, exception, msg):
        with TempDir() as path:
            file = os.path.join(path, 'clearly_a_file')
            with open('file', 'w') as f:
                f.write(' ')
            folder = parse_sync_folder(os.path.join(file, 'nonsense'), MagicMock())
            with pytest.raises(exception, match=msg):
                folder.ensure_present()

    @pytest.mark.parametrize(
        'exception,msg',
        [
            pytest.param(EmptyDirectory, '.*', marks=pytest.mark.apiver(from_ver=2)),
            pytest.param(
                CommandError,
                'Directory .* is empty.  Use --allowEmptySource to sync anyway.',
                marks=pytest.mark.apiver(to_ver=1)
            ),
        ],
    )
    def test_ensure_non_empty(self, exception, msg):
        with TempDir() as path:
            folder = parse_sync_folder(path, MagicMock())
            with pytest.raises(exception, match=msg):
                folder.ensure_non_empty()

    @pytest.mark.parametrize(
        'exception,msg',
        [
            pytest.param(InvalidArgument, '.*', marks=pytest.mark.apiver(from_ver=2)),
            pytest.param(
                CommandError, "'//' not allowed in path names", marks=pytest.mark.apiver(to_ver=1)
            ),
        ],
    )
    def test_double_slash_not_allowed(self, exception, msg):

        with pytest.raises(exception, match=msg):
            parse_sync_folder('b2://a//b', MagicMock())


class TestZipFolders(TestSync):
    def test_empty(self):
        folder_a = FakeB2Folder([])
        folder_b = FakeB2Folder([])
        self.assertEqual([], list(zip_folders(folder_a, folder_b, self.reporter)))

    def test_one_empty(self):
        file_a1 = LocalSyncPath("a.txt", "a.txt", 100, 10)
        folder_a = FakeLocalFolder([file_a1])
        folder_b = FakeB2Folder([])
        self.assertEqual([(file_a1, None)], list(zip_folders(folder_a, folder_b, self.reporter)))

    def test_two(self):
        file_a1 = ("a.txt", 100, 10)
        file_a2 = ("b.txt", 100, 10)
        file_a3 = ("d.txt", 100, 10)
        file_a4 = ("f.txt", 100, 10)
        file_b1 = ("b.txt", 200, 10)
        file_b2 = ("e.txt", 200, 10)
        folder_a = FakeB2Folder([file_a1, file_a2, file_a3, file_a4])
        folder_b = FakeB2Folder([file_b1, file_b2])
        self.assertEqual(
            [
                (FakeB2Folder.sync_path_from_file_tuple(*file_a1), None),
                (
                    FakeB2Folder.sync_path_from_file_tuple(*file_a2),
                    FakeB2Folder.sync_path_from_file_tuple(*file_b1)
                ), (FakeB2Folder.sync_path_from_file_tuple(*file_a3), None),
                (None, FakeB2Folder.sync_path_from_file_tuple(*file_b2)),
                (FakeB2Folder.sync_path_from_file_tuple(*file_a4), None)
            ], list(zip_folders(folder_a, folder_b, self.reporter))
        )

    def test_pass_reporter_to_folder(self):
        """
        Check that the zip_folders() function passes the reporter through
        to both folders.
        """
        folder_a = MagicMock()
        folder_b = MagicMock()
        folder_a.all_files = MagicMock(return_value=iter([]))
        folder_b.all_files = MagicMock(return_value=iter([]))
        self.assertEqual([], list(zip_folders(folder_a, folder_b, self.reporter)))
        folder_a.all_files.assert_called_once_with(self.reporter, ANY)
        folder_b.all_files.assert_called_once_with(self.reporter)


class FakeArgs(object):
    """
    Can be passed to sync code to simulate command-line options.
    """

    def __init__(
        self,
        delete=False,
        keepDays=None,
        skipNewer=False,
        replaceNewer=False,
        compareVersions=None,
        compareThreshold=None,
        excludeRegex=None,
        excludeDirRegex=None,
        includeRegex=None,
        debugLogs=True,
        dryRun=False,
        allowEmptySource=False,
        excludeAllSymlinks=False,
    ):
        self.delete = delete
        self.keepDays = keepDays
        self.skipNewer = skipNewer
        self.replaceNewer = replaceNewer
        self.compareVersions = compareVersions
        self.compareThreshold = compareThreshold
        if excludeRegex is None:
            excludeRegex = []
        self.excludeRegex = excludeRegex
        if includeRegex is None:
            includeRegex = []
        self.includeRegex = includeRegex
        if excludeDirRegex is None:
            excludeDirRegex = []
        self.excludeDirRegex = excludeDirRegex
        self.debugLogs = debugLogs
        self.dryRun = dryRun
        self.allowEmptySource = allowEmptySource
        self.excludeAllSymlinks = excludeAllSymlinks


def local_file(name, mod_time, size=10):
    """
    Makes a File object for a b2 file, with one FileVersion for
    each modification time given in mod_times.
    """
    return LocalSyncPath(name, name, mod_time, size)


class TestExclusions(TestSync):
    def _check_folder_sync(self, expected_actions, fakeargs):
        # only local
        file_a = ('a.txt', 100)
        file_b = ('b.txt', 100)
        file_d = ('d/d.txt', 100)
        file_e = ('e/e.incl', 100)

        # both local and remote
        file_bi = ('b.txt.incl', 100)
        file_z = ('z.incl', 100)

        # only remote
        file_c = ('c.txt', 100)

        local_folder = FakeLocalFolder(
            [local_file(*f) for f in (file_a, file_b, file_d, file_e, file_bi, file_z)]
        )
        b2_folder = FakeB2Folder([file_bi, file_c, file_z])

        policies_manager = ScanPoliciesManager(
            exclude_dir_regexes=fakeargs.excludeDirRegex,
            exclude_file_regexes=fakeargs.excludeRegex,
            include_file_regexes=fakeargs.includeRegex,
            exclude_all_symlinks=fakeargs.excludeAllSymlinks
        )
        actions = list(
            make_folder_sync_actions(
                local_folder, b2_folder, fakeargs, TODAY, self.reporter, policies_manager
            )
        )
        self.assertEqual(expected_actions, [str(a) for a in actions])

    def test_file_exclusions_with_delete(self):
        expected_actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 100)',
            'b2_delete(folder/b.txt.incl, id_b_100, )',
            'b2_delete(folder/c.txt, id_c_100, )',
            'b2_upload(/dir/d/d.txt, folder/d/d.txt, 100)',
            'b2_upload(/dir/e/e.incl, folder/e/e.incl, 100)',
        ]
        self._check_folder_sync(expected_actions, FakeArgs(delete=True, excludeRegex=["b\\.txt"]))

    def test_file_exclusions_inclusions_with_delete(self):
        expected_actions = [
            'b2_upload(/dir/a.txt, folder/a.txt, 100)',
            'b2_delete(folder/b.txt.incl, id_b_100, )',
            'b2_delete(folder/c.txt, id_c_100, )',
            'b2_upload(/dir/d/d.txt, folder/d/d.txt, 100)',
            'b2_upload(/dir/e/e.incl, folder/e/e.incl, 100)',
            'b2_upload(/dir/b.txt.incl, folder/b.txt.incl, 100)',
        ]
        fakeargs = FakeArgs(delete=True, excludeRegex=["b\\.txt"], includeRegex=[".*\\.incl"])
        self._check_folder_sync(expected_actions, fakeargs)


class TestBoundedQueueExecutor(TestBase):
    def test_run_more_than_queue_size(self):
        """
        Makes sure that the executor will run more jobs that the
        queue size, which ensures that the semaphore gets released,
        even if an exception is thrown.
        """
        raw_executor = futures.ThreadPoolExecutor(1)
        bounded_executor = BoundedQueueExecutor(raw_executor, 5)

        class Counter(object):
            """
            Counts how many times run() is called.
            """

            def __init__(self):
                self.counter = 0

            def run(self):
                """
                Always increments the counter.  Sometimes raises an exception.
                """
                self.counter += 1
                if self.counter % 2 == 0:
                    raise Exception('test')

        counter = Counter()
        for _ in range(10):
            bounded_executor.submit(counter.run)
        bounded_executor.shutdown()
        self.assertEqual(10, counter.counter)

    def test_wait_for_running_jobs(self):
        """
        Makes sure that no more than queue_limit workers are
        running at once, which checks that the semaphore is
        acquired before submitting an action.
        """
        raw_executor = futures.ThreadPoolExecutor(2)
        bounded_executor = BoundedQueueExecutor(raw_executor, 1)
        assert_equal = self.assertEqual

        class CountAtOnce(object):
            """
            Counts how many threads are running at once.
            There should never be more than 1 because that's
            the limit on the bounded executor.
            """

            def __init__(self):
                self.running_at_once = 0
                self.lock = threading.Lock()

            def run(self):
                with self.lock:
                    self.running_at_once += 1
                    assert_equal(1, self.running_at_once)
                # While we are sleeping here, no other actions should start
                # running.  If they do, they will increment the counter and
                # fail the above assertion.
                time.sleep(0.05)
                with self.lock:
                    self.running_at_once -= 1
                self.counter += 1
                if self.counter % 2 == 0:
                    raise Exception('test')

        count_at_once = CountAtOnce()
        for _ in range(5):
            bounded_executor.submit(count_at_once.run)
        bounded_executor.shutdown()


if __name__ == '__main__':
    unittest.main()
