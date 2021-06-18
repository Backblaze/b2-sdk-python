######################################################################
#
# File: test/unit/sync/fixtures.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from unittest import mock

import pytest

import apiver_deps
from apiver_deps import AbstractFolder, B2Folder, LocalFolder, B2SyncPath, LocalSyncPath
from apiver_deps import CompareVersionMode, NewerFileSyncMode, KeepOrDeleteMode
from apiver_deps import DEFAULT_SCAN_MANAGER, Synchronizer

if apiver_deps.V <= 1:
    from apiver_deps import FileVersionInfo as VFileVersion
else:
    from apiver_deps import FileVersion as VFileVersion


class FakeB2Folder(B2Folder):
    def __init__(self, test_files):
        self.file_versions = []
        for test_file in test_files:
            self.file_versions.extend(self._file_versions(*test_file))
        super().__init__('test-bucket', 'folder', mock.MagicMock())

    def get_file_versions(self):
        yield from iter(self.file_versions)

    def _file_versions(self, name, mod_times, size=10):
        """
        Makes FileVersion objects.

        Positive modification times are uploads, and negative modification
        times are hides.  It's a hack, but it works.

        """
        if apiver_deps.V <= 1:
            mandatory_kwargs = {}
        else:
            mandatory_kwargs = {
                'api': None,
                'account_id': 'account-id',
                'bucket_id': 'bucket-id',
                'content_md5': 'content_md5',
                'server_side_encryption': None,
            }
        return [
            VFileVersion(
                id_='id_%s_%d' % (name[0], abs(mod_time)),
                file_name='folder/' + name,
                upload_timestamp=abs(mod_time),
                action='upload' if 0 < mod_time else 'hide',
                size=size,
                file_info={'in_b2': 'yes'},
                content_type='text/plain',
                content_sha1='content_sha1',
                **mandatory_kwargs,
            ) for mod_time in mod_times
        ]  # yapf disable


class FakeLocalFolder(LocalFolder):
    def __init__(self, test_files):
        super().__init__('folder')
        self.local_sync_paths = [self._local_sync_path(*test_file) for test_file in test_files]

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

    def _local_sync_path(self, name, mod_times, size=10):
        """
        Makes a LocalSyncPath object for a local file.
        """
        return LocalSyncPath(name, name, mod_times[0], size)


@pytest.fixture(scope='session')
def folder_factory():
    def get_folder(f_type, *files):
        if f_type == 'b2':
            return FakeB2Folder(files)
        return FakeLocalFolder(files)

    return get_folder


@pytest.fixture(scope='session')
def synchronizer_factory():
    def get_synchronizer(
        policies_manager=DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=NewerFileSyncMode.RAISE_ERROR,
        keep_days_or_delete=KeepOrDeleteMode.NO_DELETE,
        keep_days=None,
        compare_version_mode=CompareVersionMode.MODTIME,
        compare_threshold=None,
    ):
        return Synchronizer(
            1,
            policies_manager=policies_manager,
            dry_run=dry_run,
            allow_empty_source=allow_empty_source,
            newer_file_mode=newer_file_mode,
            keep_days_or_delete=keep_days_or_delete,
            keep_days=keep_days,
            compare_version_mode=compare_version_mode,
            compare_threshold=compare_threshold,
        )

    return get_synchronizer


@pytest.fixture
def synchronizer(synchronizer_factory):
    return synchronizer_factory()
