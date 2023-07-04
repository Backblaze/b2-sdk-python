######################################################################
#
# File: test/unit/fixtures/folder.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from unittest import mock

import apiver_deps
import pytest
from apiver_deps import DEFAULT_SCAN_MANAGER, B2Folder, LocalFolder, LocalPath

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
        self.local_paths = [self._local_path(*test_file) for test_file in test_files]

    def all_files(self, reporter, policies_manager=DEFAULT_SCAN_MANAGER):
        for single_path in self.local_paths:
            if single_path.relative_path.endswith('/'):
                if policies_manager.should_exclude_b2_directory(single_path.relative_path):
                    continue
            else:
                if policies_manager.should_exclude_local_path(single_path):
                    continue
            yield single_path

    def make_full_path(self, name):
        return '/dir/' + name

    def _local_path(self, name, mod_times, size=10):
        """
        Makes a LocalPath object for a local file.
        """
        return LocalPath(name, name, mod_times[0], size)


@pytest.fixture(scope='session')
def folder_factory():
    def get_folder(f_type, *files):
        if f_type == 'b2':
            return FakeB2Folder(files)
        return FakeLocalFolder(files)

    return get_folder
