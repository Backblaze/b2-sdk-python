######################################################################
#
# File: test/unit/sync/fixtures.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from ..apiver import apiver


class FakeFolder(apiver.AbstractFolder):
    def __init__(self, f_type, files=None):
        if files is None:
            files = []

        self.f_type = f_type
        self.files = files

    def all_files(self, reporter, policies_manager=apiver.DEFAULT_SCAN_MANAGER):
        for single_file in self.files:
            if single_file.name.endswith('/'):
                if policies_manager.should_exclude_directory(single_file.name):
                    continue
            else:
                if policies_manager.should_exclude_file(single_file.name):
                    continue
            yield single_file

    def folder_type(self):
        return self.f_type

    def make_full_path(self, name):
        if self.f_type == 'local':
            return '/dir/' + name
        else:
            return 'folder/' + name

    def __str__(self):
        return '%s(%s, %s)' % (self.__class__.__name__, self.f_type, self.make_full_path(''))


def local_file(name, mod_times, size=10):
    """
    Makes a File object for a b2 file, with one FileVersion for
    each modification time given in mod_times.
    """
    versions = [
        apiver.FileVersion('/dir/%s' % (name,), name, mod_time, 'upload', size)
        for mod_time in mod_times
    ]
    return apiver.File(name, versions)


def b2_file(name, mod_times, size=10):
    """
    Makes a File object for a b2 file, with one FileVersion for
    each modification time given in mod_times.

    Positive modification times are uploads, and negative modification
    times are hides.  It's a hack, but it works.

        b2_file('a.txt', [300, -200, 100])

    Is the same as:

        File(
            'a.txt',
            [
               FileVersion('id_a_300', 'a.txt', 300, 'upload'),
               FileVersion('id_a_200', 'a.txt', 200, 'hide'),
               FileVersion('id_a_100', 'a.txt', 100, 'upload')
            ]
        )
    """
    versions = [
        apiver.FileVersion(
            'id_%s_%d' % (name[0], abs(mod_time)),
            'folder/' + name,
            abs(mod_time),
            'upload' if 0 < mod_time else 'hide',
            size,
        ) for mod_time in mod_times
    ]  # yapf disable
    return apiver.File(name, versions)


@pytest.fixture(scope='module')
def folder_factory():
    def get_folder(f_type, *files):
        def get_files():
            nonlocal files
            for file in files:
                if f_type == 'local':
                    yield local_file(*file)
                else:
                    yield b2_file(*file)

        return FakeFolder(f_type, list(get_files()))

    return get_folder


@pytest.fixture(scope='module')
def synchronizer_factory():
    def get_synchronizer(
        policies_manager=apiver.DEFAULT_SCAN_MANAGER,
        dry_run=False,
        allow_empty_source=False,
        newer_file_mode=apiver.NewerFileSyncMode.RAISE_ERROR,
        keep_days_or_delete=apiver.KeepOrDeleteMode.NO_DELETE,
        keep_days=None,
        compare_version_mode=apiver.CompareVersionMode.MODTIME,
        compare_threshold=None,
    ):
        return apiver.Synchronizer(
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
