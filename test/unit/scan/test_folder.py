######################################################################
#
# File: test/unit/scan/test_folder.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import platform
from pathlib import Path

import pytest

from b2sdk._internal.scan.exception import UnsupportedFilename
from b2sdk._internal.scan.folder import LocalFolder
from b2sdk._internal.utils import fix_windows_path_limit


@pytest.mark.skipif(
    platform.system() == 'Windows',
    reason="Windows doesn't allow / or \\ in filenames",
)
class TestFolder:
    @pytest.fixture
    def root_path(self, tmp_path: Path):
        return tmp_path / 'dir'

    @pytest.fixture
    def folder(self, root_path: Path):
        return LocalFolder(str(root_path))

    @pytest.mark.parametrize('file_path_str', ['dir/foo.txt', 'dir/foo/bar.txt', 'foo.txt'])
    def test_make_full_path(self, folder: LocalFolder, root_path: Path, file_path_str: str):
        file_path = root_path / file_path_str
        assert folder.make_full_path(str(file_path)) == str(root_path / file_path_str)

    @pytest.mark.parametrize('file_path_str', ['invalid/test.txt', 'dirinvalid.txt'])
    def test_make_full_path_invalid_prefix(
        self, folder: LocalFolder, tmp_path: Path, file_path_str: str
    ):
        file_path = tmp_path / file_path_str

        with pytest.raises(UnsupportedFilename):
            folder.make_full_path(str(file_path))


@pytest.mark.skipif(
    platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
    reason='Symlinks not supported on PyPy/Windows',
)
def test_make_full_path_rejects_symlink_outside_root(tmp_path):
    root = tmp_path / 'root'
    root.mkdir()
    outside = tmp_path / 'outside'
    outside.mkdir()
    (root / 'link').symlink_to(outside, target_is_directory=True)

    with pytest.raises(UnsupportedFilename):
        LocalFolder(root).make_full_path('link/new-file.txt')

    assert not (outside / 'new-file.txt').exists()


@pytest.mark.skipif(
    platform.system() == 'Windows' and platform.python_implementation() == 'PyPy',
    reason='Symlinks not supported on PyPy/Windows',
)
def test_make_full_path_accepts_internal_link_and_symlinked_root(tmp_path):
    root = tmp_path / 'root'
    root.mkdir()
    (root / 'inside').mkdir()
    (root / 'link').symlink_to(root / 'inside', target_is_directory=True)
    root_link = tmp_path / 'root-link'
    root_link.symlink_to(root, target_is_directory=True)

    assert LocalFolder(root_link).make_full_path('link/new-file.txt') == fix_windows_path_limit(
        str(root_link / 'link' / 'new-file.txt')
    )
